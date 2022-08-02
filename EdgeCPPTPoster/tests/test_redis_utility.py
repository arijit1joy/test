import sys
import boto3
from unittest import TestCase
from unittest.mock import patch, MagicMock
from moto import mock_secretsmanager

sys.path.append("../")
sys.modules["edge_logger"] = MagicMock()
sys.modules["rediscluster"] = MagicMock()

MOCK_SECRETSMANAGER = mock_secretsmanager()
SM_CLIENT = boto3.client('secretsmanager', region_name='us-east-1')
REDIS_SECRET_NAME = "test_secret_name"


def create_test_secret():
    SM_CLIENT.create_secret(Name=REDIS_SECRET_NAME, SecretString='{"redis_host": "testing", "redis_port": "testing"}')


def delete_test_secret():
    SM_CLIENT.delete_secret(SecretId=REDIS_SECRET_NAME)

MOCK_SECRETSMANAGER.start()
print("<---Successfully started mocking secrets manager actions!--->")

create_test_secret()
print("<---Successfully created the test secret!--->")

with patch.dict("os.environ", {'RedisSecretName': 'test_secret_name',
                               'region': 'us-east-1',
                               "LoggingLevel": "debug",
                               'EDGEDBReader_ARN': 'arn:::12345'
                               }):
    from utilities.redis_utility import get_redis_connection, get_set_redis_value

del sys.modules["edge_logger"]
del sys.modules["rediscluster"]


class TestRedisUtility(TestCase):
    @classmethod
    def tearDownClass(cls):
        delete_test_secret()
        print("<---Successfully deleted the test secret!--->")

        MOCK_SECRETSMANAGER.stop()
        print("<---Successfully stopped mocking secrets manager actions!--->")


    # @patch.dict("os.environ",
    # {'redis_secret_name': REDIS_SECRET_NAME, 'region': 'us-east-1', "LoggingLevel": "debug"})
    # def test_getRedisConnection_whenGetRedisConnectionIsCalled_thenConnectionObjectIsReturned(self):
    #     print("<---test_getRedisConnection_whenGetRedisConnectionIsCalled_thenConnectionObjectIsReturned--->")
    #
    #     result = get_redis_connection()
    #
    #     self.assertIsNotNone(result)

    @patch("utilities.redis_utility.RedisCluster")
    def test_getRedisConnection_whenGetRedisConnectionIsCalledAndExceptionOccurs_thenNoneIsReturned(self, mock_redis_cluster):
        print("<---test_getRedisConnection_whenGetRedisConnectionIsCalledAndExceptionOccurs_thenNoneIsReturned--->")
        mock_redis_cluster.side_effect = Exception('Mock redis cluster exception')
        result = get_redis_connection()

        self.assertIsNone(result)

    @patch("utilities.redis_utility.invoke_db_reader")
    @patch("utilities.redis_utility.get_redis_connection")
    @patch("utilities.redis_utility.REDIS_CLIENT")
    def test_getSetRedisValue_whenGetSetRedisValueIsCalledAndConnectionObjectAvailableWithoutCache_thenDBCalled(
            self, mock_redis_client, mock_get_redis_connection, mock_invoke_db_reader):
        print("<---test_getSetRedisValue_whenGetSetRedisValueIsCalledAndConnectionObjectAvailableWithoutCache_"
              "thenDBCalled--->")

        mock_redis_client.get.return_value = {}
        mock_get_redis_connection.return_value = None
        mock_invoke_db_reader.return_value = [{"test": "test"}]

        result = get_set_redis_value("test_key", "test_query", 3600)

        self.assertEqual(result[0]["test"], "test")
        mock_invoke_db_reader.assert_called_with("test_query")
        mock_redis_client.set.assert_called_with("test_key", '[{"test": "test"}]', ex=3600)

    @patch("utilities.redis_utility.invoke_db_reader")
    @patch("utilities.redis_utility.get_redis_connection")
    @patch("utilities.redis_utility.REDIS_CLIENT")
    def test_getSetRedisValue_whenGetSetRedisValueIsCalledAndConnectionObjectAvailableWithCache_thenDBNotCalled(
            self, mock_redis_client, mock_get_redis_connection, mock_read_from_the_edge_database):
        print("<---test_getSetRedisValue_whenGetSetRedisValueIsCalledAndConnectionObjectAvailableWithCache_"
              "thenDBNotCalled--->")

        mock_redis_client.get.return_value = '[{"test": "test"}]'

        result = get_set_redis_value("test_key", "test_query", 3600)

        self.assertEqual(result[0]["test"], "test")
        mock_get_redis_connection.assert_not_called()
        mock_read_from_the_edge_database.assert_not_called()

    @patch("utilities.redis_utility.invoke_db_reader")
    @patch("utilities.redis_utility.get_redis_connection")
    def test_getSetRedisValue_whenGetSetRedisValueIsCalledAndErrorOccurred_thenExceptionOccurred(
            self, mock_get_redis_connection, mock_read_from_the_edge_database):
        print("<---test_getSetRedisValue_whenGetSetRedisValueIsCalledAndErrorOccurred_thenExceptionOccurred--->")

        mock_get_redis_connection.return_value = None

        with patch("utilities.redis_utility.REDIS_CLIENT", None):
            get_set_redis_value("test_key", "test_query", 3600)

        self.assertRaises(Exception)
        mock_get_redis_connection.assert_called()
        mock_read_from_the_edge_database.assert_not_called()
