import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.append("../")

from tests.cda_module_mock_context import CDAModuleMockingContext

with  CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict("os.environ", {'RedisSecretName': 'test_secret_name',
                               'region': 'us-east-1',
                               "LoggingLevel": "debug",
                               'EDGEDBReader_ARN': 'arn:::12345'
                               }):
    cda_module_mock_context.mock_module("utility"),
    cda_module_mock_context.mock_module("edge_db_lambda_client"),
    cda_module_mock_context.mock_module("rediscluster")
    cda_module_mock_context.mock_module("boto3")
    cda_module_mock_context.mock_module("edge_secretsmanager_utility_layer")

    from utilities.redis_utility import get_redis_connection, get_set_redis_value


class TestRedisUtility(unittest.TestCase):

    @patch("utilities.redis_utility.RedisCluster")
    def test_getRedisConnection_whenGetRedisConnectionIsCalledAndExceptionOccurs_thenNoneIsReturned(self,
                                                                                                    mock_redis_cluster):
        print("<---test_getRedisConnection_whenGetRedisConnectionIsCalledAndExceptionOccurs_thenNoneIsReturned--->")
        mock_redis_cluster.side_effect = Exception('Mock redis cluster exception')
        result = get_redis_connection()

        self.assertIsNone(result)

    @patch("utilities.redis_utility.EDGE_DB_CLIENT")
    @patch("utilities.redis_utility.get_redis_connection")
    @patch("utilities.redis_utility.REDIS_CLIENT")
    def test_getSetRedisValue_whenGetSetRedisValueIsCalledAndConnectionObjectAvailableWithoutCache_thenDBCalled(
            self, mock_redis_client, mock_get_redis_connection, mock_invoke_db_reader):
        print("<---test_getSetRedisValue_whenGetSetRedisValueIsCalledAndConnectionObjectAvailableWithoutCache_"
              "thenDBCalled--->")

        mock_redis_client.get.return_value = {}
        mock_get_redis_connection.return_value = None
        mock_invoke_db_reader.execute.return_value = [{"test": "test"}]

        result = get_set_redis_value("test_key", "test_query", 3600)

        self.assertEqual(result[0]["test"], "test")
        mock_invoke_db_reader.execute.assert_called_with("test_query")
        mock_redis_client.set.assert_called_with("test_key", '[{"test": "test"}]', ex=3600)

    @patch("utilities.redis_utility.EDGE_DB_CLIENT")
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
        mock_read_from_the_edge_database.execute.assert_not_called()

    @patch("utilities.redis_utility.EDGE_DB_CLIENT")
    @patch("utilities.redis_utility.get_redis_connection")
    def test_getSetRedisValue_whenGetSetRedisValueIsCalledAndErrorOccurred_thenExceptionOccurred(
            self, mock_get_redis_connection, mock_read_from_the_edge_database):
        print("<---test_getSetRedisValue_whenGetSetRedisValueIsCalledAndErrorOccurred_thenExceptionOccurred--->")

        mock_get_redis_connection.return_value = None

        with patch("utilities.redis_utility.REDIS_CLIENT", None):
            get_set_redis_value("test_key", "test_query", 3600)

        self.assertRaises(Exception)
        mock_get_redis_connection.assert_called()
        mock_read_from_the_edge_database.execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
