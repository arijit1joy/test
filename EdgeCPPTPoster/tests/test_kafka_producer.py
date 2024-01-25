import json
import sys
import unittest

from botocore.exceptions import ClientError
from tests.cda_module_mock_context import CDAModuleMockingContext
from unittest.mock import ANY, patch

with CDAModuleMockingContext(sys) as cda_module_mock_context:
    cda_module_mock_context.mock_module("utility")
    cda_module_mock_context.mock_module("edge_sqs_utility_layer.kafka")
    cda_module_mock_context.mock_module("boto3")

    import kafka_producer


class TestKafkaProducer(unittest.TestCase):
    """
    Test module for kafka_producer.py
    """

    @patch.dict("os.environ", {"mskSecretArn": "secret-arn"})
    @patch("kafka_producer.boto3.client")
    def test_get_secret_value_successful(self, mock_boto3_client):
        """
        Test for get_secret_value() running successfully.
        """
        mock_secrets_client = mock_boto3_client.return_value
        mock_secrets_client.get_secret_value.return_value = "secret-response"

        response = kafka_producer.get_secret_value()

        mock_secrets_client.get_secret_value.assert_called_with(SecretId="secret-arn")
        self.assertEqual(response, "secret-response")


    @patch.dict("os.environ", {"mskSecretArn": "secret-arn"})
    @patch("kafka_producer.boto3.client")
    @patch("kafka_producer.LOGGER")
    def test_get_secret_value_on_error(self, mock_logger, mock_boto3_client):
        """
        Test for get_secret_value() when it throws an exception.
        """
        mock_secrets_client = mock_boto3_client.return_value

        error_content = lambda code: {"Error": {"Code": code, "Message": ANY}}
        error_args = lambda code: (error_content(code), ANY)

        mock_secrets_client.get_secret_value.side_effect = ClientError(*error_args("ResourceNotFoundException"))
        kafka_producer.get_secret_value()

        mock_secrets_client.get_secret_value.side_effect = ClientError(*error_args("InvalidRequestException"))
        kafka_producer.get_secret_value()
        
        mock_secrets_client.get_secret_value.side_effect = ClientError(*error_args("InvalidParameterException"))
        kafka_producer.get_secret_value()

        mock_secrets_client.get_secret_value.side_effect = ClientError(*error_args("RandomException"))
        kafka_producer.get_secret_value()

        mock_logger.error.assert_called()


    @patch("kafka_producer.boto3.client")
    @patch("kafka_producer.cluster_response", None)
    def test_get_brokers_successful(self, mock_boto3_client):
        """
        Test for get_brokers() running successfully.
        """
        mock_cluster_client = mock_boto3_client.return_value
        mock_cluster_client.get_bootstrap_brokers.return_value = {
            "BootstrapBrokerStringSaslScram": "broker1,broker2,broker3"
        }

        response = kafka_producer.get_brokers("test-arn")

        mock_cluster_client.get_bootstrap_brokers.assert_called_with(ClusterArn="test-arn")
        self.assertEqual(response, ["broker1", "broker2", "broker3"])


    def test_create_kafka_message_successful(self):
        """
        Test for _create_kafka_message() running successfully.
        """
        response = kafka_producer._create_kafka_message(
            "message-id",
            "body",
            "device-id",
            "esn",
            "topic",
            "file-type",
            "bu",
            "sqs-message"
        )

        self.assertEqual(
            response,
            {
                "metadata": {
                    "messageID": "message-id",
                    "deviceID": ["device-id"],
                    "esn": ["esn"],
                    "bu": "bu",
                    "topic": "topic",
                    "fileType": "file-type",
                    "fileSentSQSMessage": "sqs-message"
                },
                "data": "body",
            }
        )


    @patch.dict("os.environ", {"mskClusterArn": "cluster-arn"})
    @patch("kafka_producer.get_secret_value")
    @patch("kafka_producer.get_brokers")
    @patch("kafka_producer.KafkaProducer")
    def test_publish_message_successful(self, mock_producer, mock_get_brokers, mock_get_secret_value):
        """
        Test for publish_message() running successfully.
        """
        mock_get_secret_value.return_value = {
            "SecretString": json.dumps({
                "username": "username",
                "password": "password"
            })
        }
        mock_get_brokers.return_value = ["broker1"]

        response = kafka_producer.publish_message(
            {"telematicsDeviceId": "test"},
            "J1939_HB",
            "topic"
        )

        mock_producer.assert_called_with(
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            bootstrap_servers=["broker1"],
            sasl_plain_username="username",
            sasl_plain_password="password"
        )
        mock_producer.return_value.send.assert_called_with("topic", json.dumps({"telematicsDeviceId": "test"}).encode("utf-8"))
        mock_producer.return_value.flush.assert_called()
        self.assertEqual(response, {"response": "200"})


    @patch.dict("os.environ", {"mskClusterArn": "cluster-arn"})
    @patch("kafka_producer.get_secret_value")
    @patch("kafka_producer.get_brokers")
    @patch("kafka_producer.KafkaProducer")
    @patch("kafka_producer.util")
    def test_publish_message_on_error(self, mock_util, mock_producer, mock_get_brokers, mock_get_secret_value):
        """
        Test for publish_message() when it throws an exception.
        """
        mock_get_secret_value.return_value = {
            "SecretString": json.dumps({
                "username": "username",
                "password": "password"
            })
        }
        mock_get_brokers.return_value = ["broker1"]
        mock_producer.side_effect = ClientError({"Error": {"Code": ANY, "Message": ANY}}, ANY)

        response = kafka_producer.publish_message(
            {"telematicsDeviceId": "test"},
            "J1939_HB",
            "topic"
        )

        mock_util.write_to_audit_table.assert_called_with("J1939_HB", ANY, "test")
        self.assertEqual(response, {"response": "500"})
