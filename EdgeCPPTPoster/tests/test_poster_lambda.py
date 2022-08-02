import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.append("../")
sys.modules["edge_logger"] = MagicMock()
sys.modules["sqs_utility"] = MagicMock()
sys.modules["kafka"] = MagicMock()
sys.modules["obfuscate_gps_utility"] = MagicMock()
sys.modules["metadata_utility"] = MagicMock()
sys.modules["edge_core"] = MagicMock()
sys.modules["scheduler_query"] = MagicMock()
sys.modules["boto3"] = MagicMock()


with patch.dict("os.environ", {
    "LoggingLevel": "debug",
    "PTxAPIKey": "testKey",
    "Region": "us-east-3",
    "edgeCommonAPIURL": "testurl",
    "publishKafka": "true",
    "CDPTJ1939PostURL": "testurl",
    "CDPTJ1939Header": "testheader",
    "RedisSecretName": "testsecret",
    "EndpointFile": "EndpointFile",
    "CPPostBucket": "CPPostBucket",
    "EndpointBucket": "EndpointBucket",
    "JSONFormat": "JSONFormat",
    "PSBUSpecifier": "PSBUSpecifier",
    "EBUSpecifier": "EBUSpecifier",
    "UseEndpointBucket": "UseEndpointBucket",
    "PTJ1939PostURL": "PTJ1939PostURL",
    "PTJ1939Header": "PTJ1939Header",
    "PowerGenValue": "PowerGenValue",
    "mapTspFromOwner": "true",
    "ProcessDataQuality": "true",
    "DataQualityLambda": "DataQualityLambda",
    "MaxAttempts": "2"
}):
    import PosterLambda

del sys.modules["edge_logger"]
del sys.modules["sqs_utility"]
del sys.modules["kafka"]
del sys.modules["obfuscate_gps_utility"]
del sys.modules["metadata_utility"]
del sys.modules["edge_core"]
del sys.modules["scheduler_query"]
del sys.modules["boto3"]


class TestPosterLambda(unittest.TestCase):
    sample_device_id = '12345'

    @patch("PosterLambda.invoke_db_reader")
    def test_getDeviceInfo_success(self, mock_db_reader):
        mock_db_reader.return_value = [{'test': 'value'}]
        result = PosterLambda.get_device_info(self.sample_device_id)
        self.assertEqual(result, {'test': 'value'})
        mock_db_reader.assert_called_once()

    @patch("PosterLambda.invoke_db_reader")
    def test_getDeviceInfo_uncaughtException(self, mock_db_reader):
        mock_db_reader.side_effect = Exception("Mock db reader exception")
        result = PosterLambda.get_device_info(self.sample_device_id)
        self.assertEqual(result, False)
        mock_db_reader.assert_called_once()

    @patch("PosterLambda.invoke_db_reader")
    def test_getDeviceInfo_caughtException(self, mock_db_reader):
        mock_db_reader.return_value = None
        result = PosterLambda.get_device_info(self.sample_device_id)
        self.assertEqual(result, False)
        self.assertEqual(mock_db_reader.call_count, 2)

if __name__ == '__main__':
    unittest.main()
