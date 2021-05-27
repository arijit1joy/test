import os
import boto3
import unittest
from unittest.mock import patch, MagicMock
from moto import mock_s3
import sys

sys.path.append('../')
sys.modules['edge_logger'] = MagicMock()
sys.modules['obfuscate_gps_utility'] = MagicMock()
from obfuscate_gps_handler import obfuscate_gps, send_file_to_s3
del sys.modules['edge_logger']
del sys.modules['obfuscate_gps_utility']


class TestObfuscateGPSHandler(unittest.TestCase):
    @patch('obfuscate_gps_handler.send_file_to_s3')
    @patch('obfuscate_gps_handler.handle_gps_coordinates')
    def test_obfuscateGPS_givenBodyWithoutSamples_thenNotCalledObfuscateGPSCoordinates(
            self, mock_obfuscate_gps_coordinates, mock_send_file_to_s3):
        print("<-----test_obfuscate_gps_givenBodyWithoutSamples_thenNotCalledObfuscateGPSCoordinates----->")
        body = {"telematicsDeviceId": "1234567890"}
        result = obfuscate_gps(body)
        print("Result: ", result)
        mock_obfuscate_gps_coordinates.assert_not_called()
        mock_send_file_to_s3.assert_called()

    @patch('obfuscate_gps_handler.send_file_to_s3')
    @patch('obfuscate_gps_handler.handle_gps_coordinates')
    def test_obfuscateGPS_givenSamplesWithoutConvertedDeviceParameters_thenNotCalledObfuscateGPSCoordinates(
            self, mock_obfuscate_gps_coordinates, mock_send_file_to_s3):
        print("<-----test_obfuscate_gps_givenSamplesWithoutConvertedDeviceParameters_"
              "thenNotCalledObfuscateGPSCoordinates----->")
        body = {"telematicsDeviceId": "1234567890", "samples": [{"dateTimestamp": "2020-10-08T14:26:58.456Z"}]}
        result = obfuscate_gps(body)
        print("Result: ", result)
        mock_obfuscate_gps_coordinates.assert_not_called()
        mock_send_file_to_s3.assert_called()

    @patch('obfuscate_gps_handler.send_file_to_s3')
    @patch('obfuscate_gps_handler.handle_gps_coordinates')
    def test_obfuscateGPS_givenConvertedDeviceParametersWithoutLatitude_thenNotCalledObfuscateGPSCoordinates(
            self, mock_obfuscate_gps_coordinates, mock_send_file_to_s3):
        print("<-----test_obfuscate_gps_givenConvertedDeviceParametersWithoutLatitude_"
              "thenNotCalledObfuscateGPSCoordinates----->")
        body = {"telematicsDeviceId": "1234567890",
                "samples": [{"dateTimestamp": "2020-10-08T14:26:58.456Z",
                             "convertedDeviceParameters": {"messageID": "message_id", "Longitude": "30.9876543"}}]}
        result = obfuscate_gps(body)
        print("Result: ", result)
        mock_obfuscate_gps_coordinates.assert_not_called()
        mock_send_file_to_s3.assert_called()

    @patch('obfuscate_gps_handler.send_file_to_s3')
    @patch('obfuscate_gps_handler.handle_gps_coordinates')
    def test_obfuscateGPS_givenConvertedDeviceParametersWithoutLongitude_thenNotCalledObfuscateGPSCoordinates(
            self, mock_obfuscate_gps_coordinates, mock_send_file_to_s3):
        print("<-----test_obfuscate_gps_givenConvertedDeviceParametersWithoutLongitude_"
              "thenNotCalledObfuscateGPSCoordinates----->")
        body = {"telematicsDeviceId": "1234567890",
                "samples": [{"dateTimestamp": "2020-10-08T14:26:58.456Z",
                             "convertedDeviceParameters": {"messageID": "message_id", "Latitude": "-39.3456789"}}]}
        result = obfuscate_gps(body)
        print("Result: ", result)
        mock_obfuscate_gps_coordinates.assert_not_called()
        mock_send_file_to_s3.assert_called()

    @patch('obfuscate_gps_handler.send_file_to_s3')
    @patch('obfuscate_gps_handler.handle_gps_coordinates')
    def test_obfuscateGPS_givenConvertedDeviceParametersWithLatLong_thenCalledObfuscateGPSCoordinates(
            self, mock_obfuscate_gps_coordinates, mock_send_file_to_s3):
        print("<-----test_obfuscate_gps_givenConvertedDeviceParametersWithLatLong_"
              "thenCalledObfuscateGPSCoordinates----->")
        body = {"telematicsDeviceId": "1234567890",
                "samples": [{"dateTimestamp": "2020-10-08T14:26:58.456Z",
                             "convertedDeviceParameters": {"messageID": "message_id", "Latitude": "-39.3456789",
                                                           "Longitude": "30.9876543"}}]}
        mock_obfuscate_gps_coordinates.return_value = ("-12.345", "12.345")
        result = obfuscate_gps(body)
        print("Result: ", result)
        mock_obfuscate_gps_coordinates.assert_called()
        mock_send_file_to_s3.assert_called()

    @patch('obfuscate_gps_handler.send_file_to_s3')
    @patch('obfuscate_gps_handler.handle_gps_coordinates')
    def test_obfuscateGPS_givenMultipleSamplesWithLatLong_thenCalledObfuscateGPSCoordinates(
            self, mock_obfuscate_gps_coordinates, mock_send_file_to_s3):
        print("<-----test_obfuscate_gps_givenMultipleSamplesWithLatLong_thenCalledObfuscateGPSCoordinates----->")
        body = {"telematicsDeviceId": "1234567890",
                "samples": [{"dateTimestamp": "2020-10-08T14:26:58.456Z",
                             "convertedDeviceParameters": {"messageID": "message_id", "Latitude": "-39.3456789",
                                                           "Longitude": "30.9876543"}},
                            {"dateTimestamp": "2020-10-08T14:26:58.456Z",
                             "convertedDeviceParameters": {"messageID": "message_id", "Latitude": "-39.3456789",
                                                           "Longitude": "30.9876543"}}]}
        mock_obfuscate_gps_coordinates.side_effect = [("-12.345", "12.345"), ("-12.345", "12.345")]
        result = obfuscate_gps(body)
        print("Result: ", result)
        mock_obfuscate_gps_coordinates.assert_called()
        mock_send_file_to_s3.assert_called()

    @mock_s3
    @patch.dict('os.environ', {'j1939_end_bucket': 'test_bucket'})
    def test_sendFileToS3_givenValidBody_thenPutFileIntoBucket(self):
        print("<-----test_sendFileToS3_givenValidBody_thenPutFileIntoBucket----->")
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=os.environ['j1939_end_bucket'])
        body = {"componentSerialNumber": "10290001", "telematicsPartnerName": "Cummins",
                "dataSamplingConfigId": "SC5004", "telematicsDeviceId": "102900000000001",
                "samples": [{"dateTimestamp": "2020-10-08T14:26:58.456Z",
                             "convertedDeviceParameters": {"messageID": "message_id", "Latitude": "-39.3456789",
                                                           "Longitude": "30.9876543"}}]}
        result = send_file_to_s3(body)
        print("Result: ", result)
        bucket = boto3.resource('s3').Bucket(os.environ['j1939_end_bucket'])
        bucket.objects.all().delete()
        bucket.delete()

    @mock_s3
    @patch.dict('os.environ', {'j1939_end_bucket': 'test_bucket'})
    def test_sendFileToS3_givenValidBodyAndEsnWithAsterisk_thenPutFileIntoBucket(self):
        print("<-----test_sendFileToS3_givenValidBody_thenPutFileIntoBucket----->")
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=os.environ['j1939_end_bucket'])
        body = {"componentSerialNumber": "CMMS*MODEL**10290001***", "telematicsPartnerName": "Cummins",
                "dataSamplingConfigId": "SC5004", "telematicsDeviceId": "102900000000001",
                "samples": [{"dateTimestamp": "2020-10-08T14:26:58.456Z",
                             "convertedDeviceParameters": {"messageID": "message_id", "Latitude": "-39.3456789",
                                                           "Longitude": "30.9876543"}}]}
        result = send_file_to_s3(body)
        print("Result: ", result)
        bucket = boto3.resource('s3').Bucket(os.environ['j1939_end_bucket'])
        bucket.objects.all().delete()
        bucket.delete()

    @patch.dict('os.environ', {'j1939_end_bucket': 'test_bucket'})
    def test_sendFileToS3_givenErrorOccurredWhileStoringFile_thenRaiseException(self):
        print("<-----test_sendFileToS3_givenErrorOccurredWhileStoringFile_thenRaiseException----->")
        body = {"componentSerialNumber": "10290001", "telematicsPartnerName": "Cummins",
                "telematicsDeviceId": "102900000000001",
                "samples": [{"dateTimestamp": "2020-10-08T14:26:58.456Z",
                             "convertedDeviceParameters": {"messageID": "message_id", "Latitude": "-39.3456789",
                                                           "Longitude": "30.9876543"}}]}
        result = send_file_to_s3(body)
        print("Result: ", result)
