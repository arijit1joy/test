import os
import sys
import unittest
from unittest.mock import patch, MagicMock

from tests.cda_module_mock_context import CDAModuleMockingContext

sys.path.append("../")

with CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict("os.environ", {
            "edgeCommonAPIURL": "testurl","spn_parameter_json_object": "test","spn_parameter_json_object_key": "test",
            "cd_url": "test_url","converted_equip_params": "1","converted_device_params": "1","converted_equip_fc": "1",
            "class_arg_map": "1","time_stamp_param": "2","active_fault_code_indicator": "2","inactive_fault_code_indicator": "2",
            "param_indicator": "2","notification_version": "2","message_format_version_indicator": "2","spn_indicator": "1",
            "fmi_indicator": "2","count_indicator": "2","active_cd_parameter": "active_cd_parameter","MaxAttempts": "2","s3": "test",
            "LoggingLevel": "debug","PTxAPIKey": "testKey","Region": "us-east-1","region": "us-east-1","metaWriteQueueUrl":"metaWriteQueueUrl"
        }):
    cda_module_mock_context.mock_module("authtoken_jfrog_artifacts")
    cda_module_mock_context.mock_module("commonlib_jfrog_artifacts")
    cda_module_mock_context.mock_module("edge_db_lambda_client")
    cda_module_mock_context.mock_module('edge_core_layer.edge_logger')
    cda_module_mock_context.mock_module('edge_core_layer.edge_core')
    cda_module_mock_context.mock_module('edge_core_layer.edge_errors')
    cda_module_mock_context.mock_module('edge_core_layer.edge_s3')
    cda_module_mock_context.mock_module('edge_core_layer.edge_tata')
    cda_module_mock_context.mock_module('edge_sqs_utility_layer.sqs_utility')
    cda_module_mock_context.mock_module('lambda_cache')
    cda_module_mock_context.mock_module('metadata_utility')
    cda_module_mock_context.mock_module('obfuscate_gps_utility')
    cda_module_mock_context.mock_module('aws_utils')
    cda_module_mock_context.mock_module("boto3")
    cda_module_mock_context.mock_module('cd_sdk_conversion.cd_sdk')
    cda_module_mock_context.mock_module('edge_db_utility_layer.metadata_utility')

    import conversion


class TestConversion(unittest.TestCase):

    @patch("conversion.s3_client")
    @patch("conversion.json")
    @patch.dict('os.environ', {'metaWriteQueueUrl': 'metaWriteQueueUrl','AuditTrailQueueUrl':'AuditTrailQueueUrl','QueueUrl':'QueueUrl'})
    def test_retrieve_and_process_file_when_no_samples(self,json, s3_client):
        print("<---------- test_retrieve_and_process_file_when_no_samples ---------->")

        body = {
            "messageFormatVersion": "1.1.1",
            "telematicsPartnerName": "Accolade",
            "customerReference": "TataMotors",
            "componentSerialNumber": "30311606",
            "equipmentId": "",
            "vin": "",
            "telematicsDeviceId": "864337059675703",
            "dataSamplingConfigId": "SC3078",
            "dataEncryptionSchemeId": "ES1",
            "numberOfSamples": 1,
            "samples": []
        }
        patch("builtins.open", MagicMock())
        fetch_cs_reg_payload = open(os.path.join("tests", "test.json"), "r")

        uploaded_file_object = {"source_bucket_name": "test", "file_key": "edge_864337059675703_30311606_20230424064925_SC3078_2023-04-24T06_49_25.956Z",
                                "file_size": "1","sqs_receipt_handle":"sqs_receipt_handle" }
        s3_object =  {"Metadata": {"uuid":"469448c0-e34e-11ed-b5ea-0242ac120002","j1939type":"FC"},
                                             "LastModified": "edge_864337059675703_30311606_20230424064925_SC3078_2023-04-24T06_49_25.956Z",
                      "Body": fetch_cs_reg_payload,
                      "sqs_receipt_handle":"sqs_receipt_handle"
                }
        s3_client.get_object.return_value = s3_object
        json.loads.return_value = body
        conversion.retrieve_and_process_file(uploaded_file_object,"")

    @patch("conversion.s3_client")
    @patch("conversion.json")
    @patch.dict('os.environ', {'metaWriteQueueUrl': 'metaWriteQueueUrl', 'AuditTrailQueueUrl': 'AuditTrailQueueUrl',
                               'QueueUrl': 'QueueUrl'})
    def test_retrieve_and_process_file_when_no_meta_data(self, json, s3_client):
        print("<---------- test_retrieve_and_process_file_when_no_meta_data ---------->")

        body = {
            "messageFormatVersion": "1.1.1",
            "telematicsPartnerName": "Accolade",
            "customerReference": "TataMotors",
            "componentSerialNumber": "30311606",
            "equipmentId": "",
            "vin": "",
            "telematicsDeviceId": "864337059675703",
            "dataSamplingConfigId": "SC3078",
            "dataEncryptionSchemeId": "ES1",
            "numberOfSamples": 1,
            "sample": []
        }
        patch("builtins.open", MagicMock())

        fetch_cs_reg_payload = open(os.path.join("tests", "test.json"), "r")

        uploaded_file_object = {"source_bucket_name": "test",
                                "file_key": "edge_864337059675703_30311606_20230424064925_SC3078_2023-04-24T06_49_25.956Z",
                                "file_size": "1", "sqs_receipt_handle": "sqs_receipt_handle"}
        s3_object = {"Metadata": {"uuid": "469448c0-e34e-11ed-b5ea-0242ac120002", "j1939type": "FC"},
                     "LastModified": "edge_864337059675703_30311606_20230424064925_SC3078_2023-04-24T06_49_25.956Z",
                     "Body": fetch_cs_reg_payload,
                     "sqs_receipt_handle": "sqs_receipt_handle"
                     }
        s3_client.get_object.return_value = s3_object
        json.loads.return_value = body
        conversion.get_metadata_info.return_value = None

        conversion.retrieve_and_process_file(uploaded_file_object, "")

    @patch("cd_sdk_conversion.cd_sdk.map_ngdi_sample_to_cd_payload")
    @patch("conversion.json")
    @patch.dict('os.environ', {'metaWriteQueueUrl': 'metaWriteQueueUrl', 'AuditTrailQueueUrl': 'AuditTrailQueueUrl',
                               'QueueUrl': 'QueueUrl'})
    def test_handle_exception_for_hb_when_cust_ref_is_tata_motors(self, json, mock_map_ngdi_sample_to_cd_payload):
        print("<---------- test_handle_exception_for_hb_when_cust_ref_is_tata_motors ---------->")

        conversion.get_metadata_info.return_value = None
        converted_device_params = {}
        converted_equip_params =  {}
        converted_fc = {}
        meta_data = {'componentSerialNumber': '67384774', 'telematicsPartnerName': 'TataMotors',
                     'dataSamplingConfigId': 'SC3141', 'dataEncryptionSchemeId': 'temp', 'equipmentId': 'COSMOS_56890121',
                     'messageFormatVersion': '1.1.1', 'numberofSamples': 1, 'telematicsDeviceId': '1208a80e828bb1b7',
                     'customerReference': 'TataMotors','vin': '2XTESTTEAMS917257'}
        conversion.map_ngdi_sample_to_cd_payload.return_value = Exception
        conversion.handle_hb(converted_device_params, converted_equip_params, converted_fc,meta_data ,"" )

    @patch("conversion.LOGGER")
    @patch.dict('os.environ', {'metaWriteQueueUrl': 'metaWriteQueueUrl', 'AuditTrailQueueUrl': 'AuditTrailQueueUrl',
                               'QueueUrl': 'QueueUrl', 'class_arg_map': ''})
    def test_handle_exception_for_fc_when_cust_ref_is_tata_motors(self, LOGGER_mock):
        print("<---------- test_handle_exception_for_fc_when_cust_ref_is_tata_motors ---------->")

        converted_device_params = None
        converted_equip_params = None
        converted_fc = None
        LOGGER_mock.info.side_effect = Exception
        meta_data = {'componentSerialNumber': '67384774', 'telematicsPartnerName': 'TataMotors',
                     'dataSamplingConfigId': 'SC3141', 'customerReference': 'TataMotors',
                     'telematicsDeviceId': '1208a80e828bb1b7', 'vin': '2XTESTTEAMS917257'}
        conversion.handle_fc(converted_device_params, converted_equip_params, converted_fc, meta_data, "")

    @patch("conversion.LOGGER")
    @patch.dict('os.environ', {'metaWriteQueueUrl': 'metaWriteQueueUrl', 'AuditTrailQueueUrl': 'AuditTrailQueueUrl',
                               'QueueUrl': 'QueueUrl', 'class_arg_map': ''})
    def test_handle_exception_for_fc_when_cust_ref_is_cummins(self, LOGGER_mock):
        print("<---------- test_handle_exception_for_fc_when_cust_ref_is_cummins ---------->")

        converted_device_params = None
        converted_equip_params = None
        converted_fc = None
        LOGGER_mock.info.side_effect = Exception
        meta_data = {'componentSerialNumber': '67384774', 'telematicsPartnerName': 'TataMotors',
                     'dataSamplingConfigId': 'SC3141', 'customerReference': '',
                     'telematicsDeviceId': '1208a80e828bb1b7', 'vin': '2XTESTTEAMS917257'}
        conversion.handle_fc(converted_device_params, converted_equip_params, converted_fc, meta_data, "")

    @patch("conversion.s3_client")
    @patch("conversion.json")
    @patch.dict('os.environ', {'metaWriteQueueUrl': 'metaWriteQueueUrl', 'AuditTrailQueueUrl': 'AuditTrailQueueUrl',
                               'QueueUrl': 'QueueUrl'})
    def test_retrieve_and_process_file_when_cust_ref_is_cummins(self, json, s3_client):
        print("<---------- test_retrieve_and_process_file_when_cust_ref_is_cummins ---------->")

        body = {
            "messageFormatVersion": "1.1.1",
            "telematicsPartnerName": "Accolade",
            "customerReference": "Cummins",
            "componentSerialNumber": "30311606",
            "equipmentId": "",
            "vin": "",
            "telematicsDeviceId": "864337059675703",
            "dataSamplingConfigId": "SC3078",
            "dataEncryptionSchemeId": "ES1",
            "numberOfSamples": 1,
            "samples": []
        }
        patch("builtins.open", MagicMock())
        fetch_cs_reg_payload = open(os.path.join("tests", "test.json"), "r")

        uploaded_file_object = {"source_bucket_name": "test",
                                "file_key": "edge_864337059675703_30311606_20230424064925_SC3078_2023-04-24T06_49_25.956Z",
                                "file_size": "1", "sqs_receipt_handle": "sqs_receipt_handle"}
        s3_object = {"Metadata": {"uuid": "469448c0-e34e-11ed-b5ea-0242ac120002", "j1939type": "FC"},
                     "LastModified": "edge_864337059675703_30311606_20230424064925_SC3078_2023-04-24T06_49_25.956Z",
                     "Body": fetch_cs_reg_payload,
                     "sqs_receipt_handle": "sqs_receipt_handle"
                     }
        s3_client.get_object.return_value = s3_object
        json.loads.return_value = body
        conversion.retrieve_and_process_file(uploaded_file_object, "")

    @patch.dict('os.environ', {'metaWriteQueueUrl': 'metaWriteQueueUrl', 'AuditTrailQueueUrl': 'AuditTrailQueueUrl',
                               'QueueUrl': 'QueueUrl'})
    @patch('conversion.handle_hb')
    @patch("conversion.s3_client.get_object")
    @patch("conversion.json.loads")
    def test_retrieve_and_process_file_when_tsp_name_is_cospa(self, json, mock_s3_client, mock_handle_hb):
        print("<---------- test_retrieve_and_process_file_when_tsp_name_is_cospa ---------->")

        body = {
            "messageFormatVersion": "1.1.1",
            "telematicsPartnerName": "COSPA",
            "customerReference": "Cummins",
            "componentSerialNumber": "30311606",
            "equipmentId": "",
            "vin": "",
            "telematicsDeviceId": "864337059675703",
            "dataSamplingConfigId": "SC3078",
            "dataEncryptionSchemeId": "ES1",
            "numberOfSamples": 1,
            "samples": [{"dateTimestamp": "2020-10-08T14:26:58.456Z",
                             "convertedDeviceParameters": {"messageID": "message_id", "Longitude": "30.9876543"}}]
        }
        patch("builtins.open", MagicMock())
        fetch_cs_reg_payload = open(os.path.join("tests", "test.json"), "r")

        uploaded_file_object = {"source_bucket_name": "test",
                                "file_key": "edge_864337059675703_30311606_20230424064925_SC3078_2023-04-24T06_49_25.956Z",
                                "file_size": "1", "sqs_receipt_handle": "sqs_receipt_handle"}
        s3_object = {"Metadata": {"uuid": "469448c0-e34e-11ed-b5ea-0242ac120002", "j1939type": "HB"},
                     "LastModified": "edge_864337059675703_30311606_20230424064925_SC3078_2023-04-24T06_49_25.956Z",
                     "Body": fetch_cs_reg_payload,
                     "sqs_receipt_handle": "sqs_receipt_handle"
                     }
        mock_s3_client.return_value = s3_object
        json.return_value = body
        conversion.retrieve_and_process_file(uploaded_file_object, "")
        mock_handle_hb.assert_not_called()


    @patch.dict('os.environ', {'metaWriteQueueUrl': 'metaWriteQueueUrl', 'AuditTrailQueueUrl': 'AuditTrailQueueUrl',
                               'QueueUrl': 'QueueUrl'})
    @patch('conversion.handle_hb')
    @patch("conversion.s3_client.get_object")
    @patch("conversion.json.loads")
    def test_retrieve_and_process_file_when_tsp_name_is_not_cospa(self, json, mock_s3_client, mock_handle_hb):
        print("<---------- test_retrieve_and_process_file_when_tsp_name_is_not_cospa ---------->")

        body = {
            "messageFormatVersion": "1.1.1",
            "telematicsPartnerName": "Accolade",
            "customerReference": "Cummins",
            "componentSerialNumber": "30311606",
            "equipmentId": "",
            "vin": "",
            "telematicsDeviceId": "864337059675703",
            "dataSamplingConfigId": "SC3078",
            "dataEncryptionSchemeId": "ES1",
            "numberOfSamples": 1,
            "samples": [{"dateTimestamp": "2020-10-08T14:26:58.456Z",
                             "convertedDeviceParameters": {"messageID": "message_id", "Longitude": "30.9876543"}}]
        }
        patch("builtins.open", MagicMock())
        fetch_cs_reg_payload = open(os.path.join("tests", "test.json"), "r")

        uploaded_file_object = {"source_bucket_name": "test",
                                "file_key": "edge_864337059675703_30311606_20230424064925_SC3078_2023-04-24T06_49_25.956Z",
                                "file_size": "1", "sqs_receipt_handle": "sqs_receipt_handle"}
        s3_object = {"Metadata": {"uuid": "469448c0-e34e-11ed-b5ea-0242ac120002", "j1939type": "HB"},
                     "LastModified": "edge_864337059675703_30311606_20230424064925_SC3078_2023-04-24T06_49_25.956Z",
                     "Body": fetch_cs_reg_payload,
                     "sqs_receipt_handle": "sqs_receipt_handle"
                     }
        mock_s3_client.return_value = s3_object
        json.return_value = body
        conversion.retrieve_and_process_file(uploaded_file_object, "")
        mock_handle_hb.assert_called_once()
