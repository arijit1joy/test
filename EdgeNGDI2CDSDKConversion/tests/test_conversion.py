import json
import os
import sys
import unittest
from unittest.mock import ANY, patch, MagicMock

from tests.cda_module_mock_context import CDAModuleMockingContext

sys.path.append("../")

with CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict("os.environ", {
    "spn_parameter_json_object": "test", "spn_parameter_json_object_key": "test",
    "cd_url": "test_url", "converted_equip_params": "0", "converted_device_params": "1", "converted_equip_fc": "2",
    "class_arg_map": "1", "time_stamp_param": "2", "active_fault_code_indicator": "2",
    "inactive_fault_code_indicator": "3",
    "param_indicator": "2", "notification_version": "2", "message_format_version_indicator": "2", "spn_indicator": "1",
    "fmi_indicator": "2", "count_indicator": "3", "active_cd_parameter": "active_cd_parameter", "MaxAttempts": "2",
    "s3": "test",
    "LoggingLevel": "debug", "PTxAPIKey": "testKey", "Region": "us-east-1", "region": "us-east-1",
    "metaWriteQueueUrl": "metaWriteQueueUrl"
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
    cda_module_mock_context.mock_module('edge_db_utility_layer.obfuscate_gps_utility')
    cda_module_mock_context.mock_module('aws_utils')
    cda_module_mock_context.mock_module("boto3")
    cda_module_mock_context.mock_module("requests")
    cda_module_mock_context.mock_module('cd_sdk_conversion.cd_sdk')
    cda_module_mock_context.mock_module('edge_db_utility_layer.metadata_utility')

    import conversion


class TestConversion(unittest.TestCase):

    @patch("conversion.s3_client")
    @patch("conversion.json")
    @patch.dict('os.environ', {'metaWriteQueueUrl': 'metaWriteQueueUrl', 'AuditTrailQueueUrl': 'AuditTrailQueueUrl',
                               'QueueUrl': 'QueueUrl'})
    def test_retrieve_and_process_file_when_no_samples(self, json, s3_client):
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
        conversion.retrieve_and_process_file(uploaded_file_object)

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

        conversion.retrieve_and_process_file(uploaded_file_object)

    @patch("cd_sdk_conversion.cd_sdk.map_ngdi_sample_to_cd_payload")
    @patch("conversion.json")
    @patch.dict('os.environ', {'metaWriteQueueUrl': 'metaWriteQueueUrl', 'AuditTrailQueueUrl': 'AuditTrailQueueUrl',
                               'QueueUrl': 'QueueUrl'})
    def test_handle_exception_for_hb_when_cust_ref_is_tata_motors(self, json, mock_map_ngdi_sample_to_cd_payload):
        print("<---------- test_handle_exception_for_hb_when_cust_ref_is_tata_motors ---------->")

        conversion.get_metadata_info.return_value = None
        converted_device_params = {}
        converted_equip_params = {}
        converted_fc = {}
        meta_data = {'componentSerialNumber': '67384774', 'telematicsPartnerName': 'TataMotors',
                     'dataSamplingConfigId': 'SC3141', 'dataEncryptionSchemeId': 'temp',
                     'equipmentId': 'COSMOS_56890121',
                     'messageFormatVersion': '1.1.1', 'numberofSamples': 1, 'telematicsDeviceId': '1208a80e828bb1b7',
                     'customerReference': 'TataMotors', 'vin': '2XTESTTEAMS917257'}
        conversion.map_ngdi_sample_to_cd_payload.return_value = Exception
        conversion.handle_hb(converted_device_params, converted_equip_params, converted_fc, meta_data, "")

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
        conversion.retrieve_and_process_file(uploaded_file_object)

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
        conversion.retrieve_and_process_file(uploaded_file_object)
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
        conversion.retrieve_and_process_file(uploaded_file_object)
        mock_handle_hb.assert_called_once()

    @patch.dict("os.environ", {"QueueUrl": "url"})
    @patch("conversion.boto3.client")
    def test_delete_message_from_sqs_queue_successful(self, mock_boto3_client):
        """
        Test for delete_message_from_sqs_queue() running successfully.
        """
        mock_sqs_client = mock_boto3_client.return_value
        mock_sqs_client.delete_message.return_value = "response"

        response = conversion.delete_message_from_sqs_queue("receipt-handle")

        mock_sqs_client.delete_message.assert_called_with(QueueUrl="url", ReceiptHandle="receipt-handle")
        self.assertEqual(response, "response")

    def test_get_metadata_info_successful(self):
        """
        Test for get_metadata_info() running successfully.
        """
        response = conversion.get_metadata_info({"samples": "a", "b": "c"})
        self.assertEqual(response, {"b": "c"})

        response = conversion.get_metadata_info(3)
        self.assertFalse(response)

    @patch("conversion.requests")
    def test__post_cd_message_successful(self, mock_requests):
        """
        Test for _post_cd_message() running successfully.
        """
        conversion._post_cd_message("url", "data")

        mock_requests.post.assert_called_with(url="url", json="data")

    @patch("conversion.requests")
    def test__post_cd_message_on_error(self, mock_requests):
        """
        Test for _post_cd_message() when it throws an exception.
        """
        mock_requests.post.side_effect = Exception

        with self.assertRaises(Exception):
            conversion._post_cd_message("url", "data")

    @patch("conversion.auth_utility")
    @patch("conversion._post_cd_message")
    def test_post_cd_message_successful(self, mock_post_helper, mock_auth_utility):
        """
        Test for post_cd_message() running successfully.
        """
        mock_auth_utility.generate_auth_token.return_value = "auth"

        conversion.post_cd_message({
            "Telematics_Partner_Name": "Cummins",
            "Telematics_Box_ID": "box-id",
            "Engine_Serial_Number": "esn",
            "VIN": "",
            "Equipment_ID": ""
        })

        mock_auth_utility.generate_auth_token.assert_called_with("Cummins")
        mock_post_helper.assert_called_with(
            "test_urlauth",
            {
                "Telematics_Partner_Name": "Cummins",
                "Telematics_Box_ID": "box-id",
                "Engine_Serial_Number": "esn",
                "VIN": "",
                "Equipment_ID": "EDGE_esn",
                "Sent_Date_Time": ANY
            }
        )

    def test_get_active_faults_successful(self):
        """
        Test for get_active_faults() running successfully.
        """
        response = conversion.get_active_faults(
            [{"spn": "spn", "fmi": "fmi"}],
            "address"
        )

        self.assertEqual(response, [{"Fault_Source_Address": "address", "SPN": "spn", "FMI": "fmi"}])

    @patch("conversion.get_active_faults")
    def test_process_hb_param_successful(self, mock_get_active_faults):
        """
        Test for process_hb_param() running successfully.
        """
        mock_get_active_faults.return_value = "active-faults"

        response = conversion.process_hb_param(
            "fc-param",
            {"fc-param": "param"},
            "address",
            dict(),
            {"fc-param": "value"}
        )

        mock_get_active_faults.assert_called_with("param", "address")
        self.assertEqual(response, {"value": "active-faults"})

    @patch("conversion.get_active_faults")
    @patch("conversion.create_fc_class")
    def test_process_fc_param_active_successful(self, mock_create_fc_class, mock_get_active_faults):
        """
        Test for process_fc_param() running successfully for active fault codes.
        """
        mock_get_active_faults.return_value = "active-faults"
        response = conversion.process_fc_param(
            "2",
            {"2": ["2"]},
            "address",
            {"2": "2"},
            dict(),
            False
        )

        mock_get_active_faults.assert_called_with(["2"], "address")
        mock_create_fc_class.assert_called_with("2", "active-faults", 0, "2", {}, 1)
        self.assertEqual(response, ({}, True))

    @patch("conversion.get_active_faults")
    @patch("conversion.create_fc_class")
    def test_process_fc_param_inactive_successful(self, mock_create_fc_class, mock_get_active_faults):
        """
        Test for process_fc_param() running successfully for inactive fault codes.
        """
        mock_get_active_faults.return_value = "inactive-faults"
        response = conversion.process_fc_param(
            "3",
            {"3": ["3"]},
            "address",
            {"3": "3"},
            dict(),
            False
        )

        mock_get_active_faults.assert_called()
        mock_create_fc_class.assert_called_with("3", "inactive-faults", 0, "3", {}, 0, "inactive-faults")
        self.assertEqual(response, ({}, True))

    @patch("conversion.class_arg_map", {"2": "arg", "arg": "barg"})
    def test_process_hb_fc_apply_class_arg_map_successful(self):
        """
        Test for process_hb_fc_apply_class_arg_map() running successfully.
        """
        response = conversion.process_hb_fc_apply_class_arg_map("arg", {"arg": "marg"}, dict())
        self.assertEqual(response, {"barg": "marg"})

        response = conversion.process_hb_fc_apply_class_arg_map("2", {"arg": "marg"}, dict())
        self.assertEqual(response, {"arg": "2"})

    def test_process_hb_fc_non_time_stamp_device_param_successful(self):
        """
        Test for process_hb_fc_non_time_stamp_device_param() running successfully.
        """
        response = conversion.process_hb_fc_non_time_stamp_device_param(
            {"param": {"0": "0"}},
            "param",
            dict(),
            []
        )
        self.assertEqual(response, {"0": ""})

    @patch("conversion.spn_file_json", "file")
    @patch("conversion.get_snapshot_data")
    def test_process_hb_fc_non_time_stamp_equip_param_successful(self, mock_get_snapshot_data):
        """
        Test for process_hb_fc_non_time_stamp_equip_param() running successfully.
        """
        mock_get_snapshot_data.return_value = "result"

        response = conversion.process_hb_fc_non_time_stamp_equip_param(
            {"param": [{"2": "2"}]},
            "param",
            dict(),
            {"2": ["2"]},
            "timestamp"
        )

        mock_get_snapshot_data.assert_called_with(["2"], "timestamp", "", "file")
        self.assertEqual(response, {"2": "result"})

        response = conversion.process_hb_fc_non_time_stamp_equip_param(
            {"param": [{"0": "0"}]},
            "param",
            dict(),
            {"0": "0"},
            "timestamp"
        )
        self.assertEqual(response, {"0": "0"})

    @patch("conversion.process_hb_fc_non_time_stamp_device_param")
    @patch("conversion.process_hb_fc_non_time_stamp_equip_param")
    @patch("conversion.process_hb_param")
    @patch("conversion.process_fc_param")
    def test_process_hb_fc_non_time_stamp_param_device_successful(
            self,
            mock_process_fc_param,
            mock_process_hb_param,
            mock_process_hb_fc_non_time_stamp_equip_param,
            mock_process_hb_fc_non_time_stamp_device_param
    ):
        """
        Test for process_hb_fc_non_time_stamp_param() running successfully for device.
        """
        mock_process_hb_fc_non_time_stamp_device_param.return_value = "val"
        mock_process_fc_param.return_value = ("val", False)
        var_dict = dict()

        response = conversion.process_hb_fc_non_time_stamp_param(
            var_dict,
            "1",
            "timestamp",
            "converted-device-params",
            "converted-equip-params",
            "converted-equip-fc",
            {"1": [{"0": "0"}]},
            True
        )

        mock_process_hb_fc_non_time_stamp_device_param.assert_called_with(
            {"1": [{"0": "0"}]},
            "1",
            var_dict,
            "converted-device-params"
        )
        mock_process_fc_param.assert_not_called()
        mock_process_hb_fc_non_time_stamp_equip_param.assert_not_called()
        mock_process_hb_param.assert_not_called()

        self.assertEqual(response, ("val", True))

    @patch("conversion.process_hb_fc_non_time_stamp_device_param")
    @patch("conversion.process_hb_fc_non_time_stamp_equip_param")
    @patch("conversion.process_hb_param")
    @patch("conversion.process_fc_param")
    def test_process_hb_fc_non_time_stamp_param_equip_successful(
            self,
            mock_process_fc_param,
            mock_process_hb_param,
            mock_process_hb_fc_non_time_stamp_equip_param,
            mock_process_hb_fc_non_time_stamp_device_param
    ):
        """
        Test for process_hb_fc_non_time_stamp_param() running successfully for equipment.
        """
        mock_process_hb_fc_non_time_stamp_equip_param.return_value = "val"
        mock_process_hb_param.return_value = ("val", False)
        var_dict = dict()

        response = conversion.process_hb_fc_non_time_stamp_param(
            var_dict,
            "0",
            "timestamp",
            "converted-device-params",
            "converted-equip-params",
            "converted-equip-fc",
            {"0": [{"0": "0"}]},
            True,
            True
        )

        mock_process_hb_fc_non_time_stamp_equip_param.assert_called_with(
            {"0": [{"0": "0"}]},
            "0",
            var_dict,
            "converted-equip-params",
            "timestamp"
        )
        mock_process_hb_param.assert_not_called()
        mock_process_hb_fc_non_time_stamp_device_param.assert_not_called()
        mock_process_fc_param.assert_not_called()

        self.assertEqual(response, ("val", True))

    @patch("conversion.process_hb_fc_non_time_stamp_device_param")
    @patch("conversion.process_hb_fc_non_time_stamp_equip_param")
    @patch("conversion.process_hb_param")
    @patch("conversion.process_fc_param")
    def test_process_hb_fc_non_time_stamp_param_hb_successful(
            self,
            mock_process_fc_param,
            mock_process_hb_param,
            mock_process_hb_fc_non_time_stamp_equip_param,
            mock_process_hb_fc_non_time_stamp_device_param
    ):
        """
        Test for process_hb_fc_non_time_stamp_param() running successfully for HB.
        """
        mock_process_hb_param.return_value = "val"
        var_dict = dict()

        response = conversion.process_hb_fc_non_time_stamp_param(
            var_dict,
            "4",
            "timestamp",
            "converted-device-params",
            "converted-equip-params",
            "converted-equip-fc",
            {"4": [{"0": "0"}]},
            True,
            True
        )

        mock_process_hb_param.assert_called_with(
            "0",
            "converted-equip-fc",
            "",
            var_dict,
            {"0": "0"}
        )
        mock_process_hb_fc_non_time_stamp_device_param.assert_not_called()
        mock_process_hb_fc_non_time_stamp_equip_param.assert_not_called()
        mock_process_fc_param.assert_not_called()

        self.assertEqual(response, ("val", True))

    @patch("conversion.process_hb_fc_non_time_stamp_device_param")
    @patch("conversion.process_hb_fc_non_time_stamp_equip_param")
    @patch("conversion.process_hb_param")
    @patch("conversion.process_fc_param")
    def test_process_hb_fc_non_time_stamp_param_fc_successful(
            self,
            mock_process_fc_param,
            mock_process_hb_param,
            mock_process_hb_fc_non_time_stamp_equip_param,
            mock_process_hb_fc_non_time_stamp_device_param
    ):
        """
        Test for process_hb_fc_non_time_stamp_param() running successfully for FC.
        """
        mock_process_fc_param.return_value = ("val", False)
        var_dict = dict()

        response = conversion.process_hb_fc_non_time_stamp_param(
            var_dict,
            "4",
            "timestamp",
            "converted-device-params",
            "converted-equip-params",
            "converted-equip-fc",
            {"4": [{"0": "0"}]},
            True
        )

        mock_process_fc_param.assert_called_with(
            "0",
            "converted-equip-fc",
            "",
            {"0": "0"},
            var_dict,
            True
        )
        mock_process_hb_fc_non_time_stamp_device_param.assert_not_called()
        mock_process_hb_fc_non_time_stamp_equip_param.assert_not_called()
        mock_process_hb_param.assert_not_called()

        self.assertEqual(response, ("val", False))

    @patch("conversion.class_arg_map", {"arg": {"2": "0"}})
    @patch("conversion.process_hb_fc_apply_class_arg_map")
    @patch("conversion.process_hb_fc_non_time_stamp_param")
    def test_process_hb_fc_timestamp_successful(
            self,
            mock_process_hb_fc_non_time_stamp_param,
            mock_process_hb_fc_apply_class_arg_map
    ):
        """
        Test for process_hb_fc() running successfully for timestamp.
        """
        var_dict = dict()

        mock_process_hb_fc_apply_class_arg_map.return_value = var_dict

        response = conversion.process_hb_fc(
            var_dict,
            "metadata",
            "timestamp",
            "converted-device-params",
            "converted_equip_params",
            "converted_equip_fc"
        )

        mock_process_hb_fc_apply_class_arg_map.assert_called_with("arg", "metadata", var_dict)
        mock_process_hb_fc_non_time_stamp_param.assert_not_called()
        self.assertEqual(response, ({"0": "timestamp"}, False))

    @patch("conversion.class_arg_map", {"arg": {"param": "0"}})
    @patch("conversion.process_hb_fc_apply_class_arg_map")
    @patch("conversion.process_hb_fc_non_time_stamp_param")
    def test_process_hb_fc_non_timestamp_successful(
            self,
            mock_process_hb_fc_non_time_stamp_param,
            mock_process_hb_fc_apply_class_arg_map
    ):
        """
        Test for process_hb_fc() running successfully for non-timestamp.
        """
        var_dict = dict()

        mock_process_hb_fc_apply_class_arg_map.return_value = var_dict
        mock_process_hb_fc_non_time_stamp_param.return_value = ("val", True)

        response = conversion.process_hb_fc(
            var_dict,
            "metadata",
            "timestamp",
            "converted-device-params",
            "converted_equip_params",
            "converted_equip_fc"
        )

        mock_process_hb_fc_apply_class_arg_map.assert_called_with("arg", "metadata", var_dict)
        mock_process_hb_fc_non_time_stamp_param.assert_called_with(
            var_dict,
            "param",
            "timestamp",
            "converted-device-params",
            "converted_equip_params",
            "converted_equip_fc",
            {"param": "0"},
            False,
            is_hb=False
        )
        self.assertEqual(response, ("val", True))

    @patch("conversion.map_ngdi_sample_to_cd_payload")
    @patch("conversion.post_cd_message")
    def test_create_fc_class_successful(self, mock_post_cd_message, mock_map_fn):
        """
        Test for create_fc_class() running successfully.
        """
        mock_map_fn.return_value = "val"

        conversion.create_fc_class(
            {"SPN": "spn", "FMI": "fmi", "count": 1},
            [0, 1],
            0,
            "param",
            dict(),
            "active"
        )

        mock_map_fn.assert_called_with(
            {
                "param": [1],
                "active_cd_parameter": "active",
                "1": "spn",
                "2": "fmi",
                "3": 1
            },
            fc=True
        )
        mock_post_cd_message.assert_called_with("val")

    @patch("conversion.store_health_parameters_into_redshift")
    @patch("conversion.handle_hb")
    @patch("conversion.handle_fc")
    def test_send_sample_hb_successful(self, mock_handle_fc, mock_handle_hb, mock_store_fn):
        """
        Test for send_sample() running successfully for HB messages.
        """
        conversion.send_sample(
            {"0": [0], "1": {}, "2": []},
            "metadata",
            "HB",
            "Cummins"
        )
        mock_store_fn.assert_called_with({}, [], "metadata")
        mock_handle_hb.assert_called_with({}, 0, [], "metadata", [])
        mock_handle_fc.assert_not_called()

    @patch("conversion.store_health_parameters_into_redshift")
    @patch("conversion.handle_hb")
    @patch("conversion.handle_fc")
    def test_send_sample_fc_successful(self, mock_handle_fc, mock_handle_hb, mock_store_fn):
        """
        Test for send_sample() running successfully for FC messages.
        """
        conversion.send_sample(
            {"0": [0], "1": {}, "2": []},
            "metadata",
            "FC",
            "Cummins"
        )
        mock_handle_fc.assert_called_with({}, 0, [], "metadata", [])
        mock_store_fn.assert_not_called()
        mock_handle_hb.assert_not_called()

    @patch("conversion.send_sample")
    @patch("conversion.process_audit_error")
    @patch("conversion.delete_message_from_sqs_queue")
    def test_handle_metadata_successful(self, mock_delete_fn, mock_process_error, mock_send_sample):
        """
        Test for _handle_metadata() running successfully.
        """
        conversion._handle_metadata(
            "metadata",
            ["sample"],
            "hb",
            "device-id",
            "j1939",
            {"sqs_receipt_handle": "receipt-handle"},
            "j1939-file",
            "tsp-name"
        )

        mock_send_sample.assert_called_with("sample", "metadata", "hb", "tsp-name")
        mock_delete_fn.assert_called_with("receipt-handle")
        mock_process_error.assert_not_called()

    @patch("conversion.send_sample")
    @patch("conversion.process_audit_error")
    @patch("conversion.delete_message_from_sqs_queue")
    def test_handle_metadata_on_error(self, mock_delete_fn, mock_process_error, mock_send_sample):
        """
        Test for _handle_metadata() when sample or metadata is missing.
        """
        conversion._handle_metadata(
            "",
            [],
            "hb",
            "device-id",
            "j1939",
            {"sqs_receipt_handle": "receipt-handle"},
            "j1939-file",
            "tsp-name"
        )

        mock_process_error.assert_called_with(
            error_message=ANY,
            data_protocol="j1939",
            meta_data="j1939-file",
            device_id="device-id"
        )
        mock_delete_fn.assert_not_called()
        mock_send_sample.assert_not_called()

        conversion._handle_metadata(
            "metadata",
            [],
            "hb",
            "device-id",
            "j1939",
            {"sqs_receipt_handle": "receipt-handle"},
            "j1939-file",
            "tsp-name"
        )

        mock_process_error.assert_called_with(
            error_message=ANY,
            data_protocol="j1939",
            meta_data="metadata",
            device_id="device-id"
        )
        mock_delete_fn.assert_called_with("receipt-handle")
        mock_send_sample.assert_not_called()

    # @patch("conversion.boto3.client")
    # @patch("conversion.Process")
    # def test_lambda_handler_successful(self, mock_process, mock_boto3):
    #     """
    #     Test for lambda_handler() running successfully.
    #     """
    #     context = MagicMock(parameters={"EDGECommonAPI": "val"})
    #     lambda_invoke_event = {
    #         "Records": [
    #             {
    #                 "body": json.dumps({
    #                     "Records": [
    #                         {
    #                             "s3": {
    #                                 "object": {"key": "file-key", "size": 100},
    #                                 "bucket": {"name": "bucket"}
    #                             }
    #                         }
    #                     ]
    #                 }),
    #                 "receiptHandle": "receipt-handle"
    #             }
    #         ]
    #     }

    #     mock_ssm_client = mock_boto3.return_value

    #     conversion.lambda_handler(lambda_invoke_event, context)

    #     mock_process.assert_called_with(
    #         target=conversion.retrieve_and_process_file,
    #         args=(
    #             {
    #                 "source_bucket_name": "bucket",
    #                 "file_key": "file-key",
    #                 "file_size": 100,
    #                 "sqs_receipt_handle": "receipt-handle"
    #             },
    #             "EDGECommonAPI"
    #         )
    #     )
    #     mock_process.return_value.start.assert_called()
    #     mock_ssm_client.get_parameters.assert_not_called()

    #     mock_ssm_client.get_parameters.return_value = {"Parameters": [{"Value": "url"}]}

    #     conversion.lambda_handler(lambda_invoke_event, None)

    #     mock_ssm_client.get_parameters.assert_called_with(Names=ANY, WithDecryption=False)
    #     mock_process.assert_called_with(
    #         target=conversion.retrieve_and_process_file,
    #         args=(
    #             {
    #                 "source_bucket_name": "bucket",
    #                 "file_key": "file-key",
    #                 "file_size": 100,
    #                 "sqs_receipt_handle": "receipt-handle"
    #             },
    #             "url"
    #         )
    #     )
    #     mock_process.return_value.start.assert_called()

    def test_resolve_value_from_converted_device_parameters_successful(self):
        """
        Test for resolve_value_from_converted_device_parameters() running successfully.
        """
        response = conversion.resolve_value_from_converted_device_parameters({"k": "v"}, "k")
        self.assertEqual(response, "v")

        response = conversion.resolve_value_from_converted_device_parameters({"v": "v"}, "k")
        self.assertEqual(response, None)

    @patch("conversion.write_health_parameter_to_database_v2")
    def test_store_health_parameters_into_redshift(self, mock_write_fn):
        """
        Test for store_health_parameters_into_redshift() running successfully.

        Assume resolve_value_from_converted_device_parameters() is running as intended.
        """
        conversion.store_health_parameters_into_redshift(
            {
                "messageID": "message-id",
                "CPU_temperature": "cpu-temp",
                "PMIC_temperature": "pmic-temp",
                "Latitude": "lat",
                "Longitude": "long",
                "Altitude": "alt",
                "PDOP": "pdop",
                "Satellites_Used": "sats",
                "LTE_RSSI": "lte-rssi",
                "LTE_RSCP": "lte-rscp",
                "LTE_RSRQ": "lte-rsrq",
                "LTE_RSRP": "lte-rsrp",
                "CPU_Usage_Level": "cpu-level",
                "RAM_Usage_Level": "ram-level",
                "SNR_per_Satellite": "SNR-per-sat"
            },
            "1981-08-03T01:17:04.000Z",
            {"telematicsDeviceId": "device-id", "componentSerialNumber": "esn"}
        )

        mock_write_fn.assert_called_with(
            "message-id",
            "cpu-temp",
            "pmic-temp",
            "lat",
            "long",
            "alt",
            "pdop",
            "sats",
            "lte-rssi",
            "lte-rscp",
            "lte-rsrq",
            "lte-rsrp",
            "cpu-level",
            "ram-level",
            "SNR-per-sat",
            "1981-08-03 01:17:04",
            "device-id",
            "esn"
        )

    @patch("conversion.audit_utility.write_to_audit_table")
    @patch("conversion.write_to_audit_table")
    def test_process_audit_error_successful(self, mock_write_to_audit_table, mock_write_fn):
        """
        Test for process_audit_error() running successfully.
        """
        metadata = {"customerReference": "TATA"}

        conversion.process_audit_error(
            "message",
            module_name=None,
            data_protocol="j1939",
            meta_data=metadata,
            device_id="device-id"
        )

        mock_write_fn.assert_called_with("400", ANY)
        mock_write_to_audit_table.assert_not_called()

    @patch("conversion.audit_utility.write_to_audit_table")
    @patch("conversion.write_to_audit_table")
    def test_process_audit_error_on_error(self, mock_write_to_audit_table, mock_write_fn):
        """
        Test for process_audit_error() when it throws an error.
        """
        conversion.process_audit_error(
            "message",
            module_name="J1939_HB",
            data_protocol="j1939",
            meta_data="",
            device_id="device-id"
        )

        mock_write_to_audit_table.assert_called()
        mock_write_fn.assert_not_called()
