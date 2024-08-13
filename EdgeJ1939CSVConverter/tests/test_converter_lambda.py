import copy
import datetime
import io
import json
import sys
import unittest
from unittest.mock import ANY, MagicMock, patch, call
from moto import mock_aws
import boto3

from resources.cda_module_mocking_context import CDAModuleMockingContext

with CDAModuleMockingContext(sys) as cda_module_mocking_context, patch.dict("os.environ", {
    "PoolSize": "0",
    "CPPostBucket": "CP_file_DUMP",
    "edgeCommonAPIURL": "jdbc:postgresql://<database-endpoint-url>:<port>/<database>",
    "NGDIBody": "{\"Table\": \"CP_FILE_DUMP\", \"ROWS\": 30, \"COLUMNS\": 30}",
    "mapTspFromOwner": json.dumps({"Cummins": "Cummins"}),
    "MaxAttempts": "5",
    "APPLICATION_ENVIRONMENT": "TEST",
    "J1939ActiveFaultCodeTable": "CSVCONVERTER"

}):
    cda_module_mocking_context.mock_module("edge_sqs_utility_layer")
    cda_module_mocking_context.mock_module("edge_db_lambda_client")
    cda_module_mocking_context.mock_module("utility")
    cda_module_mocking_context.mock_module("boto3")
    cda_module_mocking_context.mock_module("botocore.exceptions")
    cda_module_mocking_context.mock_module("aws_utils")
    import ConverterLambda


@mock_aws
class TestConverterLambda(unittest.TestCase):
    def setUp(self):
        """
        Create database resource
        """
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    def tearDown(self):
        """
        Delete database resource
        """
        self.dynamodb = None

    @patch("ConverterLambda.EDGE_DB_CLIENT")
    def test_generate_spn_fmi_fc_obj(self, mock_db_reader):
        print("<---------- test_generate_spn_fmi_fc_obj ---------->")
        # mock_db_reader.execute.return_value = None
        actual_ac_fc = "spn:1001~fmi:4~count:1"
        conv_eq_fc_obj = {"activeFaultCodes": []}
        result = ConverterLambda.generate_spn_fmi_fc_obj(actual_ac_fc, conv_eq_fc_obj)
        self.assertEqual(result, None)

    def test_get_active_fault_codes_from_dynamodb(self):
        print("<---------- test_get_active_fault_codes_from_dynamodb ---------->")

        csv_esn = 123456
        result = ConverterLambda.get_active_fault_codes_from_dynamodb(csv_esn)
        self.assertTrue(result)

    def test_check_active_fault_codes_timestamp_none(self):
        print("<---------- test_check_active_fault_codes_timestamp_none ---------->")

        csv_timestamp = '2023-02-10 10:20:34'
        db_esn_ac_fcs = None
        result = ConverterLambda.check_active_fault_codes_timestamp(db_esn_ac_fcs, csv_timestamp)
        self.assertTrue(result)

    def test_check_active_fault_codes_timestamp_true(self):
        print("<---------- test_check_active_fault_codes_timestamp_true ---------->")

        csv_timestamp = '2023-02-10 10:20:34'
        db_esn_ac_fcs = {}
        db_esn_ac_fcs['timestamp'] = '2023-01-10 10:20:34'
        result = ConverterLambda.check_active_fault_codes_timestamp(db_esn_ac_fcs, csv_timestamp)
        self.assertTrue(result)

    def test_check_active_fault_codes_timestamp_false(self):
        print("<---------- test_check_active_fault_codes_timestamp_false ---------->")

        csv_timestamp = '2023-02-10 10:20:34'
        db_esn_ac_fcs = {}
        db_esn_ac_fcs['timestamp'] = '2023-03-10 10:20:34'
        result = ConverterLambda.check_active_fault_codes_timestamp(db_esn_ac_fcs, csv_timestamp)
        self.assertFalse(result)

    def test_put_active_fault_codes(self):
        print("<---------- test_put_active_fault_codes ---------->")

        ac_fc = {
            "spn:100~fmi:4": 1,
            "spn:2623~fmi:3": 1,
            "spn:101~fmi:5": 1,
            "spn:100~fmi:4": 8
        }
        result = ConverterLambda.put_active_fault_codes(123456, '2023-01-01 10:10:29', ac_fc)
        self.assertTrue(result)

    def test_generate_active_fault_codes(self):
        print("<---------- test_generate_active_fault_codes ---------->")

        from ConverterLambda import put_active_fault_codes
        from ConverterLambda import get_active_fault_codes_from_dynamodb

        expected_result = {
            'activeFaultCodes': [{'spn': '1001', 'fmi': '4', 'count': '1'}, {'spn': '2456', 'fmi': '3', 'count': '5'},
                                 {'spn': '2623', 'fmi': '3', 'count': '2'}]}

        ac_fc = {
            "spn:100~fmi:4": 1,
            "spn:2623~fmi:3": 1,
            "spn:101~fmi:5": 1,
            "spn:100~fmi:4": 8
        }
        ac_fc1 = {
            "spn:100~fmi:4": 1,
            "spn:2623~fmi:3": 1
        }

        put_active_fault_codes(123456, '2023-01-01 10:10:29', ac_fc)
        put_active_fault_codes(1234, '2023-02-01 10:10:29', ac_fc1)

        # GET FAULT FORM DATABSE FOR ESN
        csv_esn = 123456
        # get_result_db = get_active_fault_codes_from_dynamodb(csv_esn)
        get_result_db = {'Item': {'esn': 123456, 'timestamp': '2023-01-01 10:10:29', 'fcs': {'spn:100~fmi:4': 3}},
                         'ResponseMetadata': {'RequestId': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah',
                                              'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'amazon.com',
                                                                                     'x-amzn-requestid': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah',
                                                                                     'x-amz-crc32': '2810715295'},
                                              'RetryAttempts': 0}}

        db_esn_ac_fcs = None
        if 'Item' in get_result_db:
            db_esn_ac_fcs = get_result_db['Item']

        csv_ac_fc = "spn:1001~fmi:4~count:1|spn:2623~fmi:3~count:2|spn:2456~fmi:3~count:5|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []
        csv_timestamp = '2023-02-10 10:20:34'
        result = ConverterLambda.generate_active_fault_codes(csv_esn, csv_ac_fc, conc_eq_fc_obj, db_esn_ac_fcs,
                                                             csv_timestamp)
        self.assertEqual(result, expected_result)

    def test_generate_active_fault_codes_empty_ac_fc(self):
        print("<---------- test_generate_active_fault_codes_empty_ac_fc ---------->")

        conv_eq_fc_obj = {"activeFaultCodes": []}
        esn = 123456
        ac_fc = ""
        db_esn_ac_fcs = None
        timestamp = '2023-02-10 10:20:34'
        expected_conv_eq_fc_obj = {"activeFaultCodes": []}
        result = ConverterLambda.generate_active_fault_codes(esn, ac_fc, conv_eq_fc_obj, db_esn_ac_fcs, timestamp)
        self.assertEqual(result, expected_conv_eq_fc_obj)

    def test_generate_active_fault_codes_new_esn(self):
        print("<---------- test_generate_active_fault_codes_new_esn ---------->")

        conv_eq_fc_obj = {"activeFaultCodes": []}
        esn = 123456
        ac_fc = "spn:1001~fmi:4~count:1|"
        # db_esn_ac_fcs=None
        get_result_db = {'Item': {'esn': 123456, 'timestamp': '2023-01-01 10:10:29', 'fcs': {'spn:100~fmi:4': 3}},
                         'ResponseMetadata': {'RequestId': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah',
                                              'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'amazon.com',
                                                                                     'x-amzn-requestid': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah',
                                                                                     'x-amz-crc32': '2810715295'},
                                              'RetryAttempts': 0}}

        db_esn_ac_fcs = None
        if 'Item' in get_result_db:
            db_esn_ac_fcs = get_result_db['Item']
        timestamp = '2023-02-10 10:20:34'
        expected_conv_eq_fc_obj = {'activeFaultCodes': [{'spn': '1001', 'fmi': '4', 'count': '1'}]}
        result = ConverterLambda.generate_active_fault_codes(esn, ac_fc, conv_eq_fc_obj, db_esn_ac_fcs, timestamp)
        self.assertEqual(result, expected_conv_eq_fc_obj)

    def test_generate_active_fault_codes_duplicate_fc_for_esn(self):
        print("<---------- test_generate_active_fault_codes_duplicate_fc_for_esn ---------->")

        expected_conv_eq_fc_obj = {'activeFaultCodes': []}
        csv_esn = 123456
        csv_ac_fc = "spn:100~fmi:4~count:3|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []
        csv_timestamp = '2023-02-10 10:20:34'
        # GET FAULT FORM DATABSE FOR ESN
        get_result_db = {'Item': {'esn': 123456, 'timestamp': '2023-01-01 10:10:29', 'fcs': {'spn:100~fmi:4': 3}},
                         'ResponseMetadata': {'RequestId': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah',
                                              'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'amazon.com',
                                                                                     'x-amzn-requestid': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah',
                                                                                     'x-amz-crc32': '2810715295'},
                                              'RetryAttempts': 0}}
        db_esn_ac_fcs = None
        if 'Item' in get_result_db:
            db_esn_ac_fcs = get_result_db['Item']

        result = ConverterLambda.generate_active_fault_codes(csv_esn, csv_ac_fc, conc_eq_fc_obj, db_esn_ac_fcs,
                                                             csv_timestamp)
        self.assertEqual(result, expected_conv_eq_fc_obj)

    def test_generate_active_fault_codes_one_new_duplicate_fc_esn(self):
        print("<---------- test_generate_active_fault_codes_one_new_duplicate_fc_esn ---------->")

        expected_conv_eq_fc_obj = {'activeFaultCodes': [{'spn': '1001', 'fmi': '4', 'count': '1'}]}
        csv_esn = 123456

        get_result_db = {'Item': {'esn': 123456, 'timestamp': '2023-01-01 10:10:29', 'fcs': {'spn:100~fmi:4': 3}},
                         'ResponseMetadata': {'RequestId': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah',
                                              'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'amazon.com',
                                                                                     'x-amzn-requestid': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah',
                                                                                     'x-amz-crc32': '2810715295'},
                                              'RetryAttempts': 0}}
        db_esn_ac_fcs = None
        if 'Item' in get_result_db:
            db_esn_ac_fcs = get_result_db['Item']
        csv_ac_fc = "spn:100~fmi:4~count:3|spn:1001~fmi:4~count:1|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []
        csv_timestamp = '2023-02-10 10:20:34'
        result = ConverterLambda.generate_active_fault_codes(csv_esn, csv_ac_fc, conc_eq_fc_obj, db_esn_ac_fcs,
                                                             csv_timestamp)
        self.assertEqual(result, expected_conv_eq_fc_obj)

    def test_generate_active_fault_codes_fc_oocu_esn(self):
        print("<---------- test_generate_active_fault_codes_fc_oocu_esn ---------->")

        expected_conv_eq_fc_obj = {'activeFaultCodes': [{'spn': '100', 'fmi': '4', 'count': '2'}]}
        csv_esn = 123456
        db_esn_ac_fcs = {'esn': 123456, 'timestamp': '2023-01-01 10:10:29', 'fcs': {'spn:100~fmi:4': 1}}
        csv_ac_fc = "spn:100~fmi:4~count:2|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []
        csv_timestamp = '2023-02-10 10:20:34'

        result = ConverterLambda.generate_active_fault_codes(csv_esn, csv_ac_fc, conc_eq_fc_obj, db_esn_ac_fcs,
                                                             csv_timestamp)
        self.assertEqual(result, expected_conv_eq_fc_obj)

    def test_delete_esn_from_dynamodb(self):
        print("<---------- test_delete_esn_from_dynamodb ---------->")

        csv_esn = 123456
        response = ConverterLambda.delete_esn_from_dynamodb(csv_esn)
        self.assertTrue(response)


    @patch.dict("os.environ", {"QueueUrl": "url"})
    @patch("ConverterLambda.boto3.client")
    def test_delete_message_from_sqs_queue_successful(self, mock_boto3_client):
        """
        Test for delete_message_from_sqs_queue() running successfully.
        """
        mock_sqs_client = mock_boto3_client.return_value
        mock_sqs_client.delete_message.return_value = "response"

        response = ConverterLambda.delete_message_from_sqs_queue("receipt-handle")

        mock_sqs_client.delete_message.assert_called_with(QueueUrl="url", ReceiptHandle="receipt-handle")
        self.assertEqual(response, "response")


    def test_process_ss_successful(self):
        """
        Test for process_ss() running successfully.
        """
        ss_dict = {
            "boxId": "boxId",
            "vin": "vin",
            "componentSerialNumber": "componentSerialNumber",
            "dateTimestamp": "dateTimestamp",
            "MessageId": "MessageId"
        }
        ngdi_json_template = {
            "boxId": "placeholder",
            "vin": "placeholder",
            "samples": []
        }
        ss_rows = [{}, ss_dict]
        ss_converted_prot_header = "test~protocol~network-id~address"
        ss_converted_device_parameters = ["MessageId", "boxId", "vin"]

        response = ConverterLambda.process_ss(
            ss_rows,
            ss_dict,
            ngdi_json_template,
            ss_converted_prot_header,
            ss_converted_device_parameters
        )

        expected_response = {
            "boxId": "boxId",
            "vin": "vin",
            "samples": [
                {
                    "dateTimestamp": "dateTimestamp",
                    "convertedDeviceParameters": {"MessageId": "MessageId"},
                    "convertedEquipmentParameters": [
                        {
                            "protocol": "protocol",
                            "networkId": "network-id",
                            "deviceId": "address",
                            "parameters": {
                                "componentSerialNumber": "componentSerialNumber"
                            }
                        }
                    ]
                }
            ]
        }
        self.assertEqual(response, expected_response)


    @patch("ConverterLambda.get_active_fault_codes_from_dynamodb")
    @patch("ConverterLambda.check_active_fault_codes_timestamp")
    @patch("ConverterLambda.generate_active_fault_codes")
    @patch("ConverterLambda.delete_esn_from_dynamodb")
    @patch("ConverterLambda.BDD_ESN", ["componentSerialNumber"])
    def test_process_as_successful(
        self,
        mock_delete_esn_from_dynamodb,
        mock_generate_active_fault_codes,
        mock_check_active_fault_codes_timestamp,
        mock_get_active_fault_codes_from_dynamodb
    ):
        """
        Test for process_as() running successfully.
        """
        as_dict = {
            "boxId": "boxId",
            "vin": "vin",
            "componentSerialNumber": "componentSerialNumber",
            "dateTimestamp": "dateTimestamp",
            "messageId": "messageId",
            "activeFaultCodes": "activeFaultCodes",
            "inactiveFaultCodes": "inactiveFaultCodes",
            "pendingFaultCodes": "pendingFaultCodes",
        }
        ngdi_json_template = {
            "boxId": "boxId",
            "componentSerialNumber": "componentSerialNumber",
            "vin": "vin",
            "samples": []
        }

        values_dict = copy.deepcopy(as_dict)
        values_dict["activeFaultCodes"] = ""
        values_dict["inactiveFaultCodes"] = "00:01"
        values_dict["pendingFaultCodes"] = "00:01"
        as_rows = [values_dict]

        as_converted_prot_header = "test~protocol~network-id~address"
        as_converted_device_parameters = ["messageId", "boxId", "vin"]

        mock_get_active_fault_codes_from_dynamodb.return_value = {
            "Item": "item"
        }
        mock_check_active_fault_codes_timestamp.return_value = True

        response = ConverterLambda.process_as(
            as_rows,
            as_dict,
            ngdi_json_template,
            as_converted_prot_header,
            as_converted_device_parameters
        )

        mock_get_active_fault_codes_from_dynamodb.assert_called_with("componentSerialNumber")
        mock_check_active_fault_codes_timestamp.assert_called_with("item", "dateTimestamp")
        mock_generate_active_fault_codes.assert_not_called()
        mock_delete_esn_from_dynamodb.assert_called_with("componentSerialNumber")

        expected_response = {
            "boxId": "boxId",
            "componentSerialNumber": "componentSerialNumber",
            "vin": "vin",
            "numberOfSamples": 1,
            "samples": [
                {
                    "dateTimestamp": "dateTimestamp",
                    "rawEquipmentParameters": [],
                    "convertedDeviceParameters": {
                        "messageId": "messageId",
                        "boxId": "boxId",
                        "vin": "vin"
                    },
                    "convertedEquipmentParameters": [
                        {
                            "protocol": "protocol",
                            "networkId": "network-id",
                            "deviceId": "address",
                            "parameters": {
                                "componentSerialNumber": "componentSerialNumber"
                            }
                        }
                    ],
                    "convertedEquipmentFaultCodes": [
                        {
                            "protocol": "protocol",
                            "networkId": "network-id",
                            "deviceId": "address",
                            "activeFaultCodes": [],
                            "inactiveFaultCodes": [{"00": "01"}],
                            "pendingFaultCodes": [{"00": "01"}]
                        }
                    ]
                }
            ]
        }

        self.assertEqual(response, expected_response)


    def test_get_device_id_successful(self):
        """
        Test for get_device_id() running successfully.
        """
        response = ConverterLambda.get_device_id({"telematicsDeviceId": "test"})
        self.assertEqual(response, "test")

        response = ConverterLambda.get_device_id({})
        self.assertEqual(response, False)


    @patch("ConverterLambda.EDGE_DB_CLIENT")
    def test_get_tsp_and_cust_ref_successful(self, mock_edge_db_client):
        """
        Test for get_tsp_and_cust_ref() running successfully.
        """
        mock_edge_db_client.execute.return_value = {
            "statusCode": 200,
            "body": json.dumps([{"cust_ref": "Cummins", "device_owner": "Cummins"}])
        }

        response = ConverterLambda.get_tsp_and_cust_ref("device-id")

        self.assertEqual(response, {"cust_ref": "Cummins", "device_owner": "Cummins"})

    
    def test_get_cspec_req_id_successful(self):
        """
        Test for get_cspec_req_id() running successfully.
        """
        response = ConverterLambda.get_cspec_req_id("REQ00-REQ 01")
        self.assertEqual(response, ("REQ00", "REQ 01"))

        response = ConverterLambda.get_cspec_req_id("REQ000")
        self.assertEqual(response, ("REQ000", None))


    @patch.dict("os.environ", {
        "metaWriteQueueUrl": "url",
        "NGDIBody": json.dumps({"componentSerialNumber": "placeholder"})
    })
    @patch("ConverterLambda.s3")
    @patch("ConverterLambda.get_cspec_req_id")
    @patch("ConverterLambda.sqs_send_message")
    @patch("ConverterLambda.csv")
    @patch("ConverterLambda.util")
    @patch("ConverterLambda.process_ss")
    @patch("ConverterLambda.process_as")
    @patch("ConverterLambda.get_device_id")
    @patch("ConverterLambda.get_tsp_and_cust_ref")
    @patch("ConverterLambda.datetime", wraps=datetime)
    @patch("ConverterLambda.s3_client")
    @patch("ConverterLambda.delete_message_from_sqs_queue")
    def test_retrieve_and_process_file(
        self,
        mock_delete_message_from_sqs_queue,
        mock_s3_client,
        mock_datetime_now,
        mock_get_tsp_and_cust_ref,
        mock_get_device_id,
        mock_process_as,
        mock_process_ss,
        mock_util,
        mock_csv,
        mock_sqs_send_message,
        mock_get_cspec_req_id,
        mock_s3
    ):
        """
        Test for retrieve_and_process_file() running successfully.
        """
        datetime_now = datetime.datetime.now()
        datetime_str = datetime.datetime.strftime(datetime_now, "%Y%m%d%H%M%S")
        file_key = f"FILENAME/0_device-id_esn_{datetime_str}.csv"
        last_modified_date_str = "1981-08-03T01:17:04.000Z"

        uploaded_file_object = {
            "source_bucket_name": "source-bucket-name",
            "file_key": file_key,
            "file_size": "file-size",
            "sqs_receipt_handle": "receipt-handle"
        }

        mock_s3.get_object.return_value = {
            "LastModified": last_modified_date_str,
            "Metadata": {"uuid": "uuid"},
            "Body": io.BytesIO("csv-content".encode("utf-8"))
        }
        mock_get_cspec_req_id.return_value = ("config-spec-name", "req-id")

        mock_csv.reader.return_value = [
            ["messageFormatVersion", "message-format-version"],
            ["dataEncryptionSchemeId", "data-encryption-scheme-id"],
            ["telematicsBoxId", "telematics-box-id"],
            ["componentSerialNumber", "component-serial-number"],
            ["dataSamplingConfigId", "data-sampling-config-id"],
            ["ssDateTimestamp", "DEVICE_CONVERTED", "J1939~Raw", "j1939_converted", "dateTimestamp"],
            ["ssDateTimestamp2", "ss-date-timestamp2"],
            ["asRow1", "DEVICE_CONVERTED", "J1939~Raw", "j1939_converted", "dateTimestamp"],
            ["asRow2", "as-row2"]
        ]

        pre_converted_ngdi = {
            "componentSerialNumber": "component-serial-number",
            "messageFormatVersion": "message-format-version",
            "dataEncryptionSchemeId": "data-encryption-scheme-id",
            "telematicsDeviceId": "telematics-box-id",
            "dataSamplingConfigId": "data-sampling-config-id"
        }
        converted_ngdi = copy.deepcopy(pre_converted_ngdi)
        converted_ngdi["telematicsPartnerName"] = "Cummins"
        converted_ngdi["customerReference"] = "Cummins"

        mock_process_ss.return_value = converted_ngdi
        mock_process_as.return_value = converted_ngdi
        mock_get_device_id.return_value = "device-id"
        mock_get_tsp_and_cust_ref.return_value = {"cust_ref": "Cummins", "device_owner": "Cummins"}

        mock_s3_client.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

        ConverterLambda.retrieve_and_process_file(uploaded_file_object)

        mock_sqs_send_message.assert_called_with(
            "url",
            f"uuid,device-id,0_device-id_esn_{datetime_str}.csv,file-size,{last_modified_date_str[:19]},J1939_FC,CSV_JSON_CONVERTED,esn,config-spec-name,req-id,None, , ",
            "jdbc:postgresql://<database-endpoint-url>:<port>/<database>"
        )

        mock_process_ss.assert_called_with(
            [
                ["ssDateTimestamp", "DEVICE_CONVERTED", "J1939~Raw", "j1939_converted", "dateTimestamp"],
                ["ssDateTimestamp2", "ss-date-timestamp2"]
            ],
            {"dateTimeStamp": 4, "DEVICE_CONVERTED": 1, "j1939_converted": 3},
            pre_converted_ngdi,
            "j1939_converted",
            ["DEVICE_CONVERTED"]
        )

        mock_process_as.assert_called_with(
            [["asRow2", "as-row2"]],
            {"asRow1": 0, "dateTimeStamp": 4, "DEVICE_CONVERTED": 1, "j1939_converted": 3},
            converted_ngdi,
            "j1939_converted",
            ["DEVICE_CONVERTED"]
        )

        mock_s3_client.put_object.assert_called_with(
            Bucket="CP_file_DUMP",
            Key=f"ConvertedFiles/component-serial-number/telematics-box-id/{('%02d' % datetime_now.year)}/{('%02d' % datetime_now.month)}/{('%02d' % datetime_now.day)}/FILENAME/0_device-id_esn_{datetime_str}.json",
            Body=json.dumps(converted_ngdi).encode(),
            Metadata={"j1939type": "FC", "uuid": "uuid"}
        )

        mock_util.write_to_audit_table.assert_not_called()
        mock_datetime_now.assert_not_called()
        mock_delete_message_from_sqs_queue.assert_called_with("receipt-handle")


    @patch("ConverterLambda.Process")
    def test_lambda_handler(self, mock_process):
        """
        Test for lambda_handler() running successfully.
        """
        lambda_invoke_event = {
            "Records": [
                {
                    "body": json.dumps({
                        "Records": [
                            {
                                "s3": {
                                    "object": {"key": "file-key", "size": 100},
                                    "bucket": {"name": "bucket"}
                                }
                            }
                        ]
                    }),
                    "receiptHandle": "receipt-handle"
                }
            ]
        }

        ConverterLambda.lambda_handler(lambda_invoke_event, None)

        mock_process.assert_called_with(
            target=ConverterLambda.retrieve_and_process_file,
            args=(
                {
                    "source_bucket_name": "bucket",
                    "file_key": "file-key",
                    "file_size": 100,
                    "sqs_receipt_handle": "receipt-handle"
                },
            )
        )
        mock_process.return_value.start.assert_called()
