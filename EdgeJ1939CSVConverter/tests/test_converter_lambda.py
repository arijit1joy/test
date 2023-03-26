import sys
import unittest
from unittest.mock import MagicMock, patch, call
from moto import mock_dynamodb
import boto3

from resources.cda_module_mocking_context import CDAModuleMockingContext

with CDAModuleMockingContext(sys) as cda_module_mocking_context, patch.dict("os.environ", {
    "PoolSize": "0",
    "CPPostBucket": "CP_file_DUMP",
    "edgeCommonAPIURL": "jdbc:postgresql://<database-endpoint-url>:<port>/<database>",
    "NGDIBody": "{\"Table\": \"CP_FILE_DUMP\", \"ROWS\": 30, \"COLUMNS\": 30}",
    "mapTspFromOwner": "EDGE1939",
    "MaxAttempts": "5",
    "APPLICATION_ENVIRONMENT": "TEST",
    "J1939ActiveFaultCodeTable": "CSVCONVERTER"

}):
    cda_module_mocking_context.mock_module("edge_sqs_utility_layer.sqs_utility")
    cda_module_mocking_context.mock_module("edge_db_lambda_client")
    cda_module_mocking_context.mock_module("utility")
    cda_module_mocking_context.mock_module("boto3")
    cda_module_mocking_context.mock_module("botocore.exceptions")
    import ConverterLambda


@mock_dynamodb
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
        #mock_db_reader.execute.return_value = None
        actual_ac_fc = "spn:1001~fmi:4~count:1"
        conv_eq_fc_obj = {"activeFaultCodes": []}
        result = ConverterLambda.generate_spn_fmi_fc_obj(actual_ac_fc, conv_eq_fc_obj)
        self.assertEqual(result, None)

    def test_get_active_fault_codes_from_dynamodb(self):
        csv_esn = 123456
        result = ConverterLambda.get_active_fault_codes_from_dynamodb(csv_esn)
        self.assertTrue(result)


    def test_check_active_fault_codes_timestamp_none(self) :
        csv_timestamp = '2023-02-10 10:20:34'
        db_esn_ac_fcs = None
        result = ConverterLambda.check_active_fault_codes_timestamp(db_esn_ac_fcs,csv_timestamp)
        self.assertTrue(result)

    def test_check_active_fault_codes_timestamp_true(self):
        csv_timestamp = '2023-02-10 10:20:34'
        db_esn_ac_fcs={}
        db_esn_ac_fcs['timestamp']='2023-01-10 10:20:34'
        result = ConverterLambda.check_active_fault_codes_timestamp(db_esn_ac_fcs,csv_timestamp)
        self.assertTrue(result)

    def test_check_active_fault_codes_timestamp_false(self):
        csv_timestamp = '2023-02-10 10:20:34'
        db_esn_ac_fcs={}
        db_esn_ac_fcs['timestamp'] = '2023-03-10 10:20:34'
        result = ConverterLambda.check_active_fault_codes_timestamp(db_esn_ac_fcs, csv_timestamp)
        self.assertFalse(result)

    def test_put_active_fault_codes(self):
        ac_fc = {
            "spn:100~fmi:4": 1,
            "spn:2623~fmi:3": 1,
            "spn:101~fmi:5": 1,
            "spn:100~fmi:4": 8
        }
        result = ConverterLambda.put_active_fault_codes(123456, '2023-01-01 10:10:29', ac_fc)
        self.assertTrue(result)

    def test_generate_active_fault_codes(self):
        from ConverterLambda import put_active_fault_codes
        from ConverterLambda import get_active_fault_codes_from_dynamodb

        expected_result={'activeFaultCodes': [{'spn': '1001', 'fmi': '4', 'count': '1'}, {'spn': '2456', 'fmi': '3', 'count': '5'}, {'spn': '2623', 'fmi': '3', 'count': '2'}]}

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

        #GET FAULT FORM DATABSE FOR ESN
        csv_esn = 123456
        #get_result_db = get_active_fault_codes_from_dynamodb(csv_esn)
        get_result_db = {'Item': {'esn': 123456, 'timestamp': '2023-01-01 10:10:29', 'fcs': {'spn:100~fmi:4': 3}},
                         'ResponseMetadata': {'RequestId': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah',
                                              'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'amazon.com',
                                                                                     'x-amzn-requestid': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah',
                                                                                     'x-amz-crc32': '2810715295'},
                                              'RetryAttempts': 0}}

        db_esn_ac_fcs=None
        if 'Item' in get_result_db:
            db_esn_ac_fcs=get_result_db['Item']

        csv_ac_fc = "spn:1001~fmi:4~count:1|spn:2623~fmi:3~count:2|spn:2456~fmi:3~count:5|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []
        csv_timestamp='2023-02-10 10:20:34'
        result = ConverterLambda.generate_active_fault_codes(csv_esn, csv_ac_fc, conc_eq_fc_obj, db_esn_ac_fcs, csv_timestamp)
        self.assertEqual(result, expected_result)

    def test_generate_active_fault_codes_empty_ac_fc(self):
        conv_eq_fc_obj = {"activeFaultCodes": []}
        esn = 123456
        ac_fc = ""
        db_esn_ac_fcs=None
        timestamp = '2023-02-10 10:20:34'
        expected_conv_eq_fc_obj={"activeFaultCodes": []}
        result = ConverterLambda.generate_active_fault_codes(esn, ac_fc, conv_eq_fc_obj, db_esn_ac_fcs, timestamp)
        self.assertEqual(result, expected_conv_eq_fc_obj)

    def test_generate_active_fault_codes_new_esn(self):
        conv_eq_fc_obj = {"activeFaultCodes": []}
        esn = 123456
        ac_fc = "spn:1001~fmi:4~count:1|"
        #db_esn_ac_fcs=None
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
        expected_conv_eq_fc_obj={'activeFaultCodes': [{'spn': '1001', 'fmi': '4', 'count': '1'}]}
        result = ConverterLambda.generate_active_fault_codes(esn, ac_fc, conv_eq_fc_obj, db_esn_ac_fcs, timestamp)
        self.assertEqual(result, expected_conv_eq_fc_obj)

    def test_generate_active_fault_codes_duplicate_fc_for_esn(self):
        expected_conv_eq_fc_obj = {'activeFaultCodes': []}
        csv_esn = 123456
        csv_ac_fc = "spn:100~fmi:4~count:3|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []
        csv_timestamp = '2023-02-10 10:20:34'
        # GET FAULT FORM DATABSE FOR ESN
        get_result_db = {'Item': {'esn': 123456, 'timestamp': '2023-01-01 10:10:29', 'fcs': {'spn:100~fmi:4':3}}, 'ResponseMetadata': {'RequestId': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah', 'HTTPStatusCode': 200, 'HTTPHeaders': {'server': 'amazon.com', 'x-amzn-requestid': 'hulL0SeuT55JJtu1ClvOn3IW1B2OPxGhGLOx1LCrFgWme2GCL0Ah', 'x-amz-crc32': '2810715295'}, 'RetryAttempts': 0}}
        db_esn_ac_fcs = None
        if 'Item' in get_result_db:
            db_esn_ac_fcs = get_result_db['Item']

        result=ConverterLambda.generate_active_fault_codes(csv_esn, csv_ac_fc, conc_eq_fc_obj, db_esn_ac_fcs, csv_timestamp)
        self.assertEqual(result, expected_conv_eq_fc_obj)


    def test_generate_active_fault_codes_one_new_duplicate_fc_esn(self):
        expected_conv_eq_fc_obj={'activeFaultCodes': [{'spn': '1001', 'fmi': '4', 'count': '1'}]}
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
        csv_ac_fc="spn:100~fmi:4~count:3|spn:1001~fmi:4~count:1|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []
        csv_timestamp='2023-02-10 10:20:34'
        result = ConverterLambda.generate_active_fault_codes(csv_esn, csv_ac_fc, conc_eq_fc_obj, db_esn_ac_fcs,csv_timestamp)
        self.assertEqual(result, expected_conv_eq_fc_obj)

    def test_generate_active_fault_codes_fc_oocu_esn(self):
        expected_conv_eq_fc_obj = {'activeFaultCodes': [{'spn': '100', 'fmi': '4', 'count': '2'}]}
        csv_esn = 123456
        db_esn_ac_fcs={'esn': 123456, 'timestamp': '2023-01-01 10:10:29', 'fcs': {'spn:100~fmi:4': 1}}
        csv_ac_fc="spn:100~fmi:4~count:2|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []
        csv_timestamp='2023-02-10 10:20:34'

        result = ConverterLambda.generate_active_fault_codes(csv_esn, csv_ac_fc, conc_eq_fc_obj, db_esn_ac_fcs, csv_timestamp)
        self.assertEqual(result, expected_conv_eq_fc_obj)

    def test_delete_esn_from_dynamodb(self):
        csv_esn = 123456
        response = ConverterLambda.delete_esn_from_dynamodb(csv_esn)
        self.assertTrue(response)

