from pprint import pprint
import unittest
import boto3  # AWS SDK for Python
from botocore.exceptions import ClientError
from moto import mock_dynamodb  # since we're going to mock DynamoDB service


@mock_dynamodb
class TestDatabaseFunctions(unittest.TestCase):

    def setUp(self):
        """
        Create database resource and mock table
        """
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

        from DynamodbSample import create_active_fault_codes_table
        self.table = create_active_fault_codes_table(self.dynamodb)
        print('table :', self.table)
        print("Table status:", self.table.table_status)

    def tearDown(self):
        """
        Delete database resource and mock table
        """
        self.table.delete()
        self.dynamodb = None

    def test_table_exists(self):
        """
        Test if our mock table is ready
        """

        def test_table_exists(self):
            self.assertIn('Movies', self.table.name)  # check if the table name is 'Movies'

    '''def test_put_active_fault_codes(self):
        from DynamodbSample import put_active_fault_codes

        ac_fc = {
            "spn:100~fmi:4": 1,
            "spn:2623~fmi:3": 1,
            "spn:101~fmi:5": 1
        }
        timestamp='2023-01-01 10:10:29'
        result = put_active_fault_codes(12345,timestamp ,ac_fc, self.dynamodb)

        print("Put esn succeeded:")
        print(result)
        self.assertEqual(200, result['ResponseMetadata']['HTTPStatusCode'])

    def test_get_active_fault_codes(self):
        from DynamodbSample import put_active_fault_codes
        from DynamodbSample import get_active_fault_codes

        ac_fc = {
            "spn:100~fmi:4": 1,
            "spn:2623~fmi:3": 1,
            "spn:101~fmi:5": 1
        }
        ac_fc1 = {
            "spn:100~fmi:4": 1,
            "spn:2623~fmi:3": 1
        }

        put_active_fault_codes(12345, '2023-01-01 10:10:29',ac_fc, self.dynamodb)
        put_active_fault_codes(1234, '2023-02-01 10:10:29', ac_fc1, self.dynamodb)
        result = get_active_fault_codes(1234, self.dynamodb)
        print('result :', result)
        if result:
            print("Get movie succeeded:")
            print(result)
            if 'Item' in result:
                ac_fc_ssn=result['Item']['fcs']
                print("ac_fc_ssn:",ac_fc_ssn)
                print("ac_fc_ssn:", ac_fc_ssn['spn:100~fmi:4'])
            else:
                print("Item not Found")'''



    def test_generate_active_fault_codes(self):
        from DynamodbSample import check_active_fault_codes_timestamp
        from DynamodbSample import put_active_fault_codes
        from DynamodbSample import get_active_fault_codes_from_dynamodb
        from DynamodbSample import generate_active_fault_codes

        print('test_generate_active_fault_codes')

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

        put_active_fault_codes(123456, '2023-01-01 10:10:29', ac_fc, self.dynamodb)
        put_active_fault_codes(1234, '2023-02-01 10:10:29', ac_fc1, self.dynamodb)

        #GET FAULT FORM DATABSE FOR ESN
        csv_esn = 123456
        get_result_db = get_active_fault_codes_from_dynamodb(csv_esn, self.dynamodb)
        print('get_result_db :', get_result_db)
        db_esn_ac_fcs=None
        if 'Item' in get_result_db:
            print('esn exist in database')
            db_esn_ac_fcs=get_result_db['Item']
            print('db_esn_ac_fcs :', db_esn_ac_fcs)
        else:
            print('esn does not exist in database')

        #csv_ac_fc="spn:1001~fmi:4~count:1|"
        csv_ac_fc = "spn:1001~fmi:4~count:1|spn:2623~fmi:3~count:2|spn:2456~fmi:3~count:5|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []

        csv_timestamp='2023-02-10 10:20:34'

        result=generate_active_fault_codes(csv_esn,csv_ac_fc,conc_eq_fc_obj,db_esn_ac_fcs,csv_timestamp,self.dynamodb)
        print('result:',result)
        #get_result1 = get_active_fault_codes(esn, self.dynamodb)
        #print('get_result1 :', get_result1)

        # self.assertEqual(12345, result['esn'])
        # self.assertEqual("The Big New Movie", result['title'])
        # self.assertEqual("Nothing happens at all.", result['info']['plot'])
        # self.assertEqual(0, result['info']['rating'])

    def test_generate_active_fault_codes_for_new_esn(self):
        from DynamodbSample import get_active_fault_codes_from_dynamodb
        from DynamodbSample import generate_active_fault_codes
        from DynamodbSample import check_active_fault_codes_timestamp

        print('test_generate_active_fault_codes_for_new_esn')

        #GET FAULT FORM DATABSE FOR ESN
        csv_esn = 123456
        get_result_db = get_active_fault_codes_from_dynamodb(csv_esn, self.dynamodb)
        print('get_result_db :', get_result_db)
        db_esn_ac_fcs=None
        if 'Item' in get_result_db:
            db_esn_ac_fcs=get_result_db['Item']


        csv_ac_fc="spn:1001~fmi:4~count:1|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []

        csv_timestamp='2023-02-10 10:20:34'
        db_timestamp_check = check_active_fault_codes_timestamp(db_esn_ac_fcs, csv_timestamp)
        print('db_timestamp_check :', db_timestamp_check)
        if db_timestamp_check:
            result=generate_active_fault_codes(csv_esn,csv_ac_fc,conc_eq_fc_obj,db_esn_ac_fcs,csv_timestamp,self.dynamodb)
            print('result:',result)
        else:
            print("db_timestamp is greather than timestamp ")

    def test_generate_active_fault_codes_duplicate_fc_for_esn(self):
        from DynamodbSample import put_active_fault_codes
        from DynamodbSample import get_active_fault_codes_from_dynamodb
        from DynamodbSample import generate_active_fault_codes
        from DynamodbSample import check_active_fault_codes_timestamp

        print('test_generate_active_fault_codes_duplicate_fc_for_esn')
        csv_esn = 123456
        #csv_ac_fc = "spn:100~fmi:4~count:1|"
        csv_ac_fc = "spn:100~fmi:4~count:3|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []
        csv_timestamp = '2023-02-10 10:20:34'
        #csv_timestamp = '2023-01-10 01:20:34'

        ac_fc = {
            "spn:100~fmi:4": 3

        }

        put_active_fault_codes(csv_esn, '2023-01-01 10:10:29', ac_fc, self.dynamodb)
        #GET FAULT FORM DATABSE FOR ESN
        get_result_db = get_active_fault_codes_from_dynamodb(csv_esn, self.dynamodb)
        print('get_result_db :', get_result_db)
        db_esn_ac_fcs=None
        if 'Item' in get_result_db:
            db_esn_ac_fcs=get_result_db['Item']

        db_timestamp_check = check_active_fault_codes_timestamp(db_esn_ac_fcs, csv_timestamp)
        print('db_timestamp_check :', db_timestamp_check)
        if db_timestamp_check:
            result=generate_active_fault_codes(csv_esn,csv_ac_fc,conc_eq_fc_obj,db_esn_ac_fcs,csv_timestamp,self.dynamodb)
            print('result:',result)
        else:
            print("db_timestamp is greather than timestamp ")

    def test_generate_active_fault_codes_one_new_duplicate_fc_esn(self):
        from DynamodbSample import get_active_fault_codes_from_dynamodb
        from DynamodbSample import generate_active_fault_codes
        from DynamodbSample import put_active_fault_codes
        from DynamodbSample import check_active_fault_codes_timestamp

        print('test_generate_active_fault_codes_one_new_duplicate_fc_esn')
        csv_esn = 123456

        ac_fc = {
            "spn:100~fmi:4": 1
        }

        put_active_fault_codes(csv_esn, '2023-01-01 10:10:29', ac_fc, self.dynamodb)

        #GET FAULT FORM DATABSE FOR ESN
        csv_esn = 123456
        get_result_db = get_active_fault_codes_from_dynamodb(csv_esn, self.dynamodb)
        print('get_result_db :', get_result_db)
        db_esn_ac_fcs=None
        if 'Item' in get_result_db:
            db_esn_ac_fcs=get_result_db['Item']

        csv_ac_fc="spn:100~fmi:4~count:1|spn:1001~fmi:4~count:1|"

        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []

        csv_timestamp='2023-02-10 10:20:34'

        db_timestamp_check = check_active_fault_codes_timestamp(db_esn_ac_fcs, csv_timestamp)
        print('db_timestamp_check status:', db_timestamp_check)
        if db_timestamp_check:
            result=generate_active_fault_codes(csv_esn,csv_ac_fc,conc_eq_fc_obj,db_esn_ac_fcs,csv_timestamp,self.dynamodb)
            print('result:', result)
        else:
            print("db_timestamp is greather than timestamp ")

    def test_generate_active_fault_codes_fc_oocu_esn(self):
        from DynamodbSample import get_active_fault_codes_from_dynamodb
        from DynamodbSample import generate_active_fault_codes
        from DynamodbSample import put_active_fault_codes
        from DynamodbSample import check_active_fault_codes_timestamp

        print('test_generate_active_fault_codes_one_new_duplicate_fc_esn')
        csv_esn = 123456

        ac_fc = {
            "spn:100~fmi:4": 1
        }

        put_active_fault_codes(csv_esn, '2023-01-01 10:10:29', ac_fc, self.dynamodb)

        #GET FAULT FORM DATABSE FOR ESN
        csv_esn = 123456
        get_result_db = get_active_fault_codes_from_dynamodb(csv_esn, self.dynamodb)
        print('get_result_db :', get_result_db)
        db_esn_ac_fcs=None
        if 'Item' in get_result_db:
            db_esn_ac_fcs=get_result_db['Item']

        csv_ac_fc="spn:100~fmi:4~count:2|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []

        csv_timestamp='2023-02-10 10:20:34'

        db_timestamp_check = check_active_fault_codes_timestamp(db_esn_ac_fcs, csv_timestamp)
        print('db_timestamp_check status:', db_timestamp_check)
        if db_timestamp_check:
            result=generate_active_fault_codes(csv_esn,csv_ac_fc,conc_eq_fc_obj,db_esn_ac_fcs,csv_timestamp,self.dynamodb)
            print('result:', result)
        else:
            print("db_timestamp is greather than timestamp ")
    def test_timestamp_check_true(self):
        from DynamodbSample import put_active_fault_codes
        from DynamodbSample import get_active_fault_codes_from_dynamodb
        from DynamodbSample import generate_active_fault_codes
        from DynamodbSample import check_active_fault_codes_timestamp

        print('test_timestamp_check_true')
        csv_esn = 123456
        csv_ac_fc = "spn:100~fmi:4~count:1|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []
        csv_timestamp = '2023-02-10 10:20:34'
        #csv_timestamp = '2023-01-10 01:20:34'

        ac_fc = {
            "spn:100~fmi:4": 1

        }
        put_active_fault_codes(csv_esn, '2023-01-01 10:10:29', ac_fc, self.dynamodb)
        #GET FAULT FORM DATABSE FOR ESN
        get_result_db = get_active_fault_codes_from_dynamodb(csv_esn, self.dynamodb)
        print('get_result_db :', get_result_db)
        db_esn_ac_fcs=None
        if 'Item' in get_result_db:
            db_esn_ac_fcs=get_result_db['Item']

        db_timestamp_check = check_active_fault_codes_timestamp(db_esn_ac_fcs, csv_timestamp)
        print('db_timestamp_check :', db_timestamp_check)

    def test_timestamp_check_false(self):
        from DynamodbSample import put_active_fault_codes
        from DynamodbSample import get_active_fault_codes_from_dynamodb
        from DynamodbSample import generate_active_fault_codes
        from DynamodbSample import check_active_fault_codes_timestamp

        print('test_timestamp_check_false')
        csv_esn = 123456
        csv_ac_fc = "spn:100~fmi:4~count:1|"
        conc_eq_fc_obj = {}
        conc_eq_fc_obj['activeFaultCodes'] = []
        csv_timestamp = '2023-01-10 01:01:34'

        ac_fc = {
            "spn:100~fmi:4": 1

        }
        put_active_fault_codes(csv_esn, '2023-01-01 10:10:29', ac_fc, self.dynamodb)
        #GET FAULT FORM DATABSE FOR ESN
        get_result_db = get_active_fault_codes_from_dynamodb(csv_esn, self.dynamodb)
        print('get_result_db :', get_result_db)
        db_esn_ac_fcs=None
        if 'Item' in get_result_db:
            db_esn_ac_fcs=get_result_db['Item']

        db_timestamp_check = check_active_fault_codes_timestamp(db_esn_ac_fcs, csv_timestamp)
        print('db_timestamp_check :', db_timestamp_check)

if __name__ == '__main__':
    unittest.main()
