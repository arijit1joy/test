from unittest import TestCase
from unittest.mock import patch

import boto3
from botocore.stub import Stubber

with patch.dict('os.environ',
                {'LoggingLevel': 'info',
                 'EdgeRDSSecretName': 'rdssecret',
                 'j1939_emission_end_bucket': 'emission_bucket'
                 }):
    from lambda_function import lambda_handler


class TestLambdaFunction(TestCase):

    @patch('lambda_function.update_metadata_Table')
    def test_lambda_handler(self, mock_update_metadata_table):
        event = {"Records": [{"telematicsDeviceId": "1234567890"}]}
        mock_update_metadata_table.return_value = None
        client = boto3.client('s3')
        stub = Stubber(client)
        stub.add_response('get_object', {"Body": bytearray("{'Content': 'content'}", 'utf-8')})
        stub.activate()
        result = lambda_handler(event, None)

