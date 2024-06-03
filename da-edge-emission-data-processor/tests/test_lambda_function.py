from unittest import TestCase
from unittest.mock import patch

import sys

from cda_module_mock_context import CDAModuleMockingContext

with CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict('os.environ',
                {'LoggingLevel': 'info',
                 'EdgeRDSSecretName': 'rdssecret',
                 'j1939_emission_end_bucket': 'emission_bucket',
                 'tsb_url': 'https://tsbUrl',
                 'region': 'us-east-1',
                 'secret_name': 'secret-name'
                 }):
    cda_module_mock_context.mock_module("edge_core")
    from lambda_function import lambda_handler


class TestLambdaFunction(TestCase):

    @patch('lambda_function.get_content')
    @patch('lambda_function.push_to_tsb')
    @patch('lambda_function.insert_into_metadata_Table')
    def test_lambda_handler(self, mock_insert_into_metadata_Table, mock_push_to_tsb, mock_get_content):
        event = {"Records": [{"body": str("{\"Records\": [{\"s3\": {\"object\": {\"key\": \"/fileKey\"}}}]}")}]}
        mock_get_content.return_value = "{\"telematicsDeviceId\": \"357649072115903\", \"componentSerialNumber\": \"64505184\", \"dataSamplingConfigId\": \"SC9110\" }"
        mock_push_to_tsb.return_value = None
        mock_insert_into_metadata_Table.return_value = None
        lambda_handler(event, None)
        mock_get_content.assert_called()
        mock_push_to_tsb.assert_called()
        mock_insert_into_metadata_Table.assert_called()

    @patch('lambda_function.get_content')
    @patch('lambda_function.push_to_tsb')
    @patch('lambda_function.insert_into_metadata_Table')
    def test_lambda_handler02(self, mock_insert_into_metadata_Table, mock_push_to_tsb, mock_get_content):
        event = {"Records": [{"body": str("{\"Records\": [{\"s3\": {\"object\": {\"key\": \"/fileKey\"}}}]}")}]}
        mock_get_content.return_value = "{\"componentSerialNumber\": \"64505184\", \"dataSamplingConfigId\": \"SC9110\" }"
        mock_push_to_tsb.return_value = None
        lambda_handler(event, None)
        mock_get_content.assert_called()
        mock_push_to_tsb.assert_called()
        mock_insert_into_metadata_Table.assert_not_called()