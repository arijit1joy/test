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
    @patch('lambda_function.get_request_id_from_consumption_view')
    @patch('lambda_function.get_cspec_req_id')
    @patch('lambda_function.update_scheduler_table')
    def test_lambda_handler(self, mock_update_scheduler_table, mock_get_cspec_req_id,
                            mock_get_request_id_from_consumption_view, mock_insert_into_metadata_Table,
                            mock_push_to_tsb, mock_get_content):
        event = {"Records": [{"body": str("{\"Records\": [{\"s3\": {\"object\": {\"key\": \"/fileKey\"}}}]}")}]}
        mock_get_content.return_value = "{\"telematicsDeviceId\": \"357649072115903\", \"componentSerialNumber\": \"64505184\", \"dataSamplingConfigId\": \"SC9110\" }", "1137167d-e18a-4830-bdc6-a5a15d14d61b "
        mock_push_to_tsb.return_value = None
        mock_insert_into_metadata_Table.return_value = None
        mock_get_request_id_from_consumption_view.return_value = None, None
        mock_get_cspec_req_id.return_value = "test", 123456
        mock_update_scheduler_table.return_value = None
        lambda_handler(event, None)
        mock_get_content.assert_called()
        mock_get_request_id_from_consumption_view.assert_called()
        mock_update_scheduler_table.asset_called()
        mock_push_to_tsb.assert_called()
        mock_insert_into_metadata_Table.assert_called()

    @patch('lambda_function.get_content')
    @patch('lambda_function.get_request_id_from_consumption_view')
    def test_lambda_handler02(self,mock_get_request_id_from_consumption_view, mock_get_content):
        event = {"Records": [{"body": str("{\"Records\": [{\"s3\": {\"object\": {\"key\": \"/fileKey\"}}}]}")}]}
        mock_get_content.return_value = "{\"componentSerialNumber\": \"64505184\", \"dataSamplingConfigId\": \"SC9110\" }", "1137167d-e18a-4830-bdc6-a5a15d14d61b"
        mock_get_request_id_from_consumption_view.return_value = None, None
        lambda_handler(event, None)
        mock_get_content.assert_called()
        mock_get_request_id_from_consumption_view.assert_not_called()