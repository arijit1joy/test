from unittest.mock import patch
from unittest import TestCase, mock
import requests
import sys

from cda_module_mock_context import CDAModuleMockingContext

with CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict("os.environ", {
    "LoggingLevel": "debug",
    'region': 'rdssecret',
    'TimeFormat': '%Y-%m-%d %H:%M:%S',
    'edgeCommonAPIURL': 'https://api.edge-dev.aws.cummins.com/v3/EdgeDBLambda'
}):
    cda_module_mock_context.mock_module("edge_core_layer.edge_logger")
    cda_module_mock_context.mock_module("edge_core_layer.edge_core")
    from db_util import update_metadata_Table, update_metadata_table_query


class TestDbUtil(TestCase):

    maxDiff = None

    def test_update_metadata_table_query(self):
        device_id = '357649072115903'
        esn = '64505184'
        config_id = 'SC9004'
        query = update_metadata_table_query(device_id, esn, config_id)
        self.assertEqual("UPDATE da_edge_olympus.da_edge_metadata SET data_pipeline_stage='FILE_SENT' WHERE device_id='357649072115903' AND esn='64505184' AND config_spec_name='SC9004'", query)

    @patch('db_util.api_request')
    def test_update_metadata_table(self, mock_api_request):
        device_id = '357649072115903'
        esn = '64505184'
        config_id = 'SC9004'
        mock_api_request.return_value = '{}'
        update_metadata_Table(device_id, esn, config_id)
        mock_api_request.assert_called()

    @patch('db_util.api_request')
    @patch('db_util.server_error')
    def test_update_metadata_table02(self, mock_server_error, mock_api_request):
        device_id = '357649072115903'
        esn = '64505184'
        config_id = 'SC9004'
        mock_api_request.side_effect = requests.exceptions.ConnectionError()
        mock_server_error.return_value = '{}'
        update_metadata_Table(device_id, esn, config_id)
        mock_api_request.assert_called()
        mock_server_error.assert_called()
