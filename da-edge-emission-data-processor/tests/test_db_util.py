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
    from db_util import insert_into_metadata_Table, insert_to_metadata_table_query


class TestDbUtil(TestCase):

    maxDiff = None

    def test_insert_to_metadata_table_query(self):
        device_id = '357649072115903'
        esn = '64505184'
        config_id = 'SC9004'
        message_id = 'ebe2dcce-9566-4d43-9efd-4e947b24f34d'
        file_name = 'filename'
        file_size = 22
        query = insert_to_metadata_table_query(device_id, message_id, esn, config_id, file_name, file_size)
        self.assertTrue(query.startswith("INSERT INTO da_edge_olympus.da_edge_metadata"))

    @patch('db_util.api_request')
    def test_insert_to_metadata_table(self, mock_api_request):
        device_id = '357649072115903'
        esn = '64505184'
        config_id = 'SC9004'
        message_id = 'ebe2dcce-9566-4d43-9efd-4e947b24f34d'
        mock_api_request.return_value = '{}'
        file_name = 'filename'
        file_size = 22
        insert_into_metadata_Table(device_id, message_id, esn, config_id, file_name, file_size)
        mock_api_request.assert_called()