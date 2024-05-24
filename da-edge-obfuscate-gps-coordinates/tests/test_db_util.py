from unittest.mock import patch
import unittest
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
    cda_module_mock_context.mock_module("edge_db_utility_layer.obfuscate_gps_utility")
    from db_util import insert_to_metadata_table_query, insert_into_metadata_Table, get_certification_family_query, get_certification_family


class TestEbUtil(unittest.TestCase):
    def test_insert_to_metadata_table_query(self):
        device_id = '357649072115903'
        esn = '64505184'
        config_id = 'SC9004'
        message_id = 'ebe2dcce-9566-4d43-9efd-4e947b24f34d'
        query = insert_to_metadata_table_query(device_id, message_id, esn, config_id)
        self.assertEqual("INSERT INTO da_edge_olympus.da_edge_metadata (device_id,uuid,data_protocol,data_pipeline_stage,esn,config_spec_name,created_datetime) VALUES ('357649072115903','ebe2dcce-9566-4d43-9efd-4e947b24f34d','J1939_Emissions','FILE_RECEIVED','64505184','SC9110','2024-05-22 04:53:56')", query)

    @patch('edge.api_request')
    def test_insert_to_metadata_table(self, mock_api_request):
        device_id = '357649072115903'
        esn = '64505184'
        config_id = 'SC9004'
        message_id = 'ebe2dcce-9566-4d43-9efd-4e947b24f34d'
        mock_api_request.return_value = '{}'
        insert_to_metadata_table(device_id, message_id, esn, config_id)
        mock_api_request.assert_called()


    def test_get_certification_family_query(self):
        device_id = '357649072115903'
        esn = '64505184'
        query = get_certification_family_query(device_id, esn)
        self.assertEqual("SELECT certification_family FROM da_edge_olympus.device_information WHERE engine_serial_number='64505184' AND device_id='357649072115903'", query)


    @patch('edge.api_request')
    def test_get_certification_family(self, mock_api_request):
        device_id = '357649072115903'
        esn = '64505184'
        mock_api_request.return_value = "['Cert']"
        cert = get_certification_family(device_id, esn)
        self.assertEqual("Cert", cert)
        mock_api_request.assert_called()
