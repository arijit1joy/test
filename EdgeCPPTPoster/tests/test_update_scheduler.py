# import sys
# import unittest
# from unittest.mock import patch
#
# from tests.cda_module_mock_context import CDAModuleMockingContext
#
# with CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict('os.environ', {
#     'edgeCommonAPIURL': 'https://test.edgedb.url',
# }):
#     cda_module_mock_context.mock_module("utility")
#     cda_module_mock_context.mock_module("pypika")
#     cda_module_mock_context.mock_module("edge_core")
#     cda_module_mock_context.mock_module("scheduler_query")
#     cda_module_mock_context.mock_module("scheduler_query")
#
#     import update_scheduler
#
#
# class TestEdgeCPPTPoster(unittest.TestCase):
#
#     def test_getRequestIDFromConsumptionView_given(self):
#         query = "UPDATE da_edge_olympus.scheduler SET status='Data Rx in progress' " \
#                 "WHERE request_id='REQ1233' AND device_id='102900000000003' AND status='Config Accepted'"
#         req_id = 'REQ1233'
#         device_id = '102900000000003'
#         expected_query = update_scheduler.get_update_scheduler_query(req_id, device_id)
#         print('query', query)
#         print('expected_query', expected_query)
#         assert query == expected_query
#     #
#     # @patch('update_scheduler.edge.api_request')
#     # @patch('update_scheduler.get_update_scheduler_query')
#     # def test_update_scheduler_function_return_success(self, mock_get_update_scheduler_query, mock_api_request):
#     #     mock_get_update_scheduler_query.return_value = \
#     #         "update da_edge_olympus.scheduler set status = 'Data Rx in Progress' " \
#     #         "where request_id = 'REQ1233' AND status='Config Accepted'"
#     #     mock_api_request.return_value = "Successfully performed operation"
#     #     device_id = '102900000000003'
#     #     result = update_scheduler.update_scheduler_table('REQ1233', device_id)
#     #     mock_get_update_scheduler_query.assert_called()
#     #     mock_api_request.assert_called()
#     #
#     # @patch('update_scheduler.edge.server_error')
#     # @patch('update_scheduler.get_update_scheduler_query')
#     # def test_update_scheduler_function_return_exception(self, mock_get_update_scheduler_query, mock_api_request):
#     #     mock_get_update_scheduler_query.return_value = Exception
#     #     mock_api_request.side_effect = "Successfully performed operation"
#     #     device_id = '102900000000003'
#     #     result = update_scheduler.update_scheduler_table('REQ1233', device_id)
#     #     mock_get_update_scheduler_query.assert_called()
#     #     mock_api_request.assert_called()
#     #
#     # def test_get_request_id_from_consumption_view_query(self):
#     #     query = "SELECT request_id FROM da_edge_olympus.edge_data_consumption_vw " \
#     #             "WHERE data_type='J1939_HB' AND data_config_filename='EDGE_357649070803120_64100016_SC5079' " \
#     #             "AND config_status='Config Accepted'"
#     #     data_protocol = 'J1939_HB'
#     #     data_config_filename = 'EDGE_357649070803120_64100016_SC5079'
#     #     expected_query = update_scheduler._get_request_id_from_consumption_view_query(data_protocol,
#     #                                                                                   data_config_filename)
#     #     print('query', query)
#     #     print('expected_query', expected_query)
#     #     assert query == expected_query
