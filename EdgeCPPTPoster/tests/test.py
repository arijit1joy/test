import unittest
from unittest import mock
from unittest.mock import patch
import update_scheduler



def test_update_scheduler_function():
    query = "UPDATE da_edge_olympus.scheduler SET status='Data Rx in progress' WHERE request_id='REQ1233'"
    req_id = 'REQ1233'
    expected_query = update_scheduler.get_update_scheduler_query(req_id)
    print('expected_query',expected_query)
    assert query == expected_query


@patch.dict('os.environ', {'EdgeCommonDBAPI': 'https://api.edge-dev.aws.cummins.com/v3/EdgeDBLambda'})
@patch('update_scheduler.edge.api_request')
@patch('update_scheduler.get_update_scheduler_query')
def test_update_scheduler_function_return_success(mock_get_update_scheduler_query, mock_api_request):
    mock_get_update_scheduler_query.return_value = "update da_edge_olympus.scheduler set status = 'Data Rx in progress' where request_id = 'REQ1233'"
    mock_api_request.return_value = "Successfully performed operation"
    result = update_scheduler.update_scheduler_table('REQ1233')
    mock_get_update_scheduler_query.assert_called()
    mock_api_request.assert_called()


@patch.dict('os.environ', {'EdgeCommonDBAPI': 'https://api.edge-dev.aws.cummins.com/v3/EdgeDBLambda'})
@patch('update_scheduler.edge.server_error')
@patch('update_scheduler.get_update_scheduler_query')
def test_update_scheduler_function_return_exception(mock_get_update_scheduler_query, mock_api_request):
    mock_get_update_scheduler_query.return_value = Exception
    mock_api_request.side_effect = "Successfully performed operation"
    result = update_scheduler.update_scheduler_table('REQ1233')
    mock_get_update_scheduler_query.assert_called()
    mock_api_request.assert_called()