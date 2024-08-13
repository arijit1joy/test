from unittest import TestCase
from unittest.mock import patch
from requests.models import Response

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
    cda_module_mock_context.mock_module("edge_simple_logging_layer")
    cda_module_mock_context.mock_module("requests")
    cda_module_mock_context.mock_module("boto3")
    from tsb_util import push_to_tsb


class TestTsbUtil(TestCase):
    def mocked_response_200(*args, **kwargs):
        response = Response()
        response.status_code = 200
        response._content = str.encode("message posted to TSB successfully")
        return response

    @patch('tsb_util.get_secret')
    @patch('tsb_util.formatter')
    @patch('tsb_util.post', side_effect=mocked_response_200)
    def test_push_to_tsb(self, mock_post, mock_formatter, mock_get_secret):
        message = {'telematicsDeviceId': '357649072115903', 'componentSerialNumber': '64505184', 'dataSamplingConfigId': 'SC9110'}
        mock_get_secret.retrun_value = {'SecretString': 'cbhdbsbfsuvkbduhgz'}
        mock_formatter.return_value = [357649072115903]
        push_to_tsb(message)
        mock_get_secret.assert_called()
        mock_formatter.assert_called()
        mock_post.assert_called()

    def mocked_response_500(*args, **kwargs):
        response = Response()
        response.status_code = 500
        response._content = str.encode("message posted to TSB successfully")
        return response

    @patch('tsb_util.get_secret')
    @patch('tsb_util.formatter')
    @patch('tsb_util.post', side_effect=mocked_response_500)
    def test_push_to_tsb2(self, mock_post, mock_formatter, mock_get_secret):
        with self.assertRaises(Exception):
            message = {'telematicsDeviceId': '357649072115903', 'componentSerialNumber': '64505184', 'dataSamplingConfigId': 'SC9110'}
            mock_get_secret.retrun_value = {'SecretString': 'cbhdbsbfsuvkbduhgz'}
            mock_formatter.return_value = [357649072115903]
            push_to_tsb(message)
            mock_get_secret.assert_called()
            mock_formatter.assert_called()
            mock_post.assert_called()