import unittest
from unittest.mock import patch, MagicMock
import sys

sys.path.append('../')
sys.modules['edge_logger'] = MagicMock()
sys.modules['obfuscate_gps_utility'] = MagicMock()
from lambda_function import lambda_handler
del sys.modules['edge_logger']
del sys.modules['obfuscate_gps_utility']


class TestLambdaFunction(unittest.TestCase):
    @patch('lambda_function.obfuscate_gps')
    def test_lambdaHandler_givenValidEvent_thenCalledObfuscateGPS(self, mock_obfuscate_gps):
        print('<-----test_lambdaHandler_givenValidEvent_thenCalledObfuscateGPS----->')
        event = {"telematicsDeviceId": "1234567890"}
        result = lambda_handler(event, None)
        print("Result: ", result)
        mock_obfuscate_gps.assert_called()

    @patch('lambda_function.obfuscate_gps')
    def test_lambdaHandler_givenValidEvent_whenExceptionOccurred_thenLogException(self, mock_obfuscate_gps):
        print('<-----test_lambdaHandler_givenValidEvent_whenExceptionOccurred_thenLogException----->')
        event = {"telematicsDeviceId": "1234567890"}
        mock_obfuscate_gps.side_effect = Exception

        result = lambda_handler(event, None)
        print("Result: ", result)

        self.assertRaises(Exception)
        mock_obfuscate_gps.assert_called()
