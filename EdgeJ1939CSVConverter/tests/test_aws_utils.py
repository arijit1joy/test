# import json
# import sys
# import unittest

# from resources.cda_module_mocking_context import CDAModuleMockingContext
# from unittest.mock import ANY, MagicMock, patch

# with CDAModuleMockingContext(sys) as cda_module_mock_context:
#     cda_module_mock_context.mock_module("boto3")

#     import aws_utils


# class TestAwsUtils(unittest.TestCase):
#     """
#     Test module for aws_utils.py
#     """

#     @patch("aws_utils._fetch_bdd_esn.boto3.client")
#     def test_fetch_bdd_esn_successful(self, mock_boto3):
#         """
#         Test for _fetch_bdd_esn() running successfully.
#         """
#         mock_ssm = mock_boto3.return_value
#         mock_ssm.get_parameter.return_value = {
#             "Parameter": {"Value": json.dumps({"esn": "esn"})}
#         }

#         response = aws_utils._fetch_bdd_esn()

#         mock_ssm.get_parameter.assert_called_with(Name="da-edge-j1939-bdd-esn-list", WithDecryption=False)
#         self.assertEqual(response, "esn")
