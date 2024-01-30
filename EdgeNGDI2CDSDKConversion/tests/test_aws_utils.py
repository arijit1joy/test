# import io
# import json
# import sys
# import unittest

# from tests.cda_module_mock_context import CDAModuleMockingContext
# from unittest.mock import patch

# with CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict(
#     "os.environ",
#     {"spn_parameter_json_object": "obj", "spn_parameter_json_object_key": "key"}
# ):
#     cda_module_mock_context.mock_module("boto3")

#     import aws_utils


# class TestAwsUtils(unittest.TestCase):
#     """
#     Test module for aws_utils.py
#     """

#     @patch("aws_utils._fetch_spn_file.boto3.client")
#     def test_fetch_spn_file_successful(self, mock_boto3):
#         """
#         Test for _fetch_bdd_esn() running successfully.
#         """
#         mock_s3 = mock_boto3.return_value
#         mock_s3.get_object.return_value = {"Body": io.BytesIO(json.dumps({"k": "v"}).encode("utf-8"))}

#         response = aws_utils._fetch_spn_file()

#         mock_s3.get_object.assert_called_with(Body="obj", Key="key")
#         self.assertEqual(response, {"k": "v"})
