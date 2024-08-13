import json
import sys
import unittest

from tests.cda_module_mock_context import CDAModuleMockingContext
from unittest.mock import ANY, MagicMock, patch

with CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict("os.environ", {
    "CDPTJ1939PostURL": "post-url",
    "CDPTJ1939Header": "header",
    "edgeCommonAPIURL": "api-url"
}):
    cda_module_mock_context.mock_module("utility")
    cda_module_mock_context.mock_module("edge_sqs_utility_layer")
    cda_module_mock_context.mock_module("boto3")
    cda_module_mock_context.mock_module("pt_poster")

    import post


class TestPost(unittest.TestCase):
    """
    Test module for post.py
    """

    def test_check_endpoint_file_exists_successful(self):
        """
        Test for check_endpoint_file_exists() running successfully.
        """
        response = post.check_endpoint_file_exists("", "")
        self.assertEqual(response, False)

    
    def test_get_cspec_req_id_successful(self):
        """
        Test for get_cspec_req_id() running successfully.
        """
        response = post.get_cspec_req_id("REQ00-REQ 01")
        self.assertEqual(response, ("REQ00", "REQ 01"))

        response = post.get_cspec_req_id("REQ000")
        self.assertEqual(response, ("REQ000", None))

    
    @patch.dict("os.environ", {"metaWriteQueueUrl": "queue-url"})
    @patch("post.sqs_send_message")
    @patch("post.write_to_audit_table")
    @patch("post.check_endpoint_file_exists")
    @patch("post.pt_poster")
    def test_send_to_cd_sdk_successful(
        self,
        mock_pt_poster,
        mock_check_endpoint_file_exists,
        mock_write_to_audit_table,
        mock_sqs_send_message
    ):
        """
        Test for send_to_cd() running successfully for SDK format.
        """
        mock_client = MagicMock()

        post.send_to_cd(
            "bucket",
            "test/ConvertedFiles",
            "SDK",
            mock_client,
            "HB",
            "bucket",
            "file",
            "Y",
            {"telematicsDeviceId": "device-id", "componentSerialNumber": "esn"},
            "uuid",
            "CD_PT_POSTED",
            "csv"
        )

        mock_client.put_object.assert_called_with(
            Bucket="bucket",
            Key="test/NGDI",
            Body=json.dumps({"telematicsDeviceId": "device-id", "componentSerialNumber": "esn"}).encode(),
            Metadata={"j1939type": "HB", "uuid": "uuid"}
        )
        mock_sqs_send_message.assert_called_with("queue-url", "CD_PT_POSTED", "api-url")
        mock_write_to_audit_table.assert_not_called()
        mock_check_endpoint_file_exists.assert_not_called()
        mock_pt_poster.send_to_pt.assert_not_called()


    @patch.dict("os.environ", {"metaWriteQueueUrl": "queue-url"})
    @patch("post.sqs_send_message")
    @patch("post.write_to_audit_table")
    @patch("post.check_endpoint_file_exists")
    @patch("post.pt_poster")
    def test_send_to_cd_sdk_on_error(
        self,
        mock_pt_poster,
        mock_check_endpoint_file_exists,
        mock_write_to_audit_table,
        mock_sqs_send_message
    ):
        """
        Test for send_to_cd() when it throws an exception for SDK format.
        """
        mock_client = MagicMock()
        mock_client.put_object.side_effect = Exception

        post.send_to_cd(
            "bucket",
            "test/ConvertedFiles",
            "SDK",
            mock_client,
            "HB",
            "bucket",
            "file",
            "Y",
            {"telematicsDeviceId": "device-id", "componentSerialNumber": "esn"},
            "uuid",
            "CD_PT_POSTED",
            "csv"
        )

        mock_write_to_audit_table.assert_called_with("csv", ANY, "device-id")
        
        mock_sqs_send_message.assert_not_called()
        mock_check_endpoint_file_exists.assert_not_called()
        mock_pt_poster.send_to_pt.assert_not_called()

    
    @patch.dict("os.environ", {"metaWriteQueueUrl": "queue-url"})
    @patch("post.sqs_send_message")
    @patch("post.write_to_audit_table")
    @patch("post.check_endpoint_file_exists")
    @patch("post.pt_poster")
    def test_send_to_cd_ngdi_successful(
        self,
        mock_pt_poster,
        mock_check_endpoint_file_exists,
        mock_write_to_audit_table,
        mock_sqs_send_message
    ):
        """
        Test for send_to_cd() running successfully for NGDI format.
        """
        mock_client = MagicMock()

        post.send_to_cd(
            "bucket",
            "test/ConvertedFiles",
            "NGDI",
            mock_client,
            "HB",
            "bucket",
            "file",
            "Y",
            {"telematicsDeviceId": "device-id", "componentSerialNumber": "esn"},
            "uuid",
            "CD_PT_POSTED",
            "csv"
        )

        mock_check_endpoint_file_exists.assert_called_with("bucket", "file")

        mock_client.put_object.assert_not_called()
        mock_sqs_send_message.assert_not_called()
        mock_write_to_audit_table.assert_not_called()
        mock_pt_poster.send_to_pt.assert_not_called()


    @patch.dict("os.environ", {"metaWriteQueueUrl": "queue-url"})
    @patch("post.sqs_send_message")
    @patch("post.write_to_audit_table")
    @patch("post.check_endpoint_file_exists")
    @patch("post.pt_poster")
    def test_send_to_cd_ngdi_no_bucket_successful(
        self,
        mock_pt_poster,
        mock_check_endpoint_file_exists,
        mock_write_to_audit_table,
        mock_sqs_send_message
    ):
        """
        Test for send_to_cd() NOT using endpoint bucket for NGDI format.
        """
        mock_client = MagicMock()

        post.send_to_cd(
            "bucket",
            "test/ConvertedFiles",
            "NGDI",
            mock_client,
            "HB",
            "bucket",
            "file",
            "N",
            {"telematicsDeviceId": "device-id", "componentSerialNumber": "esn"},
            "uuid",
            "CD_PT_POSTED",
            "csv"
        )

        mock_pt_poster.send_to_pt.assert_called_with(
            "post-url",
            "header",
            {"telematicsDeviceId": "device-id", "componentSerialNumber": "esn"},
            "FILE_SENT",
            "csv",
            "HB",
            "uuid",
            "device-id",
            "esn"
        )

        mock_client.put_object.assert_not_called()
        mock_sqs_send_message.assert_not_called()
        mock_write_to_audit_table.assert_not_called()
        mock_check_endpoint_file_exists.assert_not_called()
