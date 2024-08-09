import json
import sys
import unittest

from tests.cda_module_mock_context import CDAModuleMockingContext
from unittest.mock import ANY, patch

with CDAModuleMockingContext(sys) as cda_module_mock_context:
    cda_module_mock_context.mock_module("edge_core_layer.edge_logger")

    import utility


class TestUtility(unittest.TestCase):
    """
    Test module for utility.py
    """

    @patch.dict("os.environ", {"LoggingLevel": "info"})
    @patch("utility.logging_framework")
    def test_get_logger_successful(self, mock_logging_framework):
        """
        Test for get_logger() running successfully.
        """
        response = utility.get_logger("file_naMe")

        mock_logging_framework.assert_called_with("EdgeNGDI2CDSDKConversion.FileName", "info")
        self.assertEqual(response, mock_logging_framework.return_value)


    @patch.dict("os.environ", {"AuditTrailQueueUrl": "url"})
    @patch("utility.send_error_to_audit_trail_queue")
    def test_write_to_audit_table_successful(self, mock_send_error_fn):
        """
        Test for write_to_audit_table() running successfully.
        """
        utility.write_to_audit_table("module-name", "error-message")

        mock_send_error_fn.assert_called_with(
            "url",
            {
                "module_name": "module-name",
                "error_code": "500",
                "error_message": "error-message",
                "component_name": "NGDI2CDSDK",
                "device_id": "No Device ID"
            }
        )
