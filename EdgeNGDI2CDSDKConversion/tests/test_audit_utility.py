import io
import json
import sys
import unittest

from tests.cda_module_mock_context import CDAModuleMockingContext
from unittest.mock import patch

with CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict(
        "os.environ",
        {"AWS_DEFAULT_REGION": "region"}
    ):
    cda_module_mock_context.mock_module("edge_sqs_utility_layer.edge_logger")
    cda_module_mock_context.mock_module("edge_sqs_utility_layer.sqs_utility")
    cda_module_mock_context.mock_module("edge_simple_logging_layer")
    cda_module_mock_context.mock_module('edge_sqs_utility_layer')

    import audit_utility


class TestAuditUtility(unittest.TestCase):
    """
    Test module for audit_utility.py
    """

    @patch.dict("os.environ", {"AuditTrailQueueUrl": "url"})
    @patch("audit_utility.send_error_to_audit_trail_queue")
    def test_fetch_spn_file_successful(self, mock_send_error_fn):
        """
        Test for _fetch_bdd_esn() running successfully.
        """
        audit_utility.write_to_audit_table(400, "message")

        mock_send_error_fn.assert_called_with(
            "url",
            {
                "module_name": "NGDI2CDSKConversion",
                "component_name": "NGDI2CDSDK",
                "error_code": "400",
                "error_message": "message"
            }
        )
