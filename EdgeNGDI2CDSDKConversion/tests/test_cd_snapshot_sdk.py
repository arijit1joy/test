import json
import sys
import unittest

from tests.cda_module_mock_context import CDAModuleMockingContext
from unittest.mock import ANY

with CDAModuleMockingContext(sys) as cda_module_mock_context:
    cda_module_mock_context.mock_module("utility")

    import cd_sdk_conversion.cd_snapshot_sdk as cd_snapshot_sdk


class TestCdSnapshotSdk(unittest.TestCase):
    """
    Test module for cd_snapshot_sdk.py
    """

    def test_get_snapshot_data_successful(self):
        """
        Test for get_snapshot_data() running successfully.
        """
        response = cd_snapshot_sdk.get_snapshot_data(
            {"param1": "val1"},
            "timestamp",
            "address",
            {"param1": "file1"}
        )

        self.assertEqual(
            response,
            [
                {
                    "Snapshot_DateTimestamp": ANY,
                    "Parameter": [
                        {
                            "Name": "file1",
                            "Value": "val1",
                            "Parameter_Source_Address": "address",    
                        }
                    ]
                }
            ]
        )


    def test_get_snapshot_data_on_error(self):
        """
        Test for get_snapshot_data() when it throws an exception.
        """
        with self.assertRaises(Exception):
            cd_snapshot_sdk.get_snapshot_data(
                3,
                "timestamp",
                "address",
                {"param1": "file1"}
            )
