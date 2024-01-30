import sys
import unittest

from tests.cda_module_mock_context import CDAModuleMockingContext
from unittest.mock import ANY

with CDAModuleMockingContext(sys) as cda_module_mock_context:
    cda_module_mock_context.mock_module("utility")

    import cd_sdk_conversion.cd_sdk as cd_sdk


class TestCdSdk(unittest.TestCase):
    """
    Test module for cd_sdk.py
    """

    def test_map_ngdi_sample_to_cd_payload(self):
        """
        Test for map_ngdi_sample_to_cd_payload() running successfully.
        """
        response = cd_sdk.map_ngdi_sample_to_cd_payload(
            {
                "notification_version": "1",
                "telematics_partner_name": "Cummins",
                "test": 3
            }
        )

        self.assertEqual(
            response,
            {
                "Notification_Version": "1",
                "Message_Type": "HB",
                "Telematics_Box_ID": "",
                "Telematics_Partner_Message_ID": "",
                "Telematics_Partner_Name": "Cummins",
                "Customer_Reference": "",
                "Equipment_ID": "",
                "Engine_Serial_Number": "",
                "VIN": "",
                "Occurrence_Date_Time": "",
                "Sent_Date_Time": "",
                "Source_Address": "",
                "Latitude": "0.000",
                "Longitude": "0.000",
                "Altitude": "",
                "Direction_Heading": "",
                "Vehicle_Distance": "",
                "Location_Text_Description": "",
                "GPS_Vehicle_Speed": "",
                "Software_Identification": "",
                "Active_Faults": "",
                "Customer_Equipment_Group": {},
                "Calibration_Identification": [],
                "Calibration_Verification_Number": [],
                "Number_of_Software_Identification_Fields": [],
                "Make": "",
                "Model": "",
                "Unit_number": "",
                "Snapshots": []
            }
        )
