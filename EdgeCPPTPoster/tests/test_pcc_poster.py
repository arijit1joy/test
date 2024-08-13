import os
import sys
import json

sys.path.append("../")
import unittest
from unittest.mock import patch, MagicMock, call


from tests.cda_module_mock_context import CDAModuleMockingContext

with  CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict("os.environ", {
    "LoggingLevel": "debug",
    "Region": "us-east-1",
    "PTxAPIKey": "123123",
    "PTJ1939Header": '{"Content-Type": "application/json", "Prefer": "param=single-object", "x-api-key": ""}',
    "ptTopicInfo": '{"topicName": "nimbuspt_j1939-j1939-pt-topic", "bu":"PSBU","file_type":"JSON"}',
    "Latitude": "39.202938",
    "Longitude": "-85.88672",
    "pcc_role_arn": "test",
    "j1939_stream_arn": "test",
    "pcc_region": "us-east-1",
    "pcc2_role_arn": "test",
    "pcc2_j1939_stream_arn": "test",
    "pcc2_region": "us-east-1"


}):
    cda_module_mock_context.mock_module("boto3")
    cda_module_mock_context.mock_module("utility")
    cda_module_mock_context.mock_module("edge_sqs_utility_layer")
    cda_module_mock_context.mock_module("pt_poster")

    import pcc_poster


class PCCPoster(unittest.TestCase):
    """
    Test module for pcc_poster.py
    """

    hb_params = {
        "CPU_Usage_Level": "2.02",
        "LTE_RSRP": "-107",
        "LTE_RSRQ": "-6",
        "messageID": "8c5a1650-048e-4620-9950-bc14fa4b46b7",
        "Latitude": "39.202938",
        "CPU_temperature": "40.3",
        "Satellites_Used": "20",
        "Longitude": "-85.88672",
        "PDOP": "1.159999967",
        "LTE_RSCP": "255",
        "PMIC_temperature": "33.25",
        "LTE_RSSI": "99",
        "Altitude": "165.236"
    }

    fc_params = [
        {
            "protocol": "J1939",
            "networkId": "CAN1",
            "deviceId": "0",
            "activeFaultCodes": [
                {"spn": "100", "fmi": "4", "count": "1"},
                {"spn": "101", "fmi": "4", "count": "1"}
            ],
            "inactiveFaultCodes": [{"spn": "100", "fmi": "4", "count": "1"}],
            "pendingFaultCodes": [{"spn": "100", "fmi": "4", "count": "1"}]
        }
    ]

    json_body = {
        "messageFormatVersion": "1.1.1",
        "telematicsPartnerName": "Cummins",
        "customerReference": "Cummins",
        "componentSerialNumber": "CMMNS**1929992**************************************************************",
        "rel_smn":"Test123",
        "equipmentId": "EDGE_19299954",
        "vin": "TESTVIN19299954",
        "telematicsDeviceId": "192999999999954",
        "dataSamplingConfigId": "Event1_5",
        "dataEncryptionSchemeId": "ES1",
        "numberOfSamples": 1,
        "samples": [
            {
                "convertedDeviceParameters": hb_params,
                "rawEquipmentParameters": [],
                "convertedEquipmentParameters": [
                    {
                        "protocol": "J1939",
                        "networkId": "CAN1",
                        "deviceId": "0",
                        "parameters": {
                            "190": "",
                            "174": "",
                            "175": "",
                            "110": "",
                            "100": "",
                            "101": "",
                            "102": "",
                            "168": "",
                            "157": "",
                            "105": "",
                            "513": "",
                            "899": "",
                            "91": "",
                            "92": "",
                            "109": ""
                        }
                    }
                ],
                "convertedEquipmentFaultCodes": fc_params,
                "dateTimestamp": "2021-02-09T12:30:00.015Z"
            }
        ]
    }

    @patch.dict('os.environ',
                {'metaWriteQueueUrl': 'test'})
    @patch("pcc_poster.sqs_send_message")
    @patch("pcc_poster.handle_hb_params")
    @patch("pcc_poster.boto3.client")
    def test_send_to_pcc_given(self, mock_client, hb_params: MagicMock(), sqs_send_message: MagicMock):
        hb_params.return_value = self.hb_params

        response = pcc_poster.send_to_pcc(self.json_body, "123456789", "J1939-HB", "None","null","claimed@pcc2.0")
        print(response)
        assert call().put_record(StreamARN='test', Data=json.dumps(self.json_body, indent=2).encode('utf-8'),
                                 PartitionKey='123456789-J1939-HB') in mock_client.mock_calls
        pcc_poster.sqs_send_message.assert_called()


    def test_handle_fc_params_successful(self):
        """
        Test for handle_fc_params() running successfully.
        """
        converted_fc_params = [
            {
                "protocol": "J1939",
                "networkId": "CAN1",
                "deviceId": "0",
                "activeFaultCodes": [
                    {"spn": "100", "fmi": "4", "occurenceCount": "1"},
                    {"spn": "101", "fmi": "4", "occurenceCount": "1"}
                ],
                "inactiveFaultCodes": [{"spn": "100", "fmi": "4", "occurenceCount": "1"}],
                "pendingFaultCodes": [{"spn": "100", "fmi": "4", "occurenceCount": "1"}]
            }
        ]

        response = pcc_poster.handle_fc_params(self.fc_params)

        self.assertEqual(response, converted_fc_params)


if __name__ == '__main__':
    unittest.main()
