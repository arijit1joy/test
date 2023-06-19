import os
import json
import sys

sys.path.append("../")
import unittest
from unittest.mock import patch, MagicMock

sys.modules["edge_logger"] = MagicMock()
sys.modules["sqs_utility"] = MagicMock()
sys.modules["kafka"] = MagicMock()
sys.modules["boto3"] = MagicMock()
sys.modules["requests"] = MagicMock()
sys.modules["kafka_producer"] = MagicMock()

# sys.modules["kafka_producer"] = MagicMock()
sys.modules["obfuscate_gps_utility"] = MagicMock()
sys.modules["metadata_utility"] = MagicMock()
sys.modules["utility"] = MagicMock()
from cda_module_mock_context import CDAModuleMockingContext

with  CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict("os.environ", {
    "LoggingLevel": "debug",
    "Region": "us-east-1",
    "PTxAPIKey": "123123",
    "PTJ1939Header": '{"Content-Type": "application/json", "Prefer": "param=single-object", "x-api-key": ""}',
    "edgeCommonAPIURL": "",
    "ptTopicInfo": '{"topicName": "nimbuspt_j1939-j1939-pt-topic", "bu":"PSBU","file_type":"JSON"}',
    "Latitude": "39.202938",
    "Longitude": "-85.88672"

}):
    # cda_module_mock_context.mock_module("edge_logger"),
    # cda_module_mock_context.mock_module("sqs_send_message"),
    cda_module_mock_context.mock_module("kafka_producer.publish_message")
    # cda_module_mock_context.mock_module("kafka_producer._create_kafka_message")
    # cda_module_mock_context.mock_module("obfuscate_gps_utility.handle_gps_coordinates")
    # cda_module_mock_context.mock_module("metadata_utility.write_health_parameter_to_database")
    # cda_module_mock_context.mock_module(
    #     "utility.util", mock_object=MagicMock(
    #         get_logger=MagicMock(
    #             return_value=(MagicMock(), "TestPTPoster",)
    #         )
    #     )
    # )
    import pt_poster
class MyTestCase(unittest.TestCase):
    post_url = "https://json"
    headers = '{"Content-Type": "application/json", "Prefer": "param=single-object", "x-api-key": ""}'
    json_body = {"messageFormatVersion": "1.1.1", "telematicsPartnerName": "Cummins", "customerReference": "Cummins",
                 "componentSerialNumber": "CMMNS**19299954**************************************************************",
                 "equipmentId": "EDGE_19299954", "vin": "TESTVIN19299954", "telematicsDeviceId": "192999999999954",
                 "dataSamplingConfigId": "Event1_5", "dataEncryptionSchemeId": "ES1", "numberOfSamples": 1, "samples": [
            {"convertedDeviceParameters": {"CPU_Usage_Level": "2.02", "LTE_RSRP": "-107", "LTE_RSRQ": "-6",
                                           "messageID": "8c5a1650-048e-4620-9950-bc14fa4b46b7", "Latitude": "39.202938",
                                           "CPU_temperature": "40.3", "Satellites_Used": "20", "Longitude": "-85.88672",
                                           "PDOP": "1.159999967", "LTE_RSCP": "255", "PMIC_temperature": "33.25",
                                           "LTE_RSSI": "99", "Altitude": "165.236"
                                           }, "rawEquipmentParameters": [], "convertedEquipmentParameters": [
                {"protocol": "J1939", "networkId": "CAN1", "deviceId": "0",
                 "parameters": {"190": "", "174": "", "175": "", "110": "", "100": "", "101": "", "102": "", "168": "",
                                "157": "", "105": "", "513": "", "899": "", "91": "", "92": "", "109": ""
                                }
                 }
            ], "convertedEquipmentFaultCodes": [
                {"protocol": "J1939", "networkId": "CAN1", "deviceId": "0", "activeFaultCodes": [
                    {"spn": "100", "fmi": "4", "count": "1"
                     },
                    {"spn": "101", "fmi": "4", "count": "1"
                     }
                ], "inactiveFaultCodes": [], "pendingFaultCodes": []
                 }
            ], "dateTimestamp": "2021-02-09T12: 30: 00.015Z"
             }
        ]
                 }
    j1939_data_type = "FC"
    j1939_type = "FC"
    file_uuid = '88123123123'

    device_id = '192999999999954'

    config_spec_and_req_id = "1234"

    file_name = 'TestKafka'
    file_size = '10'
    esn = "CMMNS**19299954**************************************************************"
    sqs_message_template = \
        f"{file_uuid},{device_id},{file_name},{str(file_size)}," \
        f"{'{FILE_METADATA_CURRENT_DATE_TIME}'},{j1939_data_type}," \
        f"{'{FILE_METADATA_FILE_STAGE}'},{esn},{config_spec_and_req_id}"

    headers_json = {
        "SecretString": {
            "x-api-key": "12345"
        }
    }
    hb_param_json = {"CPU_Usage_Level": "2.02", "LTE_RSRP": "-107", "LTE_RSRQ": "-6",
                     "messageID": "8c5a1650-048e-4620-9950-bc14fa4b46b7", "Latitude": "39.202938",
                     "CPU_temperature": "40.3", "Satellites_Used": "20", "Longitude": "-85.88672",
                     "PDOP": "1.159999967", "LTE_RSCP": "255", "PMIC_temperature": "33.25", "LTE_RSSI": "99",
                     "Altitude": "165.236"
                     }

    @patch.dict('os.environ', {'publishKafka': 'False'})
    @patch("pt_poster.util")
    @patch("pt_poster.requests")
    @patch("pt_poster.LOGGER")
    @patch("pt_poster.publish_message")
    @patch("pt_poster._create_kafka_message")
    @patch("pt_poster.store_device_health_params")
    @patch("pt_poster.handle_hb_params")
    @patch("pt_poster.sec_client")
    def test_send_to_pt_given(self, mocK_sec_client: MagicMock,
                              hb_params: MagicMock(), health_params: MagicMock,
                              create_kafka: MagicMock, publish_message: MagicMock,
                              mock_logger: MagicMock, mock_requests: MagicMock, mock_util: MagicMock):
        mocK_sec_client.return_value = self.headers_json
        hb_params.return_value = self.hb_param_json

        pt_poster.send_to_pt(self.post_url,
                             self.headers, self.json_body, self.sqs_message_template, self.j1939_data_type,
                             self.j1939_type,
                             self.file_uuid, self.device_id, self.esn)
        create_kafka.assert_not_called()
        publish_message.assert_not_called()

    @patch.dict('os.environ', {'publishKafka': 'True'})
    @patch("pt_poster.LOGGER")
    @patch("pt_poster.requests")
    @patch("pt_poster.publish_message")
    @patch("pt_poster._create_kafka_message")
    @patch("pt_poster.store_device_health_params")
    @patch("pt_poster.handle_hb_params")
    @patch("pt_poster.sec_client")
    def test_send_to_pt_given_publish_kafka_then_publish_message(self, mocK_sec_client: MagicMock,
                                                                 hb_params: MagicMock(), health_params: MagicMock,
                                                                 create_kafka: MagicMock, publish_message: MagicMock,
                                                                 mock_requests: MagicMock, mock_util: MagicMock):
        mocK_sec_client.return_value = self.headers_json
        hb_params.return_value = self.hb_param_json
        pt_poster.send_to_pt(self.post_url,
                             self.headers, self.json_body, self.sqs_message_template, self.j1939_data_type,
                             self.j1939_type,
                             self.file_uuid, self.device_id, self.esn)
        create_kafka.assert_called_once()
        publish_message.assert_called_once()


if __name__ == '__main__':
    unittest.main()
