Feature: Regression Test Cases for J1939 Heart Beat Process

# Scenarios for EBU Devices
#  Scenario: HB message is received to EDGE cloud with the valid data for EBU
#    Given A valid EBU HB message in JSON format containing a valid data
#    When The HB is posted to the /public topic
#    Then Stored J1939 HB metadata stages in EDGE DB
#    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/device_id/yyyy/mm/dd/hb_file.json with no metadata
#    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/hb_file.json with no metadata
#
#  Scenario: HB message is received to EDGE cloud for a device ID that does not exist in the EDGE ecosystem
#    Given A valid EBU HB message in JSON format containing a device ID that does not exist in the EDGE ecosystem
#    When The HB is posted to the /public topic
#    Then Stored J1939 HB metadata stages in EDGE DB
#    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/device_id/yyyy/mm/dd/hb_file.json with no metadata
#    Then No JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/hb_file.json with no metadata
#
#  Scenario: HB message is received to EDGE cloud for an invalid HB file without telematicsPartnerName and customerReference
#    Given An invalid EBU HB message in JSON format containing a valid deviceID but missing the telematicsPartnerName and customerReference
#    When The HB is posted to the /public topic
#    Then Stored J1939 HB metadata stages in EDGE DB
#    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/device_id/yyyy/mm/dd/hb_file.json with no metadata
#    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/hb_file.json with no metadata
#
#  Scenario: HB message is received to EDGE cloud for an invalid HB file having incorrect values for the telematicsPartnerName and customerReference
#    Given An invalid EBU HB message in JSON format containing a valid deviceID but having incorrect values for the telematicsPartnerName and customerReference
#    When The HB is posted to the /public topic
#    Then Stored J1939 HB metadata stages in EDGE DB
#    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/device_id/yyyy/mm/dd/hb_file.json with no metadata
#    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/hb_file.json with no metadata
#
## Scenarios for PSBU Devices
#  Scenario: HB message is received to EDGE cloud with the valid data for PSBU
#    Given A valid PSBU HB message in JSON format containing a valid data
#    When The HB is posted to the /public topic
#    Then Stored J1939 HB metadata stages in EDGE DB
#    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/device_id/yyyy/mm/dd/hb_file.json with no metadata
#    Then No JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/hb_file.json with no metadata
