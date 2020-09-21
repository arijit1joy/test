Feature: J1939 Heart Beat Processing

  Scenario Outline: HB message is received to EDGE cloud
    Given A valid <Business unit> HB message in JSON format
    When The HB is posted to the /public topic
    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/hb_file.json with no metadata
    And <Further Action>

    Examples: EBU/PT Variables
      | Business unit         | Further Action|
      | EBU                   | A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device ID/yyyy/MM/dd/hb_file.json with no metadata and CP Post Success Message is recorded|
      | PT                    | PT Posting Success is recorded                                                                                                                                                                                              |

  Scenario: HB message is received to EDGE cloud for a device ID that does not exist in the EDGE ecosystem
    Given An valid EBU HB message in JSON format containing a device ID that does not exist in the EDGE ecosystem
    When The HB is posted to the /public topic
    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/hb_file.json with no metadata
    But No JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/deviceID/yyyy/MM/dd/hb_file.json and There is a DeviceID error recorded

  Scenario: HB message is received to EDGE cloud for an invalid HB file without telematicsPartnerName and customerReference
    Given An invalid EBU HB message in JSON format containing a valid deviceID but missing the telematicsPartnerName and customerReference
    When The HB is posted to the /public topic
    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/hb_file.json with no metadata
    And A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device ID/yyyy/MM/dd/hb_file.json with no metadata and There is no CP post success recorded

  Scenario: HB message is received to EDGE cloud for an invalid HB file having incorrect values for the telematicsPartnerName and customerReference
    Given An invalid EBU HB message in JSON format containing a valid deviceID but having incorrect values for the telematicsPartnerName and customerReference
    When The HB is posted to the /public topic
    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/hb_file.json with no metadata
    And A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device ID/yyyy/MM/dd/hb_file.json with no metadata and There is no CP post success recorded