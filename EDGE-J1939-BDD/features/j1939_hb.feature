Feature: J1939 Heart Beat Processing

  Scenario Outline: HB message is received to EDGE cloud
    Given A valid <Business unit> HB message in JSON format
    When The HB is posted to the /public topic
    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/hb_file.json with a metadata called "j1939-type" whose value is "HB"
    And <Further Action>

    Examples: EBU/PT Variables
      | Business unit         | Further Action|
      | EBU                   | A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device ID/yyyy/MM/dd/hb_file.json with a metadata called "j1939-type" whose value is "HB"|
      | PT                    | Success Message                                                                                                                                                                                                              |

#  Scenario Outline: HB message is received to EDGE cloud for a device ID that does not exist in the EDGE ecosystem
#    Given An valid <Business unit> HB message in JSON format containing a device ID that does not exist in the EDGE ecosystem
#    When The HB is posted to /public topic
#    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/hb_file.json
#    But <Further Negative Action>
#
#    Examples: EBU/PT Variables
#      | Business unit         | Further Negative Action|
#      | EBU                   | |
#      | PT                    | Success Message
#
#  Scenario Outline: HB message is received to EDGE cloud for a device ID that does not exist
#    Given An valid <Business unit> HB message in JSON format containing a device ID that does not exist in the EDGE ecosystem
#    When The HB is posted to /public topic
#    Then A JSON file is created with the HB message as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/hb_file.json
#    But <Further Negative Action>
#
#    Examples: EBU/PT Variables
#      | Business unit         | Further Negative Action|
#      | EBU                   | |
#      | PT                    | Success Message
#                                                                                                                                        |