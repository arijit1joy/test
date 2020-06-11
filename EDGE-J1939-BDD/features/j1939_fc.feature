Feature: J1939 Heart Beat Processing

  Scenario: FC file is received to EDGE cloud
    Given A valid EBU FC file in CSV format
    When The FC file is uploaded to the da-edge-j1939-datalog-files-<env> bucket
    Then A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/fc_file.json with a metadata called j1939type whose value is FC
    And A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device ID/yyyy/MM/dd/fc_file.json with a metadata called j1939type whose value is FC and CP Post Success Message is recorded

  Scenario: FC file is received to EDGE cloud for a device ID that does not exist in the EDGE ecosystem
    Given A valid EBU FC file in CSV format containing a device ID that does not exist in the EDGE ecosystem
    When The FC file is uploaded to the da-edge-j1939-datalog-files-<env> bucket
    Then A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/fc_file.json with a metadata called j1939type whose value is FC
    But No JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/deviceID/yyyy/MM/dd/fc_file.json and There is a DeviceID error recorded

  Scenario: FC file is received to EDGE cloud for an FC file with no deviceID
    Given An invalid EBU FC file in CSV format containing no device ID value
    When The FC file is uploaded to the da-edge-j1939-datalog-files-<env> bucket
    Then No JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/fc_file.json and CSV Convert success is not recorded

  Scenario: FC file is received to EDGE cloud without the single sample row
    Given A valid EBU FC message in CSV format containing a valid deviceID but missing the single sample row
    When The FC file is uploaded to the da-edge-j1939-datalog-files-<env> bucket
    Then A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/fc_file.json with a metadata called j1939type whose value is FC
    And A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device ID/yyyy/MM/dd/fc_file.json with a metadata called j1939type whose value is FC and CP Post Success Message is recorded

  Scenario: FC file is received to EDGE cloud without the all sample row
    Given An invalid EBU FC message in CSV format containing a valid deviceID but missing the all sample row
    When The FC file is uploaded to the da-edge-j1939-datalog-files-<env> bucket
    Then No JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/fc_file.json and CSV Convert success is not recorded

  Scenario: FC file is received to EDGE cloud without the device parameters
    Given A valid EBU FC message in CSV format containing no device parameters in the all sample rows
    When The FC file is uploaded to the da-edge-j1939-datalog-files-<env> bucket
    Then A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/deviceID/yyyy/MM/dd/fc_file.json with a metadata called j1939type whose value is FC
    And A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device ID/yyyy/MM/dd/fc_file.json with a metadata called j1939type whose value is FC and CP Post Success Message is recorded