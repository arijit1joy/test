Feature: Regression Test Cases for J1939 Fault Code Process

# skipping the feature due to failure in BDD stage

#Scenarios for EBU Devices
 Scenario: FC file is received to EDGE cloud with the valid data for EBU
   Given A valid EBU FC message in CSV format containing a valid data
   When The FC file is uploaded to the device-data-log-<env> bucket
   Then Stored J1939 FC metadata stages in EDGE DB
   Then A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/device_id/yyyy/mm/dd/fc_file.json with a metadata called j1939type whose value is FC
   Then A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/fc_file.json with a metadata called j1939type whose value is FC and CP Post Success Message is recorded
  Scenario: FC file is received to EDGE cloud for a device_id that does not exist in the EDGE ecosystem
    Given A valid EBU FC file in CSV format containing a device_id that does not exist in the EDGE ecosystem
    When The FC file is uploaded to the device-data-log-<env> bucket
    Then Stored J1939 FC metadata stages in EDGE DB
  Scenario: FC file is received to EDGE cloud for an FC file with no device_id
    Given An invalid EBU FC file in CSV format containing no device_id value
    When The FC file is uploaded to the device-data-log-<env> bucket
    Then Stored J1939 FC metadata stages in EDGE DB by ESN
 #Scenarios for PSBU Devices
  Scenario: FC file is received to EDGE cloud with the valid data for PSBU
    Given A valid PSBU FC message in CSV format containing a valid data
    When The FC file is uploaded to the device-data-log-<env> bucket
    Then Stored J1939 FC metadata stages in EDGE DB
    Then A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/device_id/yyyy/mm/dd/fc_file.json with a metadata called j1939type whose value is FC
    Then No JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/fc_file.json
  # Scenario: FC file is received without ESN in Filename to EDGE cloud with the valid data for PSBU
  #   Given A valid PSBU FC message in CSV format containing a valid data and filename without ESN
  #   When The FC file is uploaded to the device-data-log-<env> bucket
  #   Then Stored J1939 FC metadata stages in EDGE DB
  #   Then A JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path ConvertedFiles/esn/device_id/yyyy/mm/dd/fc_file.json with a metadata called j1939type whose value is FC
  #   Then No JSON file is created with the FC message in NGDI JSON format as its content and is stored in the edge-j1939-<env> bucket under the file path NGDI/esn/device_id/yyyy/mm/dd/fc_file.json
