Feature: checking for the device whitelisting

  Scenario: Whitelisting a Device
    Given Edge device whitelist api
    When Api is invoked with proper json body
    Then Check if status code is "200" or not -wl

  Scenario: Registering a Device
    Given a whitelisted device with the registration API
    When Api is invoked with proper json body for that device
    Then Check if status code is "200" or not -reg

  Scenario: Config file upload to DCS portal
    Given A valid config spec
    And Upload URL
    When upload URL is invoked
    Then File should be in S3

  Scenario: Association of config spec to the device
    Given Config spec is in S3
    And Endpoint URL
    And Device ID
    When Association URL is invoked
    Then A new Job ID for that Device in IOT and EDG_DEVICE_ACTIVITY table