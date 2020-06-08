from enum import Enum


class InternalResponse(Enum):
    J1939BDDFormatError = "Format Error"
    J1939BDDDeviceInfoError = "DeviceID Error"
    J1939BDDPSBUDeviceInfoError = "PSBU DeviceID Error"
    J1939BUDeviceInfoError = "BU Error"
    J1939BDDDeviceTypeError = "DeviceType Error"
    J1939BDDValidDevices = "152400000000000,152400000000001,InvalidDeviceID"
    J1939CPPostSuccess = '"Successfully Received The Message"'
    J1939BDDCSVConvertSuccess = "Successfully Converted NGDI CSV to NGDI JSON"


class FCCSVCase(Enum):
    ValidFCCSV = "j1939_fc_152400000000000_valid_fc"
    ValidFCCSVNoDeviceParams = "j1939_fc_152400000000000_valid_fc_no_device_params"
    ValidFCCSVNoSS = "j1939_fc_152400000000000_valid_fc_no_ss"
    ValidFCCSVInvalidDeviceID = "j1939_fc_152400000000000_valid_fc_invalid_device_id"
    InvalidFCCSVNoDeviceID = "j1939_fc_152400000000000_invalid_fc_no_device_id"
    InvalidFCCSVNoAS = "j1939_fc_152400000000000_invalid_fc_no_as"
