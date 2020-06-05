from enum import Enum


class InternalResponse(Enum):
    J1939BDDFormatError = "Format Error"
    J1939BDDDeviceInfoError = "DeviceID Error"
    J1939BDDPSBUDeviceInfoError = "PSBU DeviceID Error"
    J1939BUDeviceInfoError = "BU Error"
    J1939BDDDeviceTypeError = "DeviceType Error"
    J1939BDDValidDevices = "152400000000000,152400000000001,InvalidDeviceID"
    J1939CPPostSuccess = '"Successfully Received The Message"'
