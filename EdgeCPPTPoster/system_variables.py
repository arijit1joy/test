from enum import Enum


class InternalResponse(Enum):
    J1939BDDFormatError = "Format Error"
    J1939BDDDeviceInfoError = "DeviceID Error"
    J1939BDDPSBUDeviceInfoError = "PSBU DeviceID Error"
    J1939BUDeviceInfoError = "BU Error"
    J1939BDDDeviceTypeError = "DeviceType Error"
    J1939BDDPTPostSuccess = "Successfully posted message to PT"
