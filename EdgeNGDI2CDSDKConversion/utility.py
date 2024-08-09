import os
from edge_sqs_utility_layer.edge_logger import logging_framework
from edge_sqs_utility_layer.sqs_utility import send_error_to_audit_trail_queue


def get_logger(file_name):
    converted_file_name = ''.join([word.capitalize() for word in file_name.split('_')])
    logger = logging_framework(f"EdgeNGDI2CDSDKConversion.{converted_file_name}", os.environ["LoggingLevel"])

    return logger


def write_to_audit_table(module_name, error_message, device_id="No Device ID"):
    error_params = {"module_name": module_name, "error_code": "500", "error_message": error_message,
                    "component_name": "NGDI2CDSDK", "device_id": device_id}
    send_error_to_audit_trail_queue(os.environ["AuditTrailQueueUrl"], error_params)
