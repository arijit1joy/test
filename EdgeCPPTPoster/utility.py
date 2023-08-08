import os

from edge_core_layer.edge_logger import logging_framework,send_error_to_audit_trail_queue


def get_logger(file_name):
    converted_file_name = ''.join([word.capitalize() for word in file_name.split('_')])
    logger = logging_framework(f"EdgeCPPTPoster.{converted_file_name}", os.environ["LoggingLevel"])

    return logger


def write_to_audit_table(module_name, error_message, device_id="No Device ID"):
    error_params = {"module_name": module_name, "error_code": "500", "error_message": error_message,
                    "component_name": "CPPTPoster", "device_id": device_id}
    send_error_to_audit_trail_queue(os.environ["AuditTrailQueueUrl"], error_params)
