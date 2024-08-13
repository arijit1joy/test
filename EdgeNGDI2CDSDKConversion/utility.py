import os
from edge_simple_logging_layer import get_logger as get_log
from edge_sqs_utility_layer import send_error_to_audit_trail_queue


def get_logger(file_name):
    return get_log(file_name)


def write_to_audit_table(module_name, error_message, device_id="No Device ID"):
    error_params = {"module_name": module_name, "error_code": "500", "error_message": error_message,
                    "component_name": "NGDI2CDSDK", "device_id": device_id}
    send_error_to_audit_trail_queue(os.environ["AuditTrailQueueUrl"], error_params)
