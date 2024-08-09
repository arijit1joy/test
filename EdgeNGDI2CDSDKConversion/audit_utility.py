import os
from utility import send_error_to_audit_trail_queue, logging_framework

logger = logging_framework("EdgeJ1939NGDI2CDSKConversion.audit_utility")
ERROR_PARAMS = {"module_name": "NGDI2CDSKConversion", "component_name": "NGDI2CDSDK"}


def write_to_audit_table(error_code, error_message):
    global ERROR_PARAMS

    ERROR_PARAMS["error_code"] = str(error_code)
    ERROR_PARAMS["error_message"] = error_message
    logger.info(f"calling method send_error_to_audit_trail_queue with error params : {ERROR_PARAMS}")
    send_error_to_audit_trail_queue(os.environ["AuditTrailQueueUrl"], ERROR_PARAMS)