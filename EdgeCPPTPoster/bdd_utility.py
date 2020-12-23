import traceback

import boto3
import edge_logger as logging


logger = logging.logging_framework("EdgeCPPTPoster.BddUtility")


def update_bdd_parameter(value, param_name=None):
    ssm_client = boto3.client("ssm")
    try:
        set_ssm_parameter_response = ssm_client.put_parameter(
            Name="da-edge-j1939-services-bdd-parameter" if not param_name else param_name,
            Value=value,
            Type="String",
            Overwrite=True
        )
        logger.info(f"Set SSM Parameter Response: {set_ssm_parameter_response}")
    except Exception as e:
        logger.error(f"An Exception occurred! Error: {e}")
        traceback.print_exc()
