import traceback

import boto3


def update_bdd_parameter(value):
    print("Value to update to ssm parameter: da-edge-j1939-services-bdd-parameter --->", value)
    ssm_client = boto3.client("ssm")
    try:
        update_ssm_param_response = ssm_client.put_parameter(
            Name="da-edge-j1939-services-bdd-parameter",
            Value=value,
            Type="String",
            Overwrite=True
        )
        print("Update SSM Parameter Response:", update_ssm_param_response)
    except Exception as e:
        print("An Exception occurred! Error:", e)
        traceback.print_exc()


def check_bdd_parameter(expected_value):
    print("Expected Value:", expected_value)
    ssm_client = boto3.client("ssm")
    try:
        get_ssm_param_response = ssm_client.get_parameter(
            Name="da-edge-j1939-services-bdd-parameter"
        )
        print("Get SSM Parameter Response:", get_ssm_param_response)
        assert get_ssm_param_response["Parameter"]["Value"] == expected_value
        return True
    except Exception as e:
        print("An Exception occurred! Error:", e)
        traceback.print_exc()
        return False
