import traceback

import boto3


def update_bdd_parameter(value, param_name=None):
    ssm_client = boto3.client("ssm", verify=False)
    try:
        set_ssm_parameter_response = ssm_client.put_parameter(
            Name="da-edge-j1939-services-bdd-parameter" if not param_name else param_name,
            Value=value,
            Type="String",
            Overwrite=True
        )
        print("Set SSM Parameter Response:", set_ssm_parameter_response)
    except Exception as e:
        print("An Exception occurred! Error:", e)
        traceback.print_exc()


def check_bdd_parameter(expected_value, param_name=None, get_parameter=False):
    ssm_client = boto3.client("ssm", verify=False)
    try:
        get_ssm_param_response = ssm_client.get_parameter(
            Name="da-edge-j1939-services-bdd-parameter" if not param_name else param_name,
        )
        print("Get SSM Parameter Response:", get_ssm_param_response)
        received_value = get_ssm_param_response["Parameter"]["Value"]
        # Please Leave the below - even though commented - for future debugging
        print("Comparing the Value ---->", str(received_value), "to Value ---->", str(expected_value), sep="\n")
        if expected_value:
            assert received_value == expected_value, "The SSM value was not what was expected"
        return True if not get_parameter else received_value
    except Exception as e:
        print("An Exception occurred! Error:", e)
        traceback.print_exc()
        return False
