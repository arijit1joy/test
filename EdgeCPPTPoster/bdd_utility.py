import traceback

import boto3


def update_bdd_parameter(value):
    ssm_client = boto3.client("ssm")
    try:
        ssm_client.put_parameter(
            Name="da-edge-j1939-services-bdd-parameter",
            Value=value,
            Type="String",
            Overwrite=True
        )
    except Exception as e:
        print("An Exception occurred! Error:", e)
        traceback.print_exc()


def check_bdd_parameter(expected_value):
    ssm_client = boto3.client("ssm")
    try:
        get_ssm_parm_response = ssm_client.get_parameter(
            Name="da-edge-j1939-services-bdd-parameter"
        )
        assert get_ssm_parm_response["Parameter"]["Value"] == expected_value
        return True
    except Exception as e:
        print("An Exception occurred! Error:", e)
        traceback.print_exc()
        return False
