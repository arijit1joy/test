import json
import os

try:
    import boto3
except ModuleNotFoundError:
    print("ERROR: Connection Util cannot import boto3. Is boto3 installed?")


def _invoke_lambda(function_name, query):
    try:
        lambda_client = boto3.client("lambda")

        db_read_lambda_response = lambda_client.invoke(
            FunctionName=function_name, InvocationType='RequestResponse', LogType='None',
            Payload=bytes(json.dumps(query).encode("utf-8"))
        )

        # Decode and return the lambda's response
        return json.loads(db_read_lambda_response["Payload"].read().decode("utf-8"))
    except NameError:
        print("ERROR: boto3 has not been imported properly")


def invoke_db_reader(read_query):
    if "EDGEDBReader_ARN" not in os.environ:
        print("ERROR when connecting to DBReader: EDGEDBReader_ARN does not exist in os.environ!")
        return None

    edge_db_reader_name = os.environ["EDGEDBReader_ARN"]
    return _invoke_lambda(edge_db_reader_name, read_query)


def invoke_db_common_api(write_query):
    if "EDGEDBCommonAPI_ARN" not in os.environ:
        print("ERROR when connecting to CommonAPI: EDGEDBCommonAPI_ARN does not exist in os.environ!")
        return None

    edge_common_api_name = os.environ["EDGEDBCommonAPI_ARN"]
    return _invoke_lambda(edge_common_api_name, write_query)
