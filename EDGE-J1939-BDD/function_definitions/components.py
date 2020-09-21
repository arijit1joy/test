import traceback

import requests
import json
import boto3
import botocore

'''

    *Do not modify the code without authorization! It is the reusable components! Please update the metadata below!*
    Last Modified: 04/03/2020
    Modified By: PX267 - Joshua Imarhiagbe

'''

s3_client = boto3.client('s3', verify=False)
iot_client = boto3.client('iot', verify=False)
iot_data_client = boto3.client('iot-data', verify=False)
lambda_client = boto3.client('lambda', verify=False)


def get_response_json(response_body, status_code, status):
    response = {
        'response_body': response_body,
        'response_status_code': status_code,
        'response_status': status
    }
    return response


def make_post_call(endpoint, payload, headers):
    try:
        api_response = requests.post(url=endpoint, data=payload, headers=headers)
        response = get_response_json(api_response.text, api_response.status_code, 'Success')
    except Exception as e:
        traceback.print_exc()
        response = get_response_json(str(e), 500, 'Error')
    return response


def make_get_call(endpoint, query_string_params, headers):
    try:
        parameter_string = '?'
        for query_string_param in query_string_params:
            parameter_string = parameter_string + query_string_param + '=' + str(
                query_string_params[query_string_param]) + '&'
            parameter_string = parameter_string[:-1]
        endpoint = endpoint + parameter_string
        api_response = requests.get(url=endpoint, headers=headers)
        response = get_response_json(api_response.text, api_response.status_code, 'Success')
    except Exception as e:
        traceback.print_exc()
        response = get_response_json(str(e), 500, 'Error')
    return response


'''
Refer this for delete jobs -
https://docs.aws.amazon.com/iot/latest/apireference/API_DeleteJob.html
'''


def iot_delete_job(job_id):
    try:
        iot_response = iot_client.delete_job(jobId=job_id, force=True)
        if 200 in iot_response:
            response = get_response_json('Successfully deleted the job with ID: ' + job_id, 200, 'Success')
        else:
            response = get_response_json(str(iot_response), 500, 'Error')
    except Exception as e:
        traceback.print_exc()
        response = get_response_json(str(e), 500, 'Error')
    return response


def iot_describe_job(job_id):
    try:
        iot_response = iot_client.describe_job(jobId=job_id)
        response = get_response_json(str(iot_response), 200, 'Success')
    except Exception as e:
        traceback.print_exc()
        response = get_response_json(str(e), 500, 'Error')
    return response


'''
See this for publish topic-
https://docs.aws.amazon.com/iot/latest/apireference/API_iotdata_Publish.html
'''


def iot_publish_topic(topic, payload):
    try:
        iot_data_response = iot_data_client.publish(topic=topic, payload=json.dumps(payload))
        if iot_data_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            response = get_response_json('Successfully published the topic: ' + topic, 200, 'Success')
        else:
            response = get_response_json(str(iot_data_response), 500, 'Error')
    except Exception as e:
        traceback.print_exc()
        response = get_response_json(str(e), 500, 'Error')
    return response


def s3_put_json_object(bucket_name, key, body, metadata=None):
    try:
        print("\nX===========================================================================X\n\n"
              "Key to put (json):", key, " <----> Bucket:", bucket_name)
        s3_put_response = s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(body).encode(),
                                               Metadata=metadata) if metadata else \
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(body).encode())
        print('S3 Put object Response:', s3_put_response)
        if s3_put_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            response = get_response_json('Successfully put the object into the bucket: ' + bucket_name, 200, 'Success')
        else:
            response = get_response_json('Error occurred while putting the object into the bucket: ' + bucket_name,
                                         500, 'Error')
    except Exception as e:
        traceback.print_exc()
        response = get_response_json(str(e), 500, 'Error')
    return response


'''
Refer this link for put csv object -
https://stackoverflow.com/questions/48399871/saving-csv-file-to-s3-using-boto3/48400499
'''


def s3_put_csv_object(context, bucket_name, body, metadata=None):
    key = context.fc_csv_file_name
    try:
        print("\nX===========================================================================X\n\n"
              "Key to put (csv):", key, " <----> Bucket:", bucket_name)
        s3_put_response = s3_client.put_object(Bucket=bucket_name, Key=key, Body=body,
                                               Metadata=metadata) if metadata else \
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
        print('S3 Put object Response:', s3_put_response)
        if s3_put_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            response = get_response_json('Successfully put the object into the bucket: ' + bucket_name, 200, 'Success')
        else:
            response = get_response_json('Error occurred while putting the object into the bucket: ' + bucket_name,
                                         500, 'Error')
    except Exception as e:
        traceback.print_exc()
        response = get_response_json(str(e), 500, 'Error')
    return response


def s3_delete_object(bucket_name, key, folder_depth=None):
    try:
        if folder_depth:
            key_split = key.split("/")
            key_array = []
            for i in range(folder_depth):
                key_array.append(key_split[i])
            key = "/".join(key_array)
        print("\nX===========================================================================X\n\nKey to delete:", key, " <----> Bucket:", bucket_name)
        s3_delete_response = s3_client.delete_object(Bucket=bucket_name, Key=key)
        print("S3 Delete Response:", s3_delete_response)
        if s3_delete_response["ResponseMetadata"]["HTTPStatusCode"] in [200, 204]:
            response = get_response_json('Successfully deleted the key: ' + key, 200, 'Success')
        else:
            response = get_response_json('Could not delete the key', 500, 'Error')
    except Exception as e:
        traceback.print_exc()
        response = get_response_json(str(e), 500, 'Error')
    return response


def s3_get_object(bucket_name, key):
    try:
        print("\nX===========================================================================X\n\n"
              "Key to get:", key, " <----> Bucket:", bucket_name)
        requested_file = s3_client.get_object(Bucket=bucket_name, Key=key)
        print("S3 Get Response:", requested_file)
        if requested_file['ResponseMetadata']['HTTPStatusCode'] == 200:
            response = get_response_json({
                "object_type": requested_file['ResponseMetadata']['HTTPHeaders']['content-type'],
                "object_string": requested_file['Body'].read().decode('utf-8'),
                "metadata": requested_file['Metadata']
            }, requested_file['ResponseMetadata']['HTTPStatusCode'], 'Success')
        else:
            response = get_response_json('An error occurred while retrieving the object.',
                                         requested_file['ResponseMetadata']['HTTPStatusCode'], 'Error')
    except Exception as e:
        traceback.print_exc()
        response = get_response_json(str(e), 500, 'Error')
    return response


def s3_check_if_key_exists(bucket_name, key, required_metadata=None, matches_json=None):
    try:
        print("\nX===========================================================================X\n\n"
              "Key to find:", key, " <----> Bucket:", bucket_name)

        response = get_response_json({
            'Message': 'Validated that the given key exists in the bucket',
            'Key': key}, 200, 'Success')

        try:
            key_object = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key, Delimiter='/')
            if "Contents" in key_object:
                key = key_object["Contents"][0]["Key"]
                s3_object_info = s3_client.get_object(Bucket=bucket_name, Key=key)
                print("S3 Get Response:", s3_object_info)
                from collections import Counter
                if required_metadata:
                    s3_object_metadata = s3_object_info["Metadata"]
                    key_match_list = [meta for meta in list(required_metadata.keys()) if
                                      meta in list(s3_object_metadata.keys())]
                    key_match_dict = {key: s3_object_metadata[key] for key in
                                      [meta for meta in list(required_metadata.keys()) if
                                       meta in list(s3_object_metadata.keys())]}
                    if len(key_match_list) != len(list(required_metadata.keys())) or \
                            Counter(key_match_dict) != Counter(required_metadata):
                        response = get_response_json('Key exists, but is missing metadata requirement', 500, 'Error')
                if matches_json:
                    s3_object_body = s3_object_info["Body"].read().decode("utf-8")
                    json_body = json.loads(s3_object_body)
                    # Please Leave the below - even though commented - for future debugging
                    print("\nX===========================================================================X\n\n"
                          "Comparing the JSON ---->", json_body, "to JSON ---->", matches_json, sep="\n")
                    if Counter(json_body) != Counter(matches_json):
                        response = get_response_json('Key exists, but the format is not as expected', 500, 'Error')
            else:
                response = get_response_json('Key path does not exist!', 500, 'Error')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                response = get_response_json('Key does not exist in the bucket', 500, 'Error')
            else:
                print("Some error occurred while getting the object:", e)
                response = get_response_json('Error occurred while checking if key exists!', 500, 'Error')
    except Exception as e:
        traceback.print_exc()
        response = get_response_json(str(e), 500, 'Error')
    return response


def invoke_lambda_function(function_name, payload):
    try:
        lambda_response = lambda_client.invoke(FunctionName=function_name, Payload=payload)
        if lambda_response['StatusCode'] == 200 or lambda_response['StatusCode'] == 202 or \
                lambda_response['StatusCode'] == 204:
            response = get_response_json(lambda_response['Payload'].read().decode('utf-8'),
                                         lambda_response['StatusCode'], 'Success')
        else:
            response = get_response_json(lambda_response, lambda_response['StatusCode'], 'Error')
    except Exception as e:
        traceback.print_exc()
        response = get_response_json(str(e), 500, 'Error')
    return response
