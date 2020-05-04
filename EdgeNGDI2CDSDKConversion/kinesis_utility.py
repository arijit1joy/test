import boto3
import json
import os
import traceback

'''
We'll receive the values as an array for the states columns from the function call in another python class.
'''


def get_return_response(status_code, body):
    return_response = {
        'status_code': status_code,
        'body': body
    }
    return return_response


def write_into_kinesis(final_json):

    env = os.environ['environment']
    if env == 'stage':
        client = boto3.client('sts')
        sts_response = client.assume_role(RoleArn=os.environ['cross_account_kinesis_role_arn'],
                                          RoleSessionName='AssumeCrossAccountRole', DurationSeconds=900)
        kinesis_firehose_client = boto3.client('firehose', region_name='us-east-2',
                                               aws_access_key_id=sts_response['Credentials']['AccessKeyId'],
                                               aws_secret_access_key=sts_response['Credentials']['SecretAccessKey'],
                                               aws_session_token=sts_response['Credentials']['SessionToken'])

    else:
        kinesis_firehose_client = boto3.client('firehose', 'us-east-2')
    try:
        response = kinesis_firehose_client.put_record(DeliveryStreamName=os.environ['delivery_stream_name'], Record={
            'Data': json.dumps(final_json).encode('utf-8')
        })
        print('Kinesis response: ', response)
        if response['RecordId']:
            return_response = get_return_response(200, json.dumps(response))
        else:
            return_response = get_return_response(500, json.dumps(response))
    except Exception as e:
        print("An exception occurred while trying to write info to Kinesis:", e, " --> Traceback:")
        traceback.print_exc()  # Printing the Stack Trace
        return_response = get_return_response(500, str(e))
    return return_response


def create_json_body_for_kinesis(uuid, device_id, file_name, file_size, file_received_date, data_protocol,
                                 data_pipeline_stage, esn, config_spec_name, requestor_id):

    try:
        values_array = [uuid, device_id, file_name, file_size, file_received_date,
                        data_protocol, data_pipeline_stage, esn, config_spec_name,
                        requestor_id]
        assert len(values_array) == 10, 'Not all required fields are received!!!'
        fields_array = ['uuid', 'device_id', 'file_name', 'file_size', 'file_received_date', 'data_protocol',
                        'data_pipeline_stage', 'esn', 'config_spec_name', 'requestor_id']
        created_dictionary = dict(zip(fields_array, values_array))
        created_dictionary_string = json.dumps(created_dictionary)
        final_json = json.loads(created_dictionary_string)
        write_response = write_into_kinesis(final_json)

        if write_response["status_code"] != 200:

            raise Exception("There was an error while writing into kinesis stream!")

    except Exception as e:

        print("An exception occurred while trying to create json body for Kinesis:", e, " --> Traceback:")
        traceback.print_exc()  # Printing the Stack Trace
