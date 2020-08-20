import json
import boto3
import requests
import traceback

secret_name = os.environ['pt_xapi_key']

# Create a Secrets Manager client
session = boto3.session.Session()
sec_client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)


def send_to_pt(post_url, headers, json_body):

    try:
        headers_json = json.loads(headers)
        get_secret_value_response = sec_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            secret = json.loads(secret)
            api_key = secret['x-api-key']
        else:
            print("PT x-api-key not exist in secret manager")
        headers_json['x-api-key'] = api_key
        final_json_body = [json_body]
        print("Posting the JSON body:", final_json_body, "to the PT Cloud through URL:",
              post_url, "with headers:", headers_json)

        pt_response = requests.post(url=post_url, data=json.dumps(final_json_body), headers=headers_json)

        print("Get device info response as text:", pt_response.text)

        pt_response_body = pt_response.json()
        pt_response_code = pt_response.status_code
        print("Get device info response code:", pt_response_code)
        print("Get device info response body:", pt_response_body)

    except Exception as e:
        traceback.print_exc()
        print("ERROR! An exception occurred while posting to PT endpoint:", e)