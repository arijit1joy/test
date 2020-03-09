import json
import boto3
import requests


def send_to_pt(post_url, headers, json_body):

    try:
        headers_json = json.loads(headers)

        final_json_body = [json_body]

        print("Posting the JSON body:", final_json_body, "to the PT Cloud through URL:",
              post_url, "with headers:", headers_json)

        pt_response = requests.post(url=post_url, data=json_body, headers=headers)

        print("Get device info response as text:", pt_response.text)

        pt_response_body = pt_response.json()
        pt_response_code = pt_response.status_code

        print("Get device info response code:", pt_response_code)
        print("Get device info response body:", pt_response_body)

    except Exception as e:

        print("ERROR! An exception occurred while posting to PT endpoint:", e)