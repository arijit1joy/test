import json
import pt_poster
import os
import boto3

s3_resource = boto3.resource('s3')
CDPTJ1939PostURL = os.environ["CDPTJ1939PostURL"]
CDPTJ1939Header = os.environ["CDPTJ1939Header"]


def check_endpoint_file_exists(endpoint_bucket, endpoint_file):
    print("Checking if endpoint file exists...")


def send_to_cd(bucket_name, key, json_format, client, j1939_type, endpoint_bucket,
               endpoint_file, use_endpoint_bucket, json_body):

    print("Received CD file for posting!")

    if json_format.lower() == "sdk":
        print("Posting to the NGDI folder for posting to CD Pipeline...")

        try:

            post_to_ngdi_response = client.put_object(Bucket=bucket_name,
                                                      Key=key.replace("ConvertedFiles", "NGDI"),
                                                      Body=json.dumps(json_body).encode(),
                                                      Metadata={'j1939type': j1939_type})

            print("Post CD File to NGDI Folder Response:", post_to_ngdi_response)

        except Exception as e:

            print("ERROR! An Exception occurred while posting the file to the NGDI folder:", e)

    elif json_format.lower() == "ngdi":

        if use_endpoint_bucket.lower() == "y":

            endpoint_file_exists = check_endpoint_file_exists(endpoint_bucket, endpoint_file)

        else:

            pt_poster.send_to_pt(CDPTJ1939PostURL, CDPTJ1939Header, json_body)




