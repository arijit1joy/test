import json
import pt_poster
import os
import boto3
from kinesis_utility import build_metadata_and_write
import traceback

s3_resource = boto3.resource('s3')
CDPTJ1939PostURL = os.environ["CDPTJ1939PostURL"]
CDPTJ1939Header = os.environ["CDPTJ1939Header"]


def check_endpoint_file_exists(endpoint_bucket, endpoint_file):
    print("Checking if endpoint file exists...")


def get_cspec_req_id(sc_number):
    if '-' in sc_number:
        config_spec_name = ''.join(sc_number.split('-')[:-1])
        req_id = sc_number.split('-')[-1]
    else:
        config_spec_name = sc_number
        req_id = None
    return config_spec_name, req_id


def send_to_cd(bucket_name, key, file_size, file_date_time, json_format, client, j1939_type, fc_uuid, endpoint_bucket,
               endpoint_file, use_endpoint_bucket, json_body, config_spec_name, req_id, device_id, esn, hb_uuid):
    print("Received CD file for posting!")

    file_key = key.split('/')[-1]

    if json_format.lower() == "sdk":
        print("Posting to the NGDI folder for posting to CD Pipeline...")

        try:

            if j1939_type.lower() == "fc":

                config_spec_name_fc, req_id_fc = get_cspec_req_id(key.split('_')[3])

                post_to_ngdi_response = client.put_object(Bucket=bucket_name,
                                                          Key=key.replace("ConvertedFiles", "NGDI"),
                                                          Body=json.dumps(json_body).encode(),
                                                          Metadata={'j1939type': j1939_type, 'uuid': fc_uuid})

                build_metadata_and_write(fc_uuid, device_id, file_key, file_size, file_date_time, 'J1939-FC',
                                             'CD_PT_POSTED', esn, config_spec_name_fc, req_id_fc)

            else:

                post_to_ngdi_response = client.put_object(Bucket=bucket_name, Key=key.replace("ConvertedFiles", "NGDI"),
                                  Body=json.dumps(json_body).encode(), Metadata={'j1939type': j1939_type,
                                                                                 'uuid': hb_uuid})

                build_metadata_and_write(hb_uuid, device_id, file_key, file_size, file_date_time, 'J1939-HB',
                                             'CD_PT_POSTED', esn, config_spec_name, req_id)

            print("Post CD File to NGDI Folder Response:", post_to_ngdi_response)

        except Exception as e:

            print("ERROR! An Exception occurred while posting the file to the NGDI folder:", e, " --> Traceback:")
            traceback.print_exc()  # Printing the Stack Trace)

    elif json_format.lower() == "ngdi":

        if use_endpoint_bucket.lower() == "y":

            endpoint_file_exists = check_endpoint_file_exists(endpoint_bucket, endpoint_file)

        else:

            pt_poster.send_to_pt(CDPTJ1939PostURL, CDPTJ1939Header, json_body)
