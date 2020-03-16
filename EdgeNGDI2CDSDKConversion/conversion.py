import json
import cd_sdk_hb as hbsdk
import os
import boto3
import requests
import datetime

# metadata = {}
spn_bucket = os.getenv('spn_parameter_json_object')
spn_bucket_key = os.getenv('spn_parameter_json_object_key')
auth_token_url = os.getenv('auth_token_url')
cd_url = os.getenv('cd_url')
converted_equip_params_var = os.getenv('converted_equip_params')
converted_device_params_var = os.getenv('converted_device_params')
converted_equip_fc_var = os.getenv('converted_equip_fc')
class_arg_map = json.loads(os.getenv('class_arg_map'))
time_stamp_param = os.getenv('time_stamp_param')
active_fault_code_indicator = os.getenv('active_fault_code_indicator')
param_indicator = os.getenv('param_indicator')
notification_version = os.getenv('notification_version')
message_format_version_indicator = os.getenv('message_format_version_indicator')

s3_client = boto3.client('s3')

spn_file_stream = s3_client.get_object(Bucket=spn_bucket, Key=spn_bucket_key)
spn_file = spn_file_stream['Body'].read()
spn_file_json = json.loads(spn_file)


def get_metadata_info(j1939_file):
    j1939_file_val = j1939_file
    print("Removing 'samples' from the j1939 file...")

    try:
        j1939_file_val.pop("samples")

        print("j1939 file with no samples:", j1939_file_val)

        return j1939_file_val

    except Exception as e:

        print("An exception occurred while retrieving metadata:", e)

        return False


def get_snapshot_data(params, time_stamp, address):
    print("Getting snapshot data for the parameter list:", params)

    snapshot_data = []

    print("SPN File as JSON:", spn_file_json)

    try:
        for param in params:
            snapshot_data.append({"Snapshot_DateTimestamp": time_stamp,
                                  "Parameter": [{
                                      "Name": spn_file_json[param],
                                      "Value": params[param],
                                      "Parameter_Source_Address": address}]})

        return snapshot_data

    except Exception as e:

        print("Error! An Exception occurred when getting the snapshot data:", e)

        return {}


def post_cd_message(data):

    PARAMS = {}
    req = requests.get(url=auth_token_url, params=PARAMS)
    auth_token = json.loads(req.text)
    # print('AuthToken  -- >', auth_token['authToken'])
    auth_token_info = auth_token['authToken']

    url = cd_url + auth_token_info

    print('Auth Token ---------------->', auth_token_info)

    sent_date_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    print('Sent_Date_Time  ------------------> ', sent_date_time)

    data["Sent_Date_Time"] = sent_date_time if sent_date_time else data["Occurrence_Date_Time"] \
        if "Occurrence_Date_Time" in data else ''

    data["equipmentId"] = ""  # Temporary solution to EQUIP_ID renaming Issue on CP end - Removing "equipmentId"
    data["customerReference"] = "Bench_Test"  # Temporary solution to Missing "Cummins" Cust_Ref
    data["telematicsPartnerName"] = "Edge"  # Temporary solution to Missing "Cummins" Telematics Partner

    print('File to send to CD   ------------------> ', data)
    print('cd_url   ------------------> ', url)
    print('Type of message:', type(data))

    r = requests.post(url=url, json=data)
    response = r.text
    print('response ------------> ', response)


def handle_hb(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp):
    var_dict = {}
    address = ""

    print("Retrieving parameters for creating HB SDK Class Object")

    print("CD SDK Arguments Map:", class_arg_map)

    try:

        for arg in class_arg_map:

            print("Handling the arg:", arg)

            if class_arg_map[arg] and type(class_arg_map[arg]) == str:

                if arg == message_format_version_indicator:

                    var_dict[class_arg_map[arg]] = notification_version

                if arg in metadata and metadata[arg]:
                    var_dict[class_arg_map[arg]] = metadata[arg]

            if class_arg_map[arg] and type(class_arg_map[arg]) == dict:

                samples = class_arg_map[arg]

                for param in samples:

                    if samples[param] and type(samples[param]) == str and param == time_stamp_param:

                        var_dict[samples[param]] = time_stamp

                    else:

                        if samples[param]:

                            if param == converted_device_params_var:

                                sample_obj = samples[param]

                                for val in sample_obj:
                                    var_dict[sample_obj[val]] = converted_device_params[val] \
                                        if val in converted_device_params else ""

                            elif param == converted_equip_params_var:

                                sample_obj = samples[param][0]

                                address = converted_equip_params["deviceId"] \
                                    if "deviceId" in converted_equip_params else ""

                                for equip_param in sample_obj:

                                    if equip_param == param_indicator:

                                        var_dict[sample_obj[equip_param]] = get_snapshot_data(
                                            converted_equip_params[equip_param], time_stamp, address) \
                                            if equip_param in converted_equip_params else ""

                                    else:

                                        var_dict[sample_obj[equip_param]] = converted_equip_params[equip_param] \
                                            if equip_param in converted_equip_params else ""

                            else:

                                sample_obj = samples[param][0]

                                for fc_param in sample_obj:

                                    final_fc = []

                                    if fc_param in converted_equip_fc:

                                        for fc in converted_equip_fc[fc_param]:
                                            fc["Fault_Source_Address"] = address
                                            fc["SPN"] = fc["spn"]
                                            fc["FMI"] = fc["fmi"]

                                            del fc["spn"]
                                            del fc["fmi"]
                                            del fc["count"]

                                            final_fc.append(fc)

                                    var_dict[sample_obj[fc_param]] = final_fc

        print("CD SDK Class Variable Dict:", var_dict)

        hb_sdk_object = hbsdk.CDHBSDK(var_dict)

        print("Posting Sample to CD...")

        post_cd_message(hb_sdk_object.get_payload())

    except Exception as e:

        print("Error! The following Exception occurred while handling this sample:", e)


def send_sample(sample, metadata, fc_or_hb):
    print("Handling Sample:", sample)

    converted_equip_params = []
    converted_device_params = {}
    converted_equip_fc = []
    time_stamp = ""

    print("converted_equip_params:", converted_equip_params_var)
    print("converted_device_params:", converted_device_params_var)
    print("converted_equip_fc:", converted_equip_fc_var)

    print("Retrieving the params from the Sample")

    if converted_equip_params_var in sample:
        converted_equip_params = sample[converted_equip_params_var][0] if sample[converted_equip_params_var] else []

        print("Found", converted_equip_params_var, ":", converted_equip_params)

    if converted_device_params_var in sample:
        converted_device_params = sample[converted_device_params_var] if sample[converted_device_params_var] else {}

        print("Found", converted_device_params_var, ":", converted_device_params)

    if converted_equip_fc_var in sample:
        converted_equip_fc = sample[converted_equip_fc_var][0] if sample[converted_equip_fc_var] else []

        print("Found", converted_equip_fc_var, ":", converted_equip_fc)

    if time_stamp_param in sample:
        time_stamp = sample[time_stamp_param]

    print("Sample Time Stamp:", time_stamp)
    print("New converted_equip_params:", converted_equip_params)
    print("New converted_device_params:", converted_device_params)
    print("New converted_equip_fc:", converted_equip_fc)

    if fc_or_hb.lower() == "hb":

        print("Handling HB...")
        handle_hb(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp)

    else:

        print("FC processing coming very soon")


def process(bucket, key):
    print("Retrieving the JSON file from the NGDI folder")

    j1939_file_object = s3_client.get_object(Bucket=bucket, Key=key)

    print("Checking if this is HB or FC...")

    file_metadata = j1939_file_object["Metadata"]

    print("File Metadata:", file_metadata)

    fc_or_hb = file_metadata['j1939type'] if "j1939type" else None

    print("FC or HB : ", fc_or_hb)

    if not fc_or_hb:
        print("Error! Cannot determine if this is an FC of an HB file. Check file metadata!")

        return

    j1939_file_stream = j1939_file_object['Body'].read()

    j1939_file = json.loads(j1939_file_stream)

    print("File as JSON:", j1939_file)

    print("Retrieving Metadata from the file:", j1939_file)

    print("Retrieving Samples from the file:", j1939_file)

    samples = j1939_file["samples"] if "samples" in j1939_file else None

    print("File Samples:", samples)

    metadata = get_metadata_info(j1939_file)

    if metadata:

        if samples:

            print("Sending samples for CD processing . . .")

            for sample in samples:
                send_sample(sample, metadata, fc_or_hb)

        else:

            print("Error! There are no samples in this file!")

            return

    else:

        print("Error! Metadata retrieval failed! See logs.")

        return


def lambda_handler(event, context):
    print("Lambda Event:", event)

    print("NGDI JSON Object:", event['Records'][0]['s3']['object']['key'])

    # Retrieve bucket and key details
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']

    print("Bucket:", bucket)
    print("Key:", key)

    process(bucket, key)


'''
Main Method For Local Testing
'''
if __name__ == "__main__":
    event = ""
    context = ""
    process()