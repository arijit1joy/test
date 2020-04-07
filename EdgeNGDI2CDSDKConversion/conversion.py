import json
import cd_sdk_hb as hb_sdk
import cd_sdk_fc as fc_sdk
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
inactive_fault_code_indicator = os.getenv('inactive_fault_code_indicator')
param_indicator = os.getenv('param_indicator')
notification_version = os.getenv('notification_version')
message_format_version_indicator = os.getenv('message_format_version_indicator')
spn_indicator = os.getenv('spn_indicator')
fmi_indicator = os.getenv('fmi_indicator')
count_indicator = os.getenv('count_indicator')
active_cd_parameter = os.getenv('active_cd_parameter')

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

        print("Snapshot Data:", snapshot_data)

        return snapshot_data

    except Exception as e:

        print("Error! An Exception occurred when getting the snapshot data:", e)

        return {}


def post_cd_message(data):

    params = {}
    req = requests.get(url=auth_token_url, params=params)
    auth_token = json.loads(req.text)
    # print('AuthToken  -- >', auth_token['authToken'])
    auth_token_info = auth_token['authToken']

    url = cd_url + auth_token_info

    print('Auth Token ---------------->', auth_token_info)

    sent_date_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    print('Sent_Date_Time  ------------------> ', sent_date_time)

    data["Sent_Date_Time"] = sent_date_time if sent_date_time else data["Occurrence_Date_Time"] \
        if "Occurrence_Date_Time" in data else ''

    data["Equipment_ID"] = ""  # Permanent solution to EQUIP_ID renaming Issue on CP end - Removing "equipmentId"
    # data["Customer_Reference"] = "Bench_Test"  # Temporary solution to Missing "Cummins" Cust_Ref
    # data["Telematics_Partner_Name"] = "Edge"  # Temporary solution to Missing "Cummins" Telematics Partner

    print('File to send to CD   ------------------> ', data)
    print('cd_url   ------------------> ', url)
    print('Type of message:', type(data))

    r = requests.post(url=url, json=data)
    response = r.text
    print('response ------------> ', response)


def get_active_faults(fault_list, address):

    print("Getting Active Faults")

    final_fc_list = []

    for fc in fault_list:

        print("Handling FC:", fc)

        fc["Fault_Source_Address"] = address
        fc["SPN"] = fc["spn"]
        fc["FMI"] = fc["fmi"]

        print("Handling Intermediate FC:", fc)

        del fc["spn"]
        del fc["fmi"]

        # if "count" in fc:
        #     del fc["count"]

        print("Handling final FC:", fc)

        final_fc_list.append(fc)

    print("Final FC list:", final_fc_list)

    return final_fc_list


def handle_hb(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp):
    var_dict = {}
    address = ""

    print("Retrieving parameters for creating FC SDK Class Object")

    print("CD SDK Arguments Map:", class_arg_map)

    try:

        for arg in class_arg_map:

            print("Handling the arg:", arg)

            if class_arg_map[arg] and type(class_arg_map[arg]) == str:

                if arg == message_format_version_indicator:

                    var_dict[class_arg_map[arg]] = notification_version

                elif arg in metadata and metadata[arg]:
                    var_dict[class_arg_map[arg]] = metadata[arg]

            if class_arg_map[arg] and type(class_arg_map[arg]) == dict:

                samples = class_arg_map[arg]

                for param in samples:

                    if samples[param] and type(samples[param]) == str and param == time_stamp_param:

                        var_dict[samples[param]] = time_stamp

                    else:

                        if samples[param]:

                            if param == converted_device_params_var:

                                print("Handling convertedDeviceParameters")

                                sample_obj = samples[param]

                                for val in sample_obj:
                                    var_dict[sample_obj[val]] = converted_device_params[val] \
                                        if val in converted_device_params else ""

                            elif param == converted_equip_params_var:

                                print("Handling convertedEquipmentParameters")

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

                                print("Handling convertedEquipmentFaultCodes")

                                sample_obj = samples[param][0]

                                for fc_param in sample_obj:

                                    final_fc = []

                                    if fc_param in converted_equip_fc:

                                        final_fc = get_active_faults(converted_equip_fc[fc_param], address)

                                    var_dict[sample_obj[fc_param]] = final_fc

        print("HB CD SDK Class Variable Dict:", var_dict)

        hb_sdk_object = hb_sdk.CDHBSDK(var_dict)

        print("Posting Sample to CD...")

        post_cd_message(hb_sdk_object.get_payload())

    except Exception as e:

        print("Error! The following Exception occurred while handling this sample:", e)


def handle_fc(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp):

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

                elif arg in metadata and metadata[arg]:
                    var_dict[class_arg_map[arg]] = metadata[arg]

            if class_arg_map[arg] and type(class_arg_map[arg]) == dict:

                samples = class_arg_map[arg]

                for param in samples:

                    if samples[param] and type(samples[param]) == str and param == time_stamp_param:

                        var_dict[samples[param]] = time_stamp

                    else:

                        if samples[param]:

                            if param == converted_device_params_var:

                                print("Handling convertedDeviceParameters")

                                sample_obj = samples[param]

                                for val in sample_obj:
                                    var_dict[sample_obj[val]] = converted_device_params[val] \
                                        if val in converted_device_params else ""

                            elif param == converted_equip_params_var:

                                print("Handling convertedEquipmentParameters")

                                sample_obj = samples[param][0]

                                address = converted_equip_params["deviceId"] \
                                    if "deviceId" in converted_equip_params else ""

                                for equip_param in sample_obj:

                                    if equip_param == param_indicator:

                                        var_dict[sample_obj[equip_param]] = get_snapshot_data(
                                            converted_equip_params[equip_param].copy(), time_stamp, address) \
                                            if equip_param in converted_equip_params else ""

                                    else:

                                        var_dict[sample_obj[equip_param]] = converted_equip_params[equip_param] \
                                            if equip_param in converted_equip_params else ""

                            else:

                                print("Handling convertedEquipmentFaultCodes")

                                sample_obj = samples[param][0]

                                for fc_param in sample_obj:

                                    if fc_param in converted_equip_fc and fc_param == active_fault_code_indicator:

                                        all_fcs = converted_equip_fc[fc_param].copy()

                                        fc_index = 0

                                        final_fc = get_active_faults(all_fcs, address)

                                        for fc in all_fcs:

                                            create_fc_class(fc, final_fc, fc_index, sample_obj[fc_param], var_dict, 1)

                                            fc_index = fc_index + 1

                                    else:
                                        if fc_param in converted_equip_fc and fc_param == inactive_fault_code_indicator:

                                            all_inactive_fcs = converted_equip_fc[fc_param].copy()

                                            all_fcs = converted_equip_fc[active_fault_code_indicator].copy() if \
                                                active_fault_code_indicator in converted_equip_fc else []

                                            fc_index = 0

                                            final_fc = get_active_faults(all_fcs, address)

                                            for fc in all_inactive_fcs:

                                                create_fc_class(fc, final_fc, fc_index, sample_obj[fc_param],
                                                                var_dict, 0)

                                                fc_index = fc_index + 1

    except Exception as e:

        print("Error! The following Exception occurred while handling this sample:", e)


def create_fc_class(fc, active_fcodes, fc_index, active_fc_param, var_dict, active_or_inactive):

    print("Current FC Index:", fc_index)

    active_fcs = active_fcodes.copy()

    active_fcs.pop(fc_index)

    print("Old active faults:", active_fcodes)
    print("New active faults:", active_fcs)

    var_dict[active_fc_param] = active_fcs
    var_dict[active_cd_parameter] = active_or_inactive

    var_dict[spn_indicator.lower()] = fc["SPN"]
    var_dict[fmi_indicator.lower()] = fc["FMI"]
    var_dict[count_indicator.lower()] = fc["count"]

    print("FC CD SDK Class Variable Dict:", var_dict)

    fc_sdk_object = fc_sdk.CDFCSDK(var_dict)

    print("Posting Sample to CD...")

    post_cd_message(fc_sdk_object.get_payload())


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
        
        print("Handling FC...")
        handle_fc(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp)


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
    print("Lambda Context:", context)

    print("NGDI JSON Object:", event['Records'][0]['s3']['object']['key'])

    # Retrieve bucket and key details
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']

    print("Bucket:", bucket)
    print("Key:", key)

    key = key.replace("%3A", ":")

    print("New FileKey:", key)

    process(bucket, key)


'''
Main Method For Local Testing
'''
if __name__ == "__main__":
    lambda_event = ""
    # context = ""
