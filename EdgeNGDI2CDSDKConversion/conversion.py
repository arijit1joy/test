import json
import cd_sdk_hb as hb_sdk
import cd_sdk_fc as fc_sdk
import os
import boto3
import requests
import datetime
from kinesis_utility import create_json_body_for_kinesis

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

    print("Retrieving the TSP name to get the Auth Token . . .")

    tsp_name = data["Telematics_Partner_Name"]

    print("TSP From File ---------------->", tsp_name)

    print("Old AuthToken URL:", auth_token_url)

    auth_url = auth_token_url.replace("{TSP-Name}", tsp_name)

    print("New AuthToken URL:", auth_url)

    req = requests.get(url=auth_url, params=params)
    auth_token = json.loads(req.text)
    # print('AuthToken  -- >', auth_token['authToken'])
    auth_token_info = auth_token['authToken']

    url = cd_url + auth_token_info

    print('Auth Token ---------------->', auth_token_info)

    sent_date_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    print('Sent_Date_Time  ------------------> ', sent_date_time)

    data["Sent_Date_Time"] = sent_date_time if sent_date_time else data["Occurrence_Date_Time"] \
        if "Occurrence_Date_Time" in data else ''

    # data["Equipment_ID"] = ""  # Permanent solution to EQUIP_ID renaming Issue on CP end - Removing "equipmentId"

    if "VIN" in data and not data["VIN"]:
        print("Vin is not in file. Setting the value of the VIN to 'None'")

        data["VIN"] = None

        print("New VIN:", data["VIN"])

    # TODO Temporary address to the Equipment_ID retrieval issue. Renaming to EDGE_<ESN> if not there already . . .

    if "Equipment_ID" not in data or not data["Equipment_ID"]:
        print("Equipment ID is not in the file. Creating it in the format EDGE_<ESN> . . .")

        data["Equipment_ID"] = "EDGE_" + data["Engine_Serial_Number"]  # Setting the Equipment ID to EDGE_<ESN>

        print("New Equipment ID:", data["Equipment_ID"])

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
    found_fcs = False

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

                                        all_active_fcs = converted_equip_fc[fc_param].copy()

                                        if not all_active_fcs:

                                            continue

                                        print("These are active Fault Codes")

                                        found_fcs = True  # Indicating that we found Fault Codes in this file.

                                        fc_index = 0

                                        final_fc = get_active_faults(all_active_fcs, address)

                                        print("Total Number of fcs:", len(all_active_fcs))

                                        for fc in all_active_fcs:
                                            create_fc_class(fc, final_fc, fc_index, sample_obj[fc_param], var_dict, 1)

                                            fc_index = fc_index + 1

                                    elif fc_param in converted_equip_fc and fc_param == inactive_fault_code_indicator:

                                        all_inactive_fcs = converted_equip_fc[fc_param].copy()

                                        if not all_inactive_fcs:
                                            
                                            continue

                                        print("These are inactive Fault Codes")

                                        found_fcs = True  # Indicating that we found Fault Codes in this file.

                                        all_active_fcs = converted_equip_fc[active_fault_code_indicator].copy() if \
                                            active_fault_code_indicator in converted_equip_fc else []

                                        fc_index = 0

                                        inactive_final_fc = get_active_faults(all_inactive_fcs, address)
                                        active_final_fc = get_active_faults(all_active_fcs, address)

                                        print("Total Number of fcs:", len(all_inactive_fcs))

                                        for fc in all_inactive_fcs:
                                            create_fc_class(fc, inactive_final_fc, fc_index, sample_obj[fc_param],
                                                            var_dict, 0, active_final_fc)

                                            fc_index = fc_index + 1

                                    else:

                                        # TODO Handle Pending Fault Codes.
                                        print("There are either no", fc_param,
                                              "in this file -- We are not handling pending FCs for now.")

        if found_fcs:

            print("We have already processed this sample since it had fault codes. Continuing to the next sample . . .")

        else:

            print("This sample had no Fault Code information, checking if this is the Single Sample . . .")

            print("Variable Dict:", var_dict)

            if "Telematics_Partner_Message_ID".lower() in var_dict:

                print("Found Message ID:", var_dict["Telematics_Partner_Message_ID".lower()],
                      "in this sample! This is the Single Sample. Proceeding to the next sample . . .")

            else:

                print("There was an Error in this FC sample. It is not the Single Sample and it does not have FC info!")

    except Exception as e:

        print("Error! The following Exception occurred while handling this sample:", e)


def create_fc_class(fc, f_codes, fc_index, fc_param, var_dict,
                    active_or_inactive, active_fault_array=None):
    print("Current FC Index:", fc_index)

    fcs = f_codes.copy()

    fcs.pop(fc_index)

    print("Old active faults:", f_codes)
    print("New active faults:", fcs)

    variable_dict = var_dict.copy()

    variable_dict[fc_param] = fcs if not active_fault_array else active_fault_array
    variable_dict[active_cd_parameter] = active_or_inactive

    variable_dict[spn_indicator.lower()] = fc["SPN"]
    variable_dict[fmi_indicator.lower()] = fc["FMI"]
    variable_dict[count_indicator.lower()] = fc["count"]

    print("FC CD SDK Class Variable Dict:", variable_dict)

    fc_sdk_object = fc_sdk.CDFCSDK(variable_dict)

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


def get_cspec_req_id(sc_number):
    if '-' in sc_number:
        config_spec_name = ''.join(sc_number.split('-')[:-1])
        req_id = sc_number.split('-')[-1]
    else:
        config_spec_name = sc_number
        req_id = None
    return config_spec_name, req_id


def process(bucket, key, file_size):
    print("Retrieving the JSON file from the NGDI folder")

    j1939_file_object = s3_client.get_object(Bucket=bucket, Key=key)

    print("Checking if this is HB or FC...")

    file_metadata = j1939_file_object["Metadata"]

    print("File Metadata:", file_metadata)

    fc_or_hb = file_metadata['j1939type'] if "j1939type" else None

    uuid = file_metadata['uuid']

    file_date_time = str(j1939_file_object['LastModified'])[:19]

    device_id = key.split('_')[1]

    print("FC or HB : ", fc_or_hb)

    if not fc_or_hb:
        print("Error! Cannot determine if this is an FC of an HB file. Check file metadata!")

        return

    j1939_file_stream = j1939_file_object['Body'].read()

    j1939_file = json.loads(j1939_file_stream)

    print("File as JSON:", j1939_file)

    if fc_or_hb.lower() == 'hb':
        esn = j1939_file['componentSerialNumber']
        config_spec_name, req_id = get_cspec_req_id(j1939_file['dataSamplingConfigId'])
        data_protocol = 'J1939-HB'
    else:
        esn = key.split('_')[2]
        config_spec_name, req_id = get_cspec_req_id(key.split('_')[3])
        data_protocol = 'J1939-FC'

    create_json_body_for_kinesis(uuid, device_id, key, file_size, file_date_time, data_protocol,
                                 'FILE_SENT', esn, config_spec_name, req_id)

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
    file_size = event['Records'][0]['s3']['object']['size']

    print("Bucket:", bucket)
    print("Key:", key)

    key = key.replace("%3A", ":")

    print("New FileKey:", key)

    process(bucket, key, file_size)


'''
Main Method For Local Testing
'''
if __name__ == "__main__":
    lambda_event = ""
    # context = ""
