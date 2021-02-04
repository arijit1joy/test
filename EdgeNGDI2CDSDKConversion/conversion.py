import datetime
import json
import os

import boto3
import edge_core as edge
import requests
from metadata_utility import build_metadata_and_write
from metadata_utility import write_health_parameter_to_database
from obfuscate_gps_utility import deobfuscate_gps_coordinates

import bdd_utility
import edge_logger as logging
from cd_sdk_conversion.cd_sdk import map_ngdi_sample_to_cd_payload
from cd_sdk_conversion.cd_snapshot_sdk import get_snapshot_data
from system_variables import InternalResponse, CDSDK

logger = logging.logging_framework("EdgeNGDI2CDSDKConversion.Conversion")

import sys

sys.path.insert(1, './lib')
from pypika import Query, Table, Order, functions as fn

'''Getting the Values from SSM Parameter Store
'''

ssm = boto3.client('ssm')  # noqa


def set_parameters():
    ssm_params = {
        "Names": ["EDGECommonAPI"],
        "WithDecryption": False
    }
    return ssm_params


params = set_parameters()
name = params['Names']
response = ssm.get_parameters(Names=name, WithDecryption=False)
api_url = response['Parameters'][0]['Value']

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
    logger.info(f"Removing 'samples' from the j1939 file...")
    try:
        j1939_file_val.pop("samples")
        logger.info(f"j1939 file with no samples: {j1939_file_val}")
        return j1939_file_val

    except Exception as e:
        logger.error(f"An exception occurred while retrieving metadata:{e}")
        return False


def post_cd_message(data):
    logger.info(f"Retrieving the TSP name to get the Auth Token . . .")
    tsp_name = data["Telematics_Partner_Name"]
    logger.info(f"TSP From File ----------------> {tsp_name}")
    auth_url = auth_token_url.replace("{TSP-Name}", tsp_name)
    req = requests.get(url=auth_url)
    auth_token = json.loads(req.text)
    auth_token_info = auth_token['authToken']
    url = cd_url + auth_token_info
    sent_date_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + "Z"
    logger.info(f'Sent_Date_Time  ------------------> {sent_date_time}')
    if sent_date_time:
        data["Sent_Date_Time"] = sent_date_time
    else:
        data["Sent_Date_Time"] = data["Occurrence_Date_Time"] if "Occurrence_Date_Time" in data else ''

    if "VIN" in data and not data["VIN"]:
        logger.info(f"Vin is not in file. Setting the value of the VIN to 'None'")
        data["VIN"] = None
        logger.info(f"New VIN {data['VIN']}")

    # TODO Temporary address to the Equipment_ID retrieval issue. Renaming to EDGE_<ESN> if not there already . . .
    if "Equipment_ID" not in data or not data["Equipment_ID"]:
        logger.info(f"Equipment ID is not in the file. Creating it in the format EDGE_<ESN> . . .")
        data["Equipment_ID"] = "EDGE_" + data["Engine_Serial_Number"]  # Setting the Equipment ID to EDGE_<ESN>
        logger.info(f"New Equipment ID: {data['Equipment_ID']}")

    is_bdd = False
    if data["Telematics_Box_ID"] in InternalResponse.J1939BDDValidDevices.value.split(","):
        logger.info(f"This is a BDD execution!")
        is_bdd = True

    # de-obfuscate GPS co-ordinates
    if "Latitude" in data and "Longitude" in data:
        latitude = data["Latitude"]
        longitude = data["Longitude"]
        data["Latitude"], data["Longitude"] = deobfuscate_gps_coordinates(latitude, longitude)

    logger.info(f'File to send to CD   ------------------> {data}')

    '''
        ***************** The below is important for J1939 BDD functionality. Please do not modify! ********************
    '''
    if is_bdd:
        bdd_utility.update_bdd_parameter("<---**--->".join([json.dumps(data), data["Telematics_Partner_Message_ID"],
                                                            sent_date_time, data["VIN"], data["Equipment_ID"]]),
                                         param_name=CDSDK.CDSDKBDDVariables.value)
    '''
        ***************** The above is important for J1939 BDD functionality. Please do not modify! ********************
    '''

    logger.info(f'cd_url   ------------------>  {url}')
    logger.info(f'Type of message: {type(data)}')

    # We are not sending payload to CD for Digital Cockpit Device
    if not data["Telematics_Box_ID"] == '192000000000101':
        r = requests.post(url=url, json=data)
        cp_response = r.text
        logger.info(f'response ------------>  {cp_response}')

        '''
            ***************** The below is important for J1939 BDD functionality. Please do not modify! ****************
        '''
        if is_bdd and cp_response == InternalResponse.J1939CPPostSuccess.value:
            bdd_utility.update_bdd_parameter(InternalResponse.J1939CPPostSuccess.value)
        '''
            ***************** The above is important for J1939 BDD functionality. Please do not modify! ****************
        '''


def get_active_faults(fault_list, address):
    logger.info(f"Getting Active Faults")
    final_fc_list = []
    for fc in fault_list:
        logger.info(f"Handling FC: {fc}")
        fc["Fault_Source_Address"] = address
        fc["SPN"] = fc["spn"]
        fc["FMI"] = fc["fmi"]
        logger.info(f"Handling Intermediate FC: {fc}")
        del fc["spn"]
        del fc["fmi"]
        logger.info(f"Handling final FC: {fc}")
        final_fc_list.append(fc)
    logger.info(f"Final FC list: {final_fc_list}")
    return final_fc_list


def handle_hb(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp):
    var_dict = {}
    address = ""
    logger.info(f"Retrieving parameters for creating FC SDK Class Object")
    logger.info(f"CD SDK Arguments Map: {class_arg_map}")
    try:
        for arg in class_arg_map:
            logger.info(f"Handling the arg: {arg}")
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
                                logger.info(f"Handling convertedDeviceParameters")
                                sample_obj = samples[param]
                                for val in sample_obj:
                                    var_dict[sample_obj[val]] = converted_device_params[val] \
                                        if val in converted_device_params else ""
                            elif param == converted_equip_params_var:
                                logger.info(f"Handling convertedEquipmentParameters")
                                sample_obj = samples[param][0]
                                address = converted_equip_params["deviceId"] \
                                    if "deviceId" in converted_equip_params else ""
                                for equip_param in sample_obj:
                                    if equip_param == param_indicator:
                                        var_dict[sample_obj[equip_param]] = get_snapshot_data(
                                            converted_equip_params[equip_param], time_stamp, address, spn_file_json) \
                                            if equip_param in converted_equip_params else ""
                                    else:
                                        var_dict[sample_obj[equip_param]] = converted_equip_params[equip_param] \
                                            if equip_param in converted_equip_params else ""
                            else:
                                logger.info(f"Handling convertedEquipmentFaultCodes")
                                sample_obj = samples[param][0]
                                for fc_param in sample_obj:
                                    final_fc = []
                                    if fc_param in converted_equip_fc:
                                        final_fc = get_active_faults(converted_equip_fc[fc_param], address)
                                    var_dict[sample_obj[fc_param]] = final_fc
        logger.info(f"HB CD SDK Class Variable Dict: {var_dict}")
        hb_sdk_object = map_ngdi_sample_to_cd_payload(var_dict, hb=True)
        logger.info(f"Posting Sample to CD...")
        post_cd_message(hb_sdk_object)
    except Exception as e:
        logger.error(f"Error! The following Exception occurred while handling this sample:{e}")


def handle_fc(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp):
    var_dict = {}
    address = ""
    found_fcs = False
    logger.info(f"Retrieving parameters for creating HB SDK Class Object")
    logger.info(f"CD SDK Arguments Map:  {class_arg_map}")
    try:
        for arg in class_arg_map:
            logger.info(f"Handling the arg: {arg}")
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
                                logger.info(f"Handling convertedDeviceParameters")
                                sample_obj = samples[param]
                                for val in sample_obj:
                                    var_dict[sample_obj[val]] = converted_device_params[val] \
                                        if val in converted_device_params else ""
                            elif param == converted_equip_params_var:
                                logger.info(f"Handling convertedEquipmentParameters")
                                sample_obj = samples[param][0]
                                address = converted_equip_params["deviceId"] \
                                    if "deviceId" in converted_equip_params else ""
                                for equip_param in sample_obj:
                                    if equip_param == param_indicator:
                                        var_dict[sample_obj[equip_param]] = get_snapshot_data(
                                            converted_equip_params[equip_param].copy(), time_stamp, address,
                                            spn_file_json) \
                                            if equip_param in converted_equip_params else ""
                                    else:
                                        var_dict[sample_obj[equip_param]] = converted_equip_params[equip_param] \
                                            if equip_param in converted_equip_params else ""
                            else:
                                logger.info(f"Handling convertedEquipmentFaultCodes")
                                sample_obj = samples[param][0]
                                for fc_param in sample_obj:
                                    if fc_param in converted_equip_fc and fc_param == active_fault_code_indicator:
                                        all_active_fcs = converted_equip_fc[fc_param].copy()
                                        if not all_active_fcs:
                                            continue
                                        logger.info(f"These are active Fault Codes")
                                        found_fcs = True  # Indicating that we found Fault Codes in this file.
                                        fc_index = 0
                                        final_fc = get_active_faults(all_active_fcs, address)
                                        logger.info(f"Total Number of fcs: {len(all_active_fcs)}")
                                        for fc in all_active_fcs:
                                            create_fc_class(fc, final_fc, fc_index, sample_obj[fc_param], var_dict, 1)
                                            fc_index = fc_index + 1
                                    elif fc_param in converted_equip_fc and fc_param == inactive_fault_code_indicator:
                                        all_inactive_fcs = converted_equip_fc[fc_param].copy()
                                        if not all_inactive_fcs:
                                            continue
                                        logger.info(f"These are inactive Fault Codes")
                                        found_fcs = True  # Indicating that we found Fault Codes in this file.
                                        all_active_fcs = converted_equip_fc[active_fault_code_indicator].copy() if \
                                            active_fault_code_indicator in converted_equip_fc else []
                                        fc_index = 0
                                        inactive_final_fc = get_active_faults(all_inactive_fcs, address)
                                        active_final_fc = get_active_faults(all_active_fcs, address)
                                        logger.info(f"Total Number of fcs: {len(all_inactive_fcs)}")
                                        for fc in all_inactive_fcs:
                                            create_fc_class(fc, inactive_final_fc, fc_index, sample_obj[fc_param],
                                                            var_dict, 0, active_final_fc)
                                            fc_index = fc_index + 1
                                    else:
                                        # TODO Handle Pending Fault Codes.
                                        logger.info(
                                            f"There are either no, fc_param in this file -- We are not handling "
                                            f"pending FCs for now.")
        if found_fcs:
            logger.info(
                f"We have already processed this sample since it had fault codes. Continuing to the next sample . . .")
        else:
            logger.info(f"This sample had no Fault Code information, checking if this is the Single Sample . . .")
            logger.info(f"Variable Dict: {var_dict}")
            if "Telematics_Partner_Message_ID".lower() in var_dict:
                logger.info(
                    f'Found Message ID: {var_dict["Telematics_Partner_Message_ID".lower()]} in this sample! This is '
                    f'the Single Sample. Proceeding to the next sample . . .')
            else:
                logger.error(
                    f"There was an Error in this FC sample. It is not the Single Sample and it does not have FC info!")
    except Exception as e:
        logger.error(f"Error! The following Exception occurred while handling this sample:{e}")


def create_fc_class(fc, f_codes, fc_index, fc_param, var_dict,
                    active_or_inactive, active_fault_array=None):
    logger.info(f"Current FC Index: {fc_index}")
    fcs = f_codes.copy()
    fcs.pop(fc_index)
    logger.info(f"Old active faults: {f_codes}")
    logger.info(f"New active faults: {fcs}")
    variable_dict = var_dict.copy()
    variable_dict[fc_param] = fcs if not active_fault_array else active_fault_array
    variable_dict[active_cd_parameter] = active_or_inactive
    variable_dict[spn_indicator.lower()] = fc["SPN"]
    variable_dict[fmi_indicator.lower()] = fc["FMI"]
    variable_dict[count_indicator.lower()] = fc["count"]
    logger.info(f"FC CD SDK Class Variable Dict: {variable_dict}")
    fc_sdk_object = map_ngdi_sample_to_cd_payload(variable_dict, fc=True)
    logger.info(f"Posting Sample to CD...")
    post_cd_message(fc_sdk_object)


def send_sample(sample, metadata, fc_or_hb):
    logger.info(f"Handling Sample: {sample}")
    converted_equip_params = []
    converted_device_params = {}
    converted_equip_fc = []
    time_stamp = ""
    logger.info(f"converted_equip_params: {converted_equip_params_var}")
    logger.info(f"converted_device_params: {converted_device_params_var}")
    logger.info(f"converted_equip_fc: {converted_equip_fc_var}")
    logger.info(f"Retrieving the params from the Sample")
    if converted_equip_params_var in sample:
        converted_equip_params = sample[converted_equip_params_var][0] if sample[converted_equip_params_var] else []
        logger.info(f"Found  {converted_equip_params_var} : {converted_equip_params}")
    if converted_device_params_var in sample:
        converted_device_params = sample[converted_device_params_var] if sample[converted_device_params_var] else {}
        logger.info(f"Found {converted_device_params_var} : {converted_device_params}")
    if converted_equip_fc_var in sample:
        converted_equip_fc = sample[converted_equip_fc_var][0] if sample[converted_equip_fc_var] else []
        logger.info(f"Found {converted_equip_fc_var} : {converted_equip_fc}")
    if time_stamp_param in sample:
        time_stamp = sample[time_stamp_param]
    logger.info(f"Sample Time Stamp: {time_stamp}")
    logger.info(f"New converted_equip_params: {converted_equip_params}")
    logger.info(f"New converted_device_params: {converted_device_params}")
    logger.info(f"New converted_equip_fc , {converted_equip_fc}")
    if fc_or_hb.lower() == "hb":
        store_health_parameters_into_redshift(converted_device_params, time_stamp, metadata)
        logger.info(f"Handling HB...")
        handle_hb(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp)
    else:
        logger.info(f"Handling FC...")
        handle_fc(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp)


def process(bucket, key, file_size):
    logger.info(f"Retrieving the JSON file from the NGDI folder")
    j1939_file_object = s3_client.get_object(Bucket=bucket, Key=key)
    logger.info(f"Checking if this is HB or FC...")
    file_metadata = j1939_file_object["Metadata"]
    logger.info(f"File Metadata: {file_metadata}")
    fc_or_hb = file_metadata['j1939type'] if "j1939type" in file_metadata else None
    uuid = file_metadata['uuid']
    file_date_time = str(j1939_file_object['LastModified'])[:19]
    file_name = key.split('/')[-1]
    device_id = file_name.split('_')[1]
    logger.info(f"FC or HB  {fc_or_hb}")
    if not fc_or_hb:
        logger.error(f"Error! Cannot determine if this is an FC of an HB file. Check file metadata!")
        return
    j1939_file_stream = j1939_file_object['Body'].read()
    j1939_file = json.loads(j1939_file_stream)
    logger.info(f"File as JSON: {j1939_file}")
    if fc_or_hb.lower() == 'hb':
        esn = j1939_file['componentSerialNumber']
        config_spec_name = j1939_file['dataSamplingConfigId']
        data_protocol = 'J1939_HB'
    else:
        esn = key.split('_')[2]
        config_spec_name = key.split('_')[3]
        data_protocol = 'J1939_FC'
    updated_file_name = '_'.join(key.split('/')[-1].split('_')[0:3]) + "%"
    edge_data_consumption_vw = Table('da_edge_olympus.edge_data_consumption_vw')
    query = Query.from_(edge_data_consumption_vw).select(
        fn.Cast(fn.Substring(edge_data_consumption_vw.request_id, 4, fn.Length(edge_data_consumption_vw.request_id)),
                'Integer', alias='request_id_int'), edge_data_consumption_vw.request_id,
        edge_data_consumption_vw.consumption_per_request).where(
        edge_data_consumption_vw.data_config_filename.like(updated_file_name)).where(
        edge_data_consumption_vw.data_type == data_protocol).orderby(edge_data_consumption_vw.request_id_int,
                                                                     order=Order.desc
                                                                     ).limit(1)
    logger.info(f"{query.get_sql(quote_char=None)}")
    try:
        get_response = edge.api_request(api_url, "get", query.get_sql(quote_char=None))
        logger.info(f"Response: {get_response}")
    except Exception as exception:
        return edge.server_error(str(exception))
    request_id = get_response[0]['request_id'] if get_response and "request_id" in get_response[0] else None
    consumption_per_request = get_response[0]['consumption_per_request'] if get_response and get_response[0][
        'consumption_per_request'] else None
    if consumption_per_request == 'null':
        consumption_per_request = None

    build_metadata_and_write(uuid, device_id, file_name, file_size, file_date_time, data_protocol,
                             'FILE_SENT', esn, config_spec_name, request_id, consumption_per_request,
                             os.environ["edgeCommonAPIURL"])
    logger.info(f"Retrieving Metadata from the file: {j1939_file}")
    logger.info(f"Retrieving Samples from the file: {j1939_file}")
    samples = j1939_file["samples"] if "samples" in j1939_file else None
    logger.info(f"File Samples: {samples}")
    metadata = get_metadata_info(j1939_file)
    if metadata:
        if samples:
            logger.info(f"Sending samples for CD processing . . .")
            for sample in samples:
                send_sample(sample, metadata, fc_or_hb)
        else:
            logger.error(f"Error! There are no samples in this file!")
    else:
        logger.error(f"Error! Metadata retrieval failed! See logs.")


def lambda_handler(event, context):
    logger.info(f"NGDI JSON Object: {event['Records'][0]['s3']['object']['key']}")
    # Retrieve bucket and key details
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    file_size = event['Records'][0]['s3']['object']['size']
    logger.info(f"Bucket: {bucket}")
    logger.info(f"Key: {key}")
    key = key.replace("%3A", ":")
    logger.info(f"New FileKey: {key}")
    process(bucket, key, file_size)


'''
Function to get Health Parameter and store into Redshift Table
'''


def store_health_parameters_into_redshift(converted_device_params, time_stamp, j1939_file_val):
    logger.info(f"Starting Kinesis Process... ")
    if 'messageID' in converted_device_params:
        message_id = converted_device_params['messageID']
        cpu_temperature = converted_device_params[
            'CPU_temperature'] if 'CPU_temperature' in converted_device_params else None
        pmic_temperature = converted_device_params[
            'PMIC_temperature'] if 'PMIC_temperature' in converted_device_params else None
        latitude = converted_device_params['Latitude'] if 'Latitude' in converted_device_params else None
        longitude = converted_device_params['Longitude'] if 'Longitude' in converted_device_params else None
        altitude = converted_device_params['Altitude'] if 'Altitude' in converted_device_params else None
        pdop = converted_device_params['PDOP'] if 'PDOP' in converted_device_params else None
        satellites_used = converted_device_params[
            'Satellites_Used'] if 'Satellites_Used' in converted_device_params else None
        lte_rssi = converted_device_params['LTE_RSSI'] if 'LTE_RSSI' in converted_device_params else None
        lte_rscp = converted_device_params['LTE_RSCP'] if 'LTE_RSCP' in converted_device_params else None
        lte_rsrq = converted_device_params['LTE_RSRQ'] if 'LTE_RSRQ' in converted_device_params else None
        lte_rsrp = converted_device_params['LTE_RSRP'] if 'LTE_RSRP' in converted_device_params else None
        cpu_usage_level = converted_device_params[
            'CPU_Usage_Level'] if 'CPU_Usage_Level' in converted_device_params else None
        ram_usage_level = converted_device_params[
            'RAM_Usage_Level'] if 'RAM_Usage_Level' in converted_device_params else None
        snr_per_satellite = converted_device_params[
            'SNR_per_Satellite'] if 'SNR_per_Satellite' in converted_device_params else None
        device_id = j1939_file_val['telematicsDeviceId']
        esn = j1939_file_val['componentSerialNumber']
        convert_timestamp = datetime.datetime.strptime(time_stamp, '%Y-%m-%dT%H:%M:%S.%fZ')
        new_timestamp = datetime.datetime.strftime(convert_timestamp, '%Y-%m-%d %H:%M:%S')
        return write_health_parameter_to_database(message_id, cpu_temperature, pmic_temperature, latitude, longitude,
                                                  altitude, pdop, satellites_used, lte_rssi, lte_rscp, lte_rsrq,
                                                  lte_rsrp, cpu_usage_level, ram_usage_level, snr_per_satellite,
                                                  new_timestamp, device_id, esn, os.environ["edgeCommonAPIURL"])
    else:
        logger.info(f"There is no Converted Device Parameter")
