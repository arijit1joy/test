import os
import sys
import json
import boto3
import datetime
import requests
import utility as util
import edge_core as edge
from lambda_cache import ssm
from multiprocessing import Process
from sqs_utility import sqs_send_message
from obfuscate_gps_utility import handle_gps_coordinates
from metadata_utility import write_health_parameter_to_database
from cd_sdk_conversion.cd_snapshot_sdk import get_snapshot_data
from cd_sdk_conversion.cd_sdk import map_ngdi_sample_to_cd_payload

sys.path.insert(1, './lib')
from pypika import Query, Table, Order, functions as fn

LOGGER = util.get_logger(__name__)

'''Getting the Values from SSM Parameter Store
'''


def set_parameters():
    ssm_params = {
        "Names": ["EDGECommonAPI"],
        "WithDecryption": False
    }
    return ssm_params


params = set_parameters()
name = params['Names']

edgeCommonAPIURL = os.environ['edgeCommonAPIURL']
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


def delete_message_from_sqs_queue(receipt_handle):
    queue_url = os.environ["QueueUrl"]
    sqs_client = boto3.client('sqs')  # noqa
    sqs_message_deletion_response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    LOGGER.debug(f"SQS message deletion response: '{sqs_message_deletion_response}'. . .")
    return sqs_message_deletion_response


def get_metadata_info(j1939_file):
    j1939_file_val = j1939_file
    try:
        j1939_file_val.pop("samples")
        return j1939_file_val

    except Exception as e:
        LOGGER.error(f"An exception occurred while retrieving metadata:{e}")
        return False


def post_cd_message(data):
    tsp_name = data["Telematics_Partner_Name"]
    LOGGER.debug(f"TSP From File: {tsp_name}")
    auth_url = auth_token_url.replace("{TSP-Name}", tsp_name)
    req = requests.get(url=auth_url)
    auth_token = json.loads(req.text)
    auth_token_info = auth_token['authToken']
    url = cd_url + auth_token_info
    sent_date_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + "Z"
    if sent_date_time:
        data["Sent_Date_Time"] = sent_date_time
    else:
        data["Sent_Date_Time"] = data["Occurrence_Date_Time"] if "Occurrence_Date_Time" in data else ''

    if "VIN" in data and not data["VIN"]:
        LOGGER.info(f"Vin is not in file. Setting the value of the VIN to 'None'")
        data["VIN"] = ""
        LOGGER.debug(f"New VIN {data['VIN']}")

    # TODO Temporary address to the Equipment_ID retrieval issue. Renaming to EDGE_<ESN> if not there already . . .
    if "Equipment_ID" not in data or not data["Equipment_ID"]:
        LOGGER.info(f"Equipment ID is not in the file. Creating it in the format EDGE_<ESN> . . .")
        data["Equipment_ID"] = "EDGE_" + data["Engine_Serial_Number"]  # Setting the Equipment ID to EDGE_<ESN>
        LOGGER.debug(f"New Equipment ID: {data['Equipment_ID']}")

    LOGGER.debug(f'File to send to CD: {data}')

    # We are not sending payload to CD for Digital Cockpit Device
    if not data["Telematics_Box_ID"] == '192000000000101':
        r = requests.post(url=url, json=data)
        cp_response = r.text
        LOGGER.info(f'CD Response: {cp_response}')


def get_active_faults(fault_list, address):
    LOGGER.info(f"Getting Active Faults")
    final_fc_list = []
    for fc in fault_list:
        fc["Fault_Source_Address"] = address
        fc["SPN"] = fc["spn"]
        fc["FMI"] = fc["fmi"]
        del fc["spn"]
        del fc["fmi"]
        final_fc_list.append(fc)
    LOGGER.debug(f"Final FC list: {final_fc_list}")
    return final_fc_list


def handle_hb(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp):
    var_dict = {}
    address = ""
    LOGGER.info(f"Retrieving parameters for creating HB SDK Class Object")
    try:
        for arg in class_arg_map:
            LOGGER.debug(f"Handling the arg: {arg}")
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
                                            converted_equip_params[equip_param], time_stamp, address, spn_file_json) \
                                            if equip_param in converted_equip_params else ""
                                    else:
                                        var_dict[sample_obj[equip_param]] = converted_equip_params[equip_param] \
                                            if equip_param in converted_equip_params else ""
                            else:
                                sample_obj = samples[param][0]
                                for fc_param in sample_obj:
                                    final_fc = []
                                    if fc_param in converted_equip_fc:
                                        final_fc = get_active_faults(converted_equip_fc[fc_param], address)
                                    var_dict[sample_obj[fc_param]] = final_fc
        LOGGER.debug(f"HB CD SDK Class Variable Dict: {var_dict}")
        hb_sdk_object = map_ngdi_sample_to_cd_payload(var_dict)

        # de-obfuscate GPS co-ordinates
        if ("Latitude" in hb_sdk_object) and ("Longitude" in hb_sdk_object):
            latitude = hb_sdk_object["Latitude"]
            longitude = hb_sdk_object["Longitude"]
            hb_sdk_object["Latitude"], hb_sdk_object["Longitude"] = \
                handle_gps_coordinates(latitude, longitude, deobfuscate=True)

        LOGGER.info(f"Posting Sample to CD...")
        post_cd_message(hb_sdk_object)
    except Exception as e:
        error_message = f"An exception occurred while handling HB sample: {e}"
        LOGGER.error(error_message)
        util.write_to_audit_table("J1939_HB", error_message)


def handle_fc(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp):
    var_dict = {}
    address = ""
    found_fcs = False
    LOGGER.info(f"Retrieving parameters for creating FC SDK Class Object")
    try:
        for arg in class_arg_map:
            LOGGER.debug(f"Handling the arg: {arg}")
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
                                            converted_equip_params[equip_param].copy(), time_stamp, address,
                                            spn_file_json) \
                                            if equip_param in converted_equip_params else ""
                                    else:
                                        var_dict[sample_obj[equip_param]] = converted_equip_params[equip_param] \
                                            if equip_param in converted_equip_params else ""
                            else:
                                sample_obj = samples[param][0]
                                for fc_param in sample_obj:
                                    if fc_param in converted_equip_fc and fc_param == active_fault_code_indicator:
                                        all_active_fcs = converted_equip_fc[fc_param].copy()
                                        if not all_active_fcs:
                                            continue
                                        found_fcs = True  # Indicating that we found Fault Codes in this file.
                                        fc_index = 0
                                        final_fc = get_active_faults(all_active_fcs, address)
                                        for fc in all_active_fcs:
                                            create_fc_class(fc, final_fc, fc_index, sample_obj[fc_param], var_dict, 1)
                                            fc_index = fc_index + 1
                                    elif fc_param in converted_equip_fc and fc_param == inactive_fault_code_indicator:
                                        all_inactive_fcs = converted_equip_fc[fc_param].copy()
                                        if not all_inactive_fcs:
                                            continue
                                        found_fcs = True  # Indicating that we found Fault Codes in this file.
                                        all_active_fcs = converted_equip_fc[active_fault_code_indicator].copy() if \
                                            active_fault_code_indicator in converted_equip_fc else []
                                        fc_index = 0
                                        inactive_final_fc = get_active_faults(all_inactive_fcs, address)
                                        active_final_fc = get_active_faults(all_active_fcs, address)
                                        for fc in all_inactive_fcs:
                                            create_fc_class(fc, inactive_final_fc, fc_index, sample_obj[fc_param],
                                                            var_dict, 0, active_final_fc)
                                            fc_index = fc_index + 1
                                    else:
                                        # TODO Handle Pending Fault Codes.
                                        LOGGER.info(f"There are either no, fc_param in this file -- We are not "
                                                    f"handling pending FCs for now.")
        if found_fcs:
            LOGGER.info("We have already processed this sample since it had fault codes. Continuing to the next sample")
        else:
            LOGGER.info(f"This sample had no Fault Code information, checking if this is the Single Sample")
            if "Telematics_Partner_Message_ID".lower() in var_dict:
                LOGGER.info(f'Found Message ID: {var_dict["Telematics_Partner_Message_ID".lower()]} in this sample!'
                            f'This is the Single Sample. Proceeding to the next sample')
            else:
                LOGGER.error(
                    f"There was an Error in this FC sample. It is not the Single Sample and it does not have FC info!")
    except Exception as e:
        error_message = f"An exception occurred while handling FC sample: {e}"
        LOGGER.error(error_message)
        util.write_to_audit_table("J1939_FC", error_message)


def create_fc_class(fc, f_codes, fc_index, fc_param, var_dict, active_or_inactive, active_fault_array=None):
    fcs = f_codes.copy()
    fcs.pop(fc_index)
    variable_dict = var_dict.copy()
    variable_dict[fc_param] = fcs if not active_fault_array else active_fault_array
    variable_dict[active_cd_parameter] = active_or_inactive
    variable_dict[spn_indicator.lower()] = fc["SPN"]
    variable_dict[fmi_indicator.lower()] = fc["FMI"]
    variable_dict[count_indicator.lower()] = fc["count"]
    fc_sdk_object = map_ngdi_sample_to_cd_payload(variable_dict, fc=True)
    LOGGER.info(f"Posting Sample to CD...")
    post_cd_message(fc_sdk_object)


def send_sample(sample, metadata, fc_or_hb):
    converted_equip_params = []
    converted_device_params = {}
    converted_equip_fc = []
    time_stamp = ""
    if converted_equip_params_var in sample:
        converted_equip_params = sample[converted_equip_params_var][0] if sample[converted_equip_params_var] else []
        LOGGER.debug(f"Found  {converted_equip_params_var} : {converted_equip_params}")
    if converted_device_params_var in sample:
        converted_device_params = sample[converted_device_params_var] if sample[converted_device_params_var] else {}
        LOGGER.debug(f"Found {converted_device_params_var} : {converted_device_params}")
    if converted_equip_fc_var in sample:
        converted_equip_fc = sample[converted_equip_fc_var][0] if sample[converted_equip_fc_var] else []
        LOGGER.debug(f"Found {converted_equip_fc_var} : {converted_equip_fc}")
    if time_stamp_param in sample:
        time_stamp = sample[time_stamp_param]
    LOGGER.debug(f"New converted_equip_params: {converted_equip_params}")
    LOGGER.debug(f"New converted_device_params: {converted_device_params}")
    LOGGER.debug(f"New converted_equip_fc , {converted_equip_fc}")
    if fc_or_hb.lower() == "hb":
        store_health_parameters_into_redshift(converted_device_params, time_stamp, metadata)
        LOGGER.info(f"Handling HB...")
        handle_hb(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp)
    else:
        LOGGER.info(f"Handling FC...")
        handle_fc(converted_device_params, converted_equip_params, converted_equip_fc, metadata, time_stamp)


def retrieve_and_process_file(uploaded_file_object, api_url):
    bucket = uploaded_file_object["source_bucket_name"]
    key = uploaded_file_object["file_key"]
    file_size = uploaded_file_object["file_size"]
    LOGGER.info(f"Retrieving the JSON file from the NGDI folder")
    j1939_file_object = s3_client.get_object(Bucket=bucket, Key=key)
    file_metadata = j1939_file_object["Metadata"]
    LOGGER.info(f"File Metadata: {file_metadata}")
    fc_or_hb = file_metadata['j1939type'] if "j1939type" in file_metadata else None
    uuid = file_metadata['uuid']
    file_date_time = str(j1939_file_object['LastModified'])[:19]
    file_name = key.split('/')[-1]
    device_id = file_name.split('_')[1]
    LOGGER.info(f"FC or HB: {fc_or_hb}")
    if not fc_or_hb:
        LOGGER.error(f"Error! Cannot determine if this is an FC of an HB file. Check file metadata!")
        return
    j1939_file_stream = j1939_file_object['Body'].read()
    j1939_file = json.loads(j1939_file_stream)
    LOGGER.debug(f"File as JSON: {j1939_file}")
    if fc_or_hb.lower() == 'hb':
        esn = j1939_file['componentSerialNumber']
        # Please note that the order is expected to be <Make>*<Model>***<ESN>**** for Improper PSBU ESN
        if esn and "*" in esn:
            esn = [esn_component for esn_component in esn.split("*") if esn_component][-1]

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
    try:
        get_response = edge.api_request(api_url, "get", query.get_sql(quote_char=None))
        LOGGER.info(f"Consumption View Response: {get_response}")
    except Exception as exception:
        # Using logging level 'info' in case exception occurred due to invalid query
        LOGGER.info(f"Consumption View Query: {query.get_sql(quote_char=None)}")
        return edge.server_error(str(exception))
    request_id = get_response[0]['request_id'] if get_response and "request_id" in get_response[0] else None
    consumption_per_request = get_response[0]['consumption_per_request'] if get_response and get_response[0][
        'consumption_per_request'] else None
    if consumption_per_request == 'null':
        consumption_per_request = None

    sqs_message = uuid + "," + str(device_id) + "," + str(file_name) + "," + str(file_size) + "," + str(
        file_date_time) + "," + str(data_protocol) + "," + 'FILE_SENT' + "," + str(esn) + "," + str(
        config_spec_name) + "," + str(request_id) + "," + str(consumption_per_request) + "," + " " + "," + " "
    sqs_send_message(os.environ["metaWriteQueueUrl"], sqs_message, edgeCommonAPIURL)
    samples = j1939_file["samples"] if "samples" in j1939_file else None
    metadata = get_metadata_info(j1939_file)
    if metadata:
        if samples:
            for sample in samples:
                send_sample(sample, metadata, fc_or_hb)
        else:
            error_message = f"There are no samples in this file for the device: {device_id}."
            LOGGER.error(error_message)
            util.write_to_audit_table(data_protocol, error_message, device_id)
        delete_message_from_sqs_queue(uploaded_file_object["sqs_receipt_handle"])
    else:
        error_message = f"Metadata retrieval failed for the device: {device_id}."
        LOGGER.error(error_message)
        util.write_to_audit_table(data_protocol, error_message, device_id)


@ssm.cache(parameter=name, entry_name='parameters')  # noqa-cache accepts list as a parameter but expects a str
def lambda_handler(event, context):
    try:
        vals = getattr(context, 'parameters')
        api_url = vals[params['Names'][0]]
    except AttributeError as ae:
        LOGGER.error(f"Error, could not find ssm in the cache\n Proceeding as usual: {ae}")
        ssm_uncached = boto3.client('ssm')
        response = ssm_uncached.get_parameters(Names=name, WithDecryption=False)
        api_url = response['Parameters'][0]['Value']
    records = event.get("Records", [])
    processes = []
    LOGGER.debug(f"Received SQS Records: {records}.")
    for record in records:
        s3_event_body = json.loads(record["body"])
        s3_event = s3_event_body['Records'][0]['s3']

        uploaded_file_object = dict(
            source_bucket_name=s3_event['bucket']['name'],
            file_key=s3_event['object']['key'].replace("%", ":").replace("3A", ""),
            file_size=s3_event['object']['size'],
            sqs_receipt_handle=record["receiptHandle"]
        )
        LOGGER.info(f"Uploaded File Object: {uploaded_file_object}.")

        # Retrieve the uploaded file from the s3 bucket and process the uploaded file
        process = Process(target=retrieve_and_process_file, args=(uploaded_file_object, api_url,))

        # Make a list of all process to wait and terminate at the end
        processes.append(process)

        # Start process
        process.start()

    # Make sure that all processes have finished
    for process in processes:
        process.join()


'''
Function to get Health Parameter and store into Redshift Table
'''


def store_health_parameters_into_redshift(converted_device_params, time_stamp, j1939_file_val):
    LOGGER.info(f"Starting Kinesis Process... ")
    if 'messageID' in converted_device_params:
        message_id = converted_device_params['messageID']
        cpu_temperature = converted_device_params[
            'CPU_temperature'] if 'CPU_temperature' in converted_device_params else None
        pmic_temperature = converted_device_params[
            'PMIC_temperature'] if ('PMIC_temperature' in converted_device_params) and converted_device_params[
            'PMIC_temperature'] else None
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
        LOGGER.info(f"There is no Converted Device Parameter")
