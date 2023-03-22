import os
import sys
sys.path.insert(1, './lib')

import csv
import json
import time
import boto3
import datetime
import utility as util
from multiprocessing import Process
from edge_sqs_utility_layer.sqs_utility import sqs_send_message
from edge_db_lambda_client import EdgeDbLambdaClient
import re
from botocore.exceptions import ClientError

LOGGER = util.get_logger(__name__)

s3 = boto3.client('s3')
s3_client = boto3.client('s3')
cp_post_bucket = os.environ["CPPostBucket"]
edgeCommonAPIURL = os.environ["edgeCommonAPIURL"]
NGDIBody = json.loads(os.environ["NGDIBody"])
mapTspFromOwner = os.environ["mapTspFromOwner"]
MAX_ATTEMPTS = int(os.environ["MaxAttempts"])
EDGE_DB_CLIENT = EdgeDbLambdaClient()
APP_ENV = os.environ["APPLICATION_ENVIRONMENT"]
TABLE_NAME = os.environ["J1939ActiveFaultCodeTable"]


def delete_message_from_sqs_queue(receipt_handle):
    queue_url = os.environ["QueueUrl"]
    sqs_client = boto3.client('sqs')  # noqa
    sqs_message_deletion_response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    LOGGER.debug(f"SQS message deletion response: '{sqs_message_deletion_response}'. . .")
    return sqs_message_deletion_response


def process_ss(ss_rows, ss_dict, ngdi_json_template, ss_converted_prot_header, ss_converted_device_parameters):
    try:
        ss_values = ss_rows[1]  # Get the SS Values row

        json_sample_head = ngdi_json_template
        parameters = {}
        ss_sample = {"convertedDeviceParameters": {}, "convertedEquipmentParameters": []}

        converted_prot_header = ss_converted_prot_header.split("~")

        try:
            protocol = converted_prot_header[1]
            network_id = converted_prot_header[2]
            address = converted_prot_header[3]
        except IndexError as e:
            LOGGER.error(f"An exception occurred while trying to retrieve the AS protocols network_id and Address:{e}")
            return

        for key in ss_converted_device_parameters:
            if key:
                if "messageid" in key.lower():
                    ss_sample["convertedDeviceParameters"][key] = ss_values[ss_dict[key]]
                else:
                    json_sample_head[key] = ss_values[ss_dict[key]]
            del ss_dict[key]

        conv_eq_obj = {"protocol": protocol, "networkId": network_id, "deviceId": address}

        for param in ss_dict:
            if "datetimestamp" in param.lower():
                ss_sample["dateTimestamp"] = ss_values[ss_dict[param]]
            elif param:
                parameters[param] = ss_values[ss_dict[param]]

        conv_eq_obj["parameters"] = parameters
        ss_sample["convertedEquipmentParameters"].append(conv_eq_obj)
        json_sample_head["samples"].append(ss_sample)
        LOGGER.debug(f"Process SS JSON Sample Head: {json_sample_head}")

        return json_sample_head
    except Exception as e:
        LOGGER.error(f"An exception occurred while handling the Single Sample:{e}")


def process_as(as_rows, as_dict, ngdi_json_template, as_converted_prot_header, as_converted_device_parameters):
    old_as_dict = as_dict
    json_sample_head = ngdi_json_template
    json_sample_head["numberOfSamples"] = len(as_rows)
    converted_prot_header = as_converted_prot_header.split("~")
    esn = ngdi_json_template["componentSerialNumber"]
    timestamp=""

    try:
        protocol = converted_prot_header[1]
        network_id = converted_prot_header[2]
        address = converted_prot_header[3]
    except IndexError as e:
        LOGGER.error(f"An exception occurred while trying to retrieve the AS protocols network_id and Address: {e}")
        return

    for values in as_rows:
        new_as_dict = {x: old_as_dict[x] for x in old_as_dict}
        parameters = {}
        sample = {"convertedDeviceParameters": {}, "rawEquipmentParameters": [], "convertedEquipmentParameters": [],
                  "convertedEquipmentFaultCodes": []}

        for key in as_converted_device_parameters:
            if key:
                sample["convertedDeviceParameters"][key] = values[new_as_dict[key]]

            del new_as_dict[key]

        conv_eq_obj = {"protocol": protocol, "networkId": network_id, "deviceId": address}

        for param in new_as_dict:
            if param:
                if "datetimestamp" in param.lower():
                    sample["dateTimestamp"] = values[new_as_dict[param]]
                    timestamp=values[new_as_dict[param]]
                elif param != "activeFaultCodes" and param != "inactiveFaultCodes" and param != "pendingFaultCodes":
                    parameters[param] = values[new_as_dict[param]]

        conv_eq_obj["parameters"] = parameters
        sample["convertedEquipmentParameters"].append(conv_eq_obj)
        conv_eq_fc_obj = {"protocol": protocol, "networkId": network_id, "deviceId": address, "activeFaultCodes": [],
                          "inactiveFaultCodes": [], "pendingFaultCodes": []}

        LOGGER.debug(f"ESN is {esn}")
        LOGGER.debug(f"TimeStamp is {timestamp}")
        active_fc_from_db=get_active_fault_codes_from_dynamodb(esn)
        db_esn_ac_fcs = None
        if 'Item' in active_fc_from_db:
            db_esn_ac_fcs = active_fc_from_db['Item']

        db_timestamp_check = check_active_fault_codes_timestamp(db_esn_ac_fcs, timestamp)
        if db_timestamp_check:
            if "activeFaultCodes" in new_as_dict:
                ac_fc = values[new_as_dict["activeFaultCodes"]]
                if ac_fc:
                    generate_active_fault_codes(esn, ac_fc, conv_eq_fc_obj, db_esn_ac_fcs, timestamp)
                else:
                    LOGGER.debug(f"{ac_fc} is empty")
        else:
            LOGGER.debug(f"db_timestamp is greater than timestamp")


        if "inactiveFaultCodes" in new_as_dict:
            inac_fc = values[new_as_dict["inactiveFaultCodes"]]
            if inac_fc:
                ac_fc_array = inac_fc.split("|")
                for fc in ac_fc_array:
                    if fc:
                        fc_obj = {}
                        fc_arr = fc.split("~")
                        for fc_val in fc_arr:
                            fc_obj[fc_val.split(":")[0]] = fc_val.split(":")[1]

                        conv_eq_fc_obj["inactiveFaultCodes"].append(fc_obj)

        if "pendingFaultCodes" in new_as_dict:
            pen_fc = values[new_as_dict["pendingFaultCodes"]]
            if pen_fc:
                ac_fc_array = pen_fc.split("|")
                for fc in ac_fc_array:
                    if fc:
                        fc_obj = {}
                        fc_arr = fc.split("~")
                        for fc_val in fc_arr:
                            fc_obj[fc_val.split(":")[0]] = fc_val.split(":")[1]

                        conv_eq_fc_obj["pendingFaultCodes"].append(fc_obj)

        sample["convertedEquipmentFaultCodes"].append(conv_eq_fc_obj)

        json_sample_head["samples"].append(sample)
        LOGGER.debug(f"Process AS JSON Sample Head: {json_sample_head}")

    return json_sample_head


def get_device_id(ngdi_json_template):
    if "telematicsDeviceId" in ngdi_json_template:
        return ngdi_json_template["telematicsDeviceId"]

    return False


def get_tsp_and_cust_ref(device_id):

    attempts = 0
    get_tsp_cust_ref_payload = f"select cust_ref, device_owner from da_edge_olympus.device_information WHERE device_id = '{device_id}';"

    LOGGER.debug(f"Get TSP and Cust_Ref payload:  {get_tsp_cust_ref_payload}")
    while attempts < MAX_ATTEMPTS:
        try:
            attempts += 1
            get_tsp_cust_ref_response = EDGE_DB_CLIENT.execute(get_tsp_cust_ref_payload, method='WRITE')

            # get_tsp_cust_ref_response = requests.post(url=edgeCommonAPIURL, json=get_tsp_cust_ref_payload)
            get_tsp_cust_ref_response_body = json.loads(get_tsp_cust_ref_response['body'])
            get_tsp_cust_ref_response_code = get_tsp_cust_ref_response['statusCode']

            LOGGER.debug(f"Get TSP and Cust_Ref response code: {get_tsp_cust_ref_response_code}, "
                         f"body: {get_tsp_cust_ref_response_body}")

            if (get_tsp_cust_ref_response_body and get_tsp_cust_ref_response_code == 200) and \
                    ("cust_ref" in get_tsp_cust_ref_response_body[0] and get_tsp_cust_ref_response_body[0]["cust_ref"]) and \
                    ("device_owner" in get_tsp_cust_ref_response_body[0] and get_tsp_cust_ref_response_body[0]["device_owner"]):
                return get_tsp_cust_ref_response_body[0]
        except Exception as e:
            pass
        time.sleep(2 * attempts / 10)  # Sleep for 200 ms exponentially

    return None


def get_cspec_req_id(sc_number):
    if '-' in sc_number:
        config_spec_name = ''.join(sc_number.split('-')[:-1])
        req_id = sc_number.split('-')[-1]
    else:
        config_spec_name = sc_number
        req_id = None
    return config_spec_name, req_id


def retrieve_and_process_file(uploaded_file_object):
    bucket_name = uploaded_file_object["source_bucket_name"]
    file_key = uploaded_file_object["file_key"]
    file_size = uploaded_file_object["file_size"]
    file_key = file_key.replace("%3A", ":")
    LOGGER.info(f"New FileKey: {file_key}")

    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_file = obj['Body'].read().decode('utf-8').splitlines(True)

    file_date_time = str(obj['LastModified'])[:19]
    file_metadata = obj["Metadata"]
    LOGGER.debug(f"File Metadata: {file_metadata}")

    fc_uuid = file_metadata['uuid']
    file_name = file_key.split('/')[-1]
    device_id = file_name.split('_')[1]
    esn = file_name.split('_')[2]
    config_spec_name, req_id = get_cspec_req_id(file_name.split('_')[3])

    sqs_message = str(fc_uuid) + "," + str(device_id) + "," + str(file_name) + "," + str(file_size) + "," + str(
        file_date_time) + "," + str('J1939_FC') + "," + str('CSV_JSON_CONVERTED') + "," + str(esn) + "," + str(
        config_spec_name) + "," + str(req_id) + "," + str(None) + "," + " " + "," + " "
    sqs_send_message(os.environ["metaWriteQueueUrl"], sqs_message, edgeCommonAPIURL)

    ngdi_json_template = json.loads(os.environ["NGDIBody"])

    ss_row = False
    seen_ss = False

    csv_rows = []

    # Get all the rows from the CSV
    for row in csv.reader(csv_file, delimiter=','):
        new_row = list(filter(lambda x: x != '', row))

        if new_row:
            csv_rows.append(row)

    LOGGER.debug(f"CSV Rows:  {csv_rows}")

    ss_rows = []
    as_rows = []

    for row in csv_rows:
        if not ss_row and not seen_ss:
            if "messageFormatVersion" in row:
                ngdi_json_template["messageFormatVersion"] = row[1] if row[1] else None
            elif "dataEncryptionSchemeId" in row:
                ngdi_json_template["dataEncryptionSchemeId"] = row[1] if row[1] else None
            elif "telematicsBoxId" in row:
                ngdi_json_template["telematicsDeviceId"] = row[1] if row[1] else None
            elif "componentSerialNumber" in row:
                ngdi_json_template["componentSerialNumber"] = row[1] if row[1] else None
            elif "dataSamplingConfigId" in row:
                ngdi_json_template["dataSamplingConfigId"] = row[1] if row[1] else None
            elif "ssDateTimestamp" in row:
                # Found the Single Sample Row. Append the row as Single Sample row.
                ss_row = True
                seen_ss = True
                ss_rows.append(row)
            elif "asDateTimestamp" in row:
                # If there are no Single Samples, append the row as an All Sample row and stop looking for SS rows
                ss_row = False
                seen_ss = True
                as_rows.append(row)
        elif ss_row:
            if "asDateTimestamp" in row:
                LOGGER.error(f"ERROR! Missing the Single Sample Values.")
                return

            ss_rows.append(row)
            ss_row = False
        else:
            as_rows.append(row)

    # Make sure that we received values in the AS (as_rows > 1) and/or SS
    if not seen_ss or (not as_rows) or len(as_rows) < 2:
        error_message = "Missing the Single Sample Values or the All Samples Values."
        LOGGER.error(error_message)
        util.write_to_audit_table(error_message, device_id)
        return

    LOGGER.debug(f"NGDI Template after main metadata addition: {ngdi_json_template}")

    ss_dict = {}
    as_dict = {}
    ss_converted_prot_header = ""
    as_converted_prot_header = ""
    count = 0

    ss_headers = ss_rows[0] if ss_rows else []
    as_headers = as_rows[0] if as_rows else []

    ss_converted_device_parameters = []
    seen_ss_dev_params = False
    seen_ss_j1939_params = False
    seen_ss_raw_params = False

    as_converted_device_parameters = []
    seen_as_dev_params = False
    seen_as_j1939_params = False
    seen_as_raw_params = False

    # For each of the headers in the SS row, map the index to the header value
    for head in ss_headers:
        if 'device' in head.lower() and 'converted' in head.lower():
            seen_ss_dev_params = True

        if 'j1939' in head.lower() and 'raw' in head.lower():
            seen_ss_raw_params = True

        if 'j1939' in head.lower() and 'converted' in head.lower():
            ss_converted_prot_header = head
            seen_ss_j1939_params = True

        if '~' in head:
            count = count + 1
        elif "datetimestamp" in head.lower():
            ss_dict["dateTimeStamp"] = count
            count = count + 1
        else:
            if seen_ss_dev_params and not seen_ss_raw_params and not seen_ss_j1939_params:
                ss_converted_device_parameters.append(head)

            ss_dict[head] = count
            count = count + 1

    count = 0

    # For each of the headers in the SS row, map the index to the header value
    for head in as_headers:
        if 'j1939' in head.lower() and 'converted' in head.lower():
            as_converted_prot_header = head
            seen_as_j1939_params = True

        if 'j1939' in head.lower() and 'raw' in head.lower():
            seen_as_raw_params = True

        if 'converted' in head.lower() and 'device' in head.lower():
            seen_as_dev_params = True

        if '~' in head:
            count = count + 1
        elif "datetimestamp" in head.lower():
            as_dict["dateTimeStamp"] = count
            count = count + 1
        else:
            if seen_as_dev_params and not seen_as_raw_params and not seen_as_j1939_params:
                as_converted_device_parameters.append(head)

            as_dict[head] = count
            count = count + 1

    # Get rid of the AS header row since we have already stored the index of each header
    if as_rows:
        LOGGER.debug(f"Removing AS Header Row --index '0'-- from: {as_rows}")
        del as_rows[0]
        LOGGER.debug(f"New AS Rows: {as_rows}")

    LOGGER.info("Handling Single Samples")
    ngdi_json_template = process_ss(ss_rows, ss_dict, ngdi_json_template, ss_converted_prot_header,
                                    ss_converted_device_parameters) if ss_rows else ngdi_json_template

    LOGGER.info("Handling All Samples")
    ngdi_json_template = process_as(as_rows, as_dict, ngdi_json_template, as_converted_prot_header,
                                    as_converted_device_parameters)

    tsp_in_file = "telematicsPartnerName" in ngdi_json_template and ngdi_json_template["telematicsPartnerName"]
    cust_ref_in_file = "customerReference" in ngdi_json_template and ngdi_json_template["customerReference"]

    if not (tsp_in_file and cust_ref_in_file):
        LOGGER.info(f"Retrieve Device ID from file . . .")
        device_id = get_device_id(ngdi_json_template)

        if not device_id:
            error_message = f"Device ID '{device_id}' is not in the file! Aborting!"
            LOGGER.error(error_message)
            util.write_to_audit_table(error_message, device_id)
            return

        LOGGER.info(f"Retrieving TSP and Customer Reference from EDGE DB . . .")
        got_tsp_and_cust_ref = get_tsp_and_cust_ref(device_id)

        if not got_tsp_and_cust_ref:
            error_message = "Could not retrieve TSP and Cust Ref. These are mandatory fields!"
            LOGGER.error(error_message)
            util.write_to_audit_table(error_message, device_id)
            return

        else:
            tsp_owners = json.loads(mapTspFromOwner)
            tsp_name = tsp_owners[str(got_tsp_and_cust_ref["device_owner"])] if str(
                got_tsp_and_cust_ref["device_owner"]) in tsp_owners else "NA"
            if tsp_name != "NA":
                ngdi_json_template["telematicsPartnerName"] = tsp_name
            else:
                LOGGER.error(f"Error! Could not retrieve TSP. This is mandatory field!")
                return
            ngdi_json_template["customerReference"] = got_tsp_and_cust_ref["cust_ref"]

            LOGGER.debug(f"Final file with TSP and Cust Ref: {ngdi_json_template}")

    filename = file_key
    LOGGER.info(f"Filename: {filename}")

    try:
        LOGGER.info(f"Retrieving date info for File Path from filename . . .")
        file_name_array = filename.split('_')
        date_component = file_name_array[3]
        current_datetime = datetime.datetime.strptime(date_component, "%Y%m%d%H%M%S")

        store_file_path = "ConvertedFiles/" + esn + '/' + device_id + '/' + ("%02d" % current_datetime.year) + '/' + \
                          ("%02d" % current_datetime.month) + '/' + ("%02d" % current_datetime.day) + '/' + \
                          filename.split('.csv')[0] + '.json'

    except Exception as e:
        LOGGER.error(
            f"An error occurred while trying to get the file path from the file name:{e} Using current date-time . . .")

        current_datetime = datetime.datetime.now()

        store_file_path = "ConvertedFiles/" + ngdi_json_template['componentSerialNumber'] + '/' + \
                          ngdi_json_template["telematicsDeviceId"] + '/' + ("%02d" % current_datetime.year) + '/' + \
                          ("%02d" % current_datetime.month) + '/' + ("%02d" % current_datetime.day) + '/' + \
                          filename.split('.csv')[0] + '.json'

    LOGGER.info(f"New Filename: {store_file_path}")

    store_file_response = s3_client.put_object(Bucket=cp_post_bucket,
                                               Key=store_file_path,
                                               Body=json.dumps(ngdi_json_template).encode(),
                                               Metadata={'j1939type': 'FC', 'uuid': fc_uuid})

    LOGGER.debug(f"Store File Response: {store_file_response}")

    if store_file_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        # Delete message from Queue after success
        delete_message_from_sqs_queue(uploaded_file_object["sqs_receipt_handle"])


def lambda_handler(lambda_event, context):  # noqa
    records = lambda_event.get("Records", [])
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
        LOGGER.debug(f"Uploaded File Object: {uploaded_file_object}.")

        # Retrieve the uploaded file from the s3 bucket and process the uploaded file
        process = Process(target=retrieve_and_process_file, args=(uploaded_file_object,))

        # Make a list of all process to wait and terminate at the end
        processes.append(process)

        # Start process
        process.start()

    # Make sure that all processes have finished
    for process in processes:
        process.join()

def get_active_fault_codes_from_dynamodb(esn):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)
    try:
        response = table.get_item(Key={'esn': esn})
    except ClientError as e:
        LOGGER.error('error', e.response['Error']['Message'])
    else:
        return response

def put_active_fault_codes(esn, ts, ac_fc):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)
    response = table.put_item(
       Item={
            'esn': esn,
            'timestamp': ts,
            'fcs': ac_fc
        }
    )

    return response
def generate_spn_fmi_fc_obj( actual_ac_fc, conc_eq_fc_obj):
    fc_obj = {}
    fc_arr = actual_ac_fc.split("~")
    for fc_val in fc_arr:
        fc_obj[fc_val.split(":")[0]] = fc_val.split(":")[1]
    conc_eq_fc_obj['activeFaultCodes'].append(fc_obj)

def check_active_fault_codes_timestamp(db_esn_ac_fcs,timestamp):
    if db_esn_ac_fcs is None:
        return True
    else:
        db_timestamp = db_esn_ac_fcs.get('timestamp')
        if db_timestamp < timestamp:
            return True
        else:
            return False

def generate_active_fault_codes(esn, ac_fc, conc_eq_fc_obj, db_esn_ac_fcs,timestamp):

    spn_fmi_combo_list = re.split("\|", ac_fc)
    if spn_fmi_combo_list and not spn_fmi_combo_list[-1].strip():
        spn_fmi_combo_list.pop()

    if not spn_fmi_combo_list:
        LOGGER.debug(f"spn_fmi_combo_list is empty : {spn_fmi_combo_list}")
        return conc_eq_fc_obj

    sorted_spn_fmi_combo_list = sorted(spn_fmi_combo_list)
    insert_spn_fmi_fcs_db = {}
    update_spn_fmi_fcs_db = {}

    for actual_ac_fc in sorted_spn_fmi_combo_list:#
        db_ac_fc = actual_ac_fc.rsplit('~', 1)[0]
        ac_fc_cnt = actual_ac_fc.split('~', 2)[2].split(":")[1]
        if db_esn_ac_fcs == None:
            LOGGER.debug(f"new esn found does not exist in database : {esn}")
            insert_spn_fmi_fcs_db[db_ac_fc] = ac_fc_cnt
            generate_spn_fmi_fc_obj(actual_ac_fc, conc_eq_fc_obj)
        else:
            existing_fc_from_db = db_esn_ac_fcs.get('fcs')
            ac_fc_db_cnt = existing_fc_from_db.get(db_ac_fc)
            LOGGER.debug(f"existing fault_codes from database for esn: {existing_fc_from_db}")
            #checking if the fault_codes contains  in the database
            if existing_fc_from_db.get(db_ac_fc) == None:
                LOGGER.debug(f"fault_code not found in database for exiting esn : {actual_ac_fc}")
                update_spn_fmi_fcs_db[db_ac_fc] = ac_fc_cnt
                generate_spn_fmi_fc_obj(actual_ac_fc, conc_eq_fc_obj)
            elif int(ac_fc_cnt) != int(ac_fc_db_cnt):
                LOGGER.debug(f"fault_code found in database for exiting esn and count not matching: {actual_ac_fc}")
                update_spn_fmi_fcs_db[db_ac_fc] = ac_fc_cnt
                generate_spn_fmi_fc_obj(actual_ac_fc, conc_eq_fc_obj)
            else:
                LOGGER.debug(f"duplicate fault_code for exiting esn : {actual_ac_fc}")

    if len(insert_spn_fmi_fcs_db) > 0:
        put_active_fault_codes(esn,timestamp,insert_spn_fmi_fcs_db)
        LOGGER.debug("fault_codes inserted successfully into the database for new esn:",insert_spn_fmi_fcs_db)

    if len(update_spn_fmi_fcs_db) > 0:
        existing_spn_fmi_fcs = db_esn_ac_fcs.get('fcs')
        for key, value in update_spn_fmi_fcs_db.items():
            existing_spn_fmi_fcs[key] = value
        put_active_fault_codes(esn, timestamp, existing_spn_fmi_fcs)
        LOGGER.debug("new fault_codes inserted successfully into the database for existing esn:", esn)

    return conc_eq_fc_obj

