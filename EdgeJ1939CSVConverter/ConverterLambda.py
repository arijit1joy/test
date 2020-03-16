import csv
import boto3
import json
import os

s3 = boto3.client('s3')
cp_post_bucket = os.environ["CPPostBucket"]
MandatoryParameters = json.loads(os.environ["MandatoryParameters"])
NGDIBody = json.loads(os.environ["NGDIBody"])
s3_client = boto3.client('s3')


def process_ss(ss_rows, ss_dict, ngdi_json_template, ss_converted_prot_header,
               ss_raw_prot_header, ss_converted_device_parameters):
    ss_values = ss_rows[1]

    print(" ")
    print("<------------------------------------------NEW SS SAMPLE--------------------------------------------->")
    print(" ")

    print("Single Sample Values:", ss_values)

    json_sample_head = ngdi_json_template

    print("Received Json Body in SS Handler:", json_sample_head)

    parameters = {}

    ss_sample = {"convertedDeviceParameters": {}, "convertedEquipmentParameters": []}

    print("Single Sample Converted Protocol Header:", ss_converted_prot_header)

    converted_prot_header = ss_converted_prot_header.split("~")

    print("Converted Protocol Header Array:", converted_prot_header)

    try:
        protocol = converted_prot_header[1]

        print("protocol:", protocol)

        network_id = converted_prot_header[2]

        print("network_id:", network_id)

        address = converted_prot_header[3]

        print("address:", address)

    except IndexError as e:

        print("An exception occurred while trying to retrieve the AS protocols network_id and Address:", e)

        return

    print("Handling the device metadata headers in SS:", ss_converted_device_parameters)

    for key in ss_converted_device_parameters:

        if key:

            print("Processing", key)

            if "messageid" in key:

                ss_sample["convertedDeviceParameters"][key] = ss_values[ss_dict[key]]

            else:

                json_sample_head[key] = ss_values[ss_dict[key]]

        del ss_dict[key]

    conv_eq_obj = {"protocol": protocol, "networkId": network_id, "deviceId": address}

    print("Json Sample Head after metadata retrieval:", json_sample_head)
    print("Converted Equipment Object:", conv_eq_obj)

    for param in ss_dict:

        if param:
            parameters[param] = ss_values[ss_dict[param]]

    print("SS Parameters:", parameters)

    conv_eq_obj["parameters"] = parameters

    print("Converted Equipment Object with Parameters:", conv_eq_obj)

    ss_sample["convertedEquipmentParameters"].append(conv_eq_obj)

    print("Single Sample:", ss_sample)

    json_sample_head["samples"].append(ss_sample)

    return json_sample_head


def process_as(as_rows, as_dict, ngdi_json_template, as_converted_prot_header,
               as_raw_prot_header, as_converted_device_parameters):
    old_as_dict = as_dict

    json_sample_head = ngdi_json_template

    json_sample_head["numberOfSamples"] = len(as_rows)

    print("Original Template from SS to AS", json_sample_head)

    converted_prot_header = as_converted_prot_header.split("~")

    print("AS Converted Protocol Header array:", converted_prot_header)

    try:
        protocol = converted_prot_header[1]

        print("protocol:", protocol)

        network_id = converted_prot_header[2]

        print("network_id:", network_id)

        address = converted_prot_header[3]

        print("address:", address)

    except IndexError as e:

        print("An exception occurred while trying to retrieve the AS protocols network_id and Address:", e)

        return

    for values in as_rows:

        new_as_dict = {x: old_as_dict[x] for x in old_as_dict}

        parameters = {}

        sample = {"convertedDeviceParameters": {}, "rawEquipmentParameters": [], "convertedEquipmentParameters": [],
                  "convertedEquipmentFaultCodes": []}

        print(" ")
        print("<------------------------------------------NEW AS SAMPLE--------------------------------------------->")
        print(" ")

        print("OLD AS DICT:", old_as_dict)
        print("AS DICT:", new_as_dict)

        for key in as_converted_device_parameters:

            if key:

                if "datetimestamp" in key.lower():

                    sample[key] = values[new_as_dict[key]]

                else:

                    sample["convertedDeviceParameters"][key] = values[new_as_dict[key]]

            del new_as_dict[key]

        print("Current Sample with Converted Device Parameters:", sample)

        conv_eq_obj = {"protocol": protocol, "networkId": network_id, "deviceId": address}

        print("Current ConvertedEquipmentParameters:", conv_eq_obj)

        for param in new_as_dict:

            if param:

                if param != "activeFaultCodes" and param != "inactiveFaultCodes" and param != "pendingFaultCodes":
                    parameters[param] = values[new_as_dict[param]]

        conv_eq_obj["parameters"] = parameters

        sample["convertedEquipmentParameters"].append(conv_eq_obj)

        print("Current Sample with Converted Equipment Parameters:", sample)

        conv_eq_fc_obj = {"protocol": protocol, "networkId": network_id, "deviceId": address, "activeFaultCodes": [],
                          "inactiveFaultCodes": [], "pendingFaultCodes": []}

        if "activeFaultCodes" in new_as_dict:

            ac_fc = values[new_as_dict["activeFaultCodes"]]

            if ac_fc:

                ac_fc_array = ac_fc.split("|")

                for fc in ac_fc_array:

                    if fc:

                        fcObj = {}

                        fcArr = fc.split("~")

                        for fcVal in fcArr:
                            fcObj[fcVal.split(":")[0]] = fcVal.split(":")[1]

                        conv_eq_fc_obj["activeFaultCodes"].append(fcObj)

        if "inactiveFaultCodes" in new_as_dict:

            inac_fc = values[new_as_dict["inactiveFaultCodes"]]

            if inac_fc:

                ac_fc_array = inac_fc.split("|")

                for fc in ac_fc_array:

                    if fc:

                        fcObj = {}

                        fcArr = fc.split("~")

                        for fcVal in fcArr:
                            fcObj[fcVal.split(":")[0]] = fcVal.split(":")[1]

                        conv_eq_fc_obj["inactiveFaultCodes"].append(fcObj)

        if "pendingFaultCodes" in new_as_dict:

            pen_fc = values[new_as_dict["pendingFaultCodes"]]

            if pen_fc:

                ac_fc_array = pen_fc.split("|")

                for fc in ac_fc_array:

                    if fc:

                        fcObj = {}

                        fcArr = fc.split("~")

                        for fcVal in fcArr:
                            fcObj[fcVal.split(":")[0]] = fcVal.split(":")[1]

                        conv_eq_fc_obj["pendingFaultCodes"].append(fcObj)

        sample["convertedEquipmentFaultCodes"].append(conv_eq_fc_obj)

        json_sample_head["samples"].append(sample)

        print("Updated JSON Sample Head:", json_sample_head)

    return json_sample_head


def lambda_handler(lambda_event, context):
    print("Event:", json.dumps(lambda_event))

    bucket_name = lambda_event['Records'][0]['s3']['bucket']['name']
    file_key = lambda_event['Records'][0]['s3']['object']['key']

    print("Bucket:", bucket_name)
    print("File Key:", file_key)

    file_key = file_key.replace("%3A", ":")

    print("New FileKey:", file_key)

    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_file = obj['Body'].read().decode('utf-8').splitlines(True)

    print("Csv File:", csv_file)

    ngdi_json_template = json.loads(os.environ["NGDIBody"])

    print("NGDI Template", ngdi_json_template)

    ss_row = False
    seen_ss = False

    csv_rows = []

    # Get all the rows from the CSV
    for row in csv.reader(csv_file, delimiter=','):

        new_row = list(filter(lambda x: x != '', row))

        if new_row:
            csv_rows.append(row)

    print("CSV Rows: ", csv_rows)

    ss_rows = []
    as_rows = []

    for row in csv_rows:

        if (not ss_row) and (not seen_ss):

            if "messageFormatVersion" in row:

                print("messageFormatVersion row:", row)

                ngdi_json_template["messageFormatVersion"] = row[1] if row[1] else None

            elif "dataEncryptionSchemeId" in row:

                print("dataEncryptionSchemeId row:", row)

                ngdi_json_template["dataEncryptionSchemeId"] = row[1] if row[1] else None

            elif "telematicsBoxId" in row:

                print("telematicsBoxId row:", row)

                ngdi_json_template["telematicsDeviceId"] = row[1] if row[1] else None

            elif "componentSerialNumber" in row:

                print("componentSerialNumber row:", row)

                ngdi_json_template["componentSerialNumber"] = row[1] if row[1] else None

            elif "dataSamplingConfigId" in row:

                print("dataSamplingConfigId row:", row)

                ngdi_json_template["dataSamplingConfigId"] = row[1] if row[1] else None

            elif "ssDateTimestamp" in row:

                print("ssDateTimestamp Header row:", row)

                ss_row = True
                seen_ss = True
                ss_rows.append(row)

        elif ss_row:

            if "asDateTimestamp" in row:
                print("Missing the Single Sample Values. ERROR")

                return

            print("ssDateTimestamp Values row:", row)

            ss_rows.append(row)

            ss_row = False

        else:

            as_rows.append(row)

    if not seen_ss or (not as_rows):
        print("Missing the Single Sample Values or the All Samples Values. Both are MANDATORY. ERROR!")
        return

    print("NGDI Template after main metadata addition --->", ngdi_json_template)

    ss_dict = {}
    as_dict = {}
    ss_converted_prot_header = ""
    as_converted_prot_header = ""
    ss_raw_prot_header = ""
    as_raw_prot_header = ""

    count = 0

    ss_headers = ss_rows[0]
    print("SS Headers:", ss_headers)

    as_headers = as_rows[0]
    print("AS Headers:", as_headers)

    ss_converted_device_parameters = []
    seen_ss_dev_params = False
    seen_ss_j1939_params = False
    seen_ss_raw_params = False

    as_converted_device_parameters = []
    seen_as_dev_params = False
    seen_as_j1939_params = False
    seen_as_raw_params = False

    mandatory_ss_parameter_list = MandatoryParameters["ss"].split(",") if "ss" in MandatoryParameters else None

    print("Mandatory SS Headers: ", mandatory_ss_parameter_list)

    if mandatory_ss_parameter_list:

        for ss_param in mandatory_ss_parameter_list:

            if ss_param not in ss_headers:
                print("Some of the SS mandatory headers are missing! Headers ",
                      mandatory_ss_parameter_list, " are mandatory! ERROR")

                return

    for head in ss_headers:

        if 'device' in head.lower() and 'converted' in head.lower():
            seen_ss_dev_params = True

        if 'j1939' in head.lower() and 'raw' in head.lower():
            ss_raw_prot_header = head
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

    print("SS_DICT:", ss_dict)
    print("SS Device Headers:", ss_converted_device_parameters)

    count = 0

    mandatory_as_parameter_list = MandatoryParameters["as"].split(",") if "as" in MandatoryParameters else None

    print("Mandatory AS Headers: ", mandatory_as_parameter_list)

    if mandatory_as_parameter_list:

        for as_param in mandatory_as_parameter_list:

            if as_param not in as_headers:
                print("Some of the AS mandatory headers are missing! Headers ",
                      mandatory_as_parameter_list, " are mandatory! ERROR")

                return

    for head in as_headers:

        if 'j1939' in head.lower() and 'converted' in head.lower():
            as_converted_prot_header = head
            seen_as_j1939_params = True

        if 'j1939' in head.lower() and 'raw' in head.lower():
            as_raw_prot_header = head
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

    print("AS_DICT:", as_dict)
    print("AS Device Parameters:", as_converted_device_parameters)

    del as_rows[0]

    print(" ")
    print("<xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx---Handling Single Samples---xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>")
    print(" ")

    ngdi_json_template = process_ss(ss_rows, ss_dict, ngdi_json_template, ss_converted_prot_header,
                                    ss_raw_prot_header, ss_converted_device_parameters)

    print("NGDI JSON Template after SS handling:", ngdi_json_template)

    print(" ")
    print("<xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx---Handled Single Samples---xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>")
    print(" ")

    print(" ")
    print("<xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx---Handling All Samples---xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>")
    print(" ")

    ngdi_json_template = process_as(as_rows, as_dict, ngdi_json_template, as_converted_prot_header,
                                    as_raw_prot_header, as_converted_device_parameters)

    print("NGDI JSON Template after AS handling:", ngdi_json_template)

    print(" ")
    print("<xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx---Handled All Samples---xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>")
    print(" ")

    print("Posting file to S3...")

    filename = file_key

    print("Filename: ", filename)

    if '-' in filename.split('_')[-1]:
        date = filename.split('_')[-1][:10].replace('-', '')
    else:
        date = filename.split('_')[-1][:8]

    store_file_response = s3_client.put_object(Bucket=cp_post_bucket,
                                    Key="ConvertedFiles/" + ngdi_json_template['componentSerialNumber'] + '/' +
                                        ngdi_json_template["telematicsDeviceId"] + '/' + date[:4] + '/' + date[4:6] +
                                        '/' + date[6:8] + '/' + filename.split('.csv')[0] + '.json',
                                    Body=json.dumps(ngdi_json_template).encode(),
                                    Metadata={'j1939type': 'FC'})

    print("Store File Response:", store_file_response)


'''
Main Method For Local Testing
'''
if __name__ == "__main__":
    event = ""
    context = ""
    lambda_handler(event, context)
