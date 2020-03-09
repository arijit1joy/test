import json
import cd_sdk_hb as hbsdk
# import cd_sdk_fc as fcsdk
# import sObject as s3_object
import os
# import pandas as pd
# from pandas.io.json import json_normalize
import boto3
import requests
# import uuid
# import base64

# metadata = {}
spn_bucket = os.getenv('spn_parameter_json_object')
spn_bucket_key = os.getenv('spn_parameter_json_object_key')
auth_token_url = os.getenv('auth_token_url')
cd_url = os.getenv('cd_url')
converted_equip_params_var = os.getenv('converted_equip_params')
converted_device_params_var = os.getenv('converted_device_params')
converted_equip_fc_var = os.getenv('converted_equip_fc')
class_arg_map = os.getenv('class_arg_map')
time_stamp_param = os.getenv('time_stamp_param')
active_fault_code_indicator = os.getenv('active_fault_code_indicator')
param_indicator = os.getenv('param_indicator')

s3_client = boto3.client('s3')

spn_file_stream = s3_client.get_object(Bucket=spn_bucket, Key=spn_bucket_key)
spn_file = spn_file_stream['Body'].read()
spn_file_json = json.loads(spn_file)

# print(spnparams)

# def metadatainformation(data):
#     metadata['messageFormatVersion'] = data['messageFormatVersion'][0] if "messageFormatVersion" in data else None
#     metadata['telematicsPartnerName'] = data['telematicsPartnerName'][0] if "telematicsPartnerName" in data else None
#     metadata['customerReference'] = data['customerReference'][0] if "customerReference" in data else None
#     metadata['componentSerialNumber'] = data['componentSerialNumber'][0] if "componentSerialNumber" in data else None
#     metadata['equipmentId'] = data['equipmentId'][0] if "equipmentId" in data else None
#     metadata['vin'] = data['vin'][0] if "vin" in data else None
#     metadata['telematicsDeviceId'] = data['telematicsDeviceId'][0] if "telematicsDeviceId" in data else None
#     metadata['dataSamplingConfigId'] = data['dataSamplingConfigId'][0] if "dataSamplingConfigId" in data else None
#     metadata['dataEncryptionSchemeId'] = data['dataEncryptionSchemeId'][0] if "dataEncryptionSchemeId" in data else None


def get_metadata_info(j1939_file):

    print("Removing 'samples' from the j1939 file...")

    try:
        del j1939_file["samples"]

        print("j1939 file with no samples:", j1939_file)

        return j1939_file

    except Exception as e:

        print("An exception occurred while retrieving metadata:", e)

        return  False


def get_snapshot_data(params, time_stamp, address):

    print("Getting snapshot data for the parameter list:", params)

    snapshot_data = []

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

    print('Auth Token ---------------->', auth_token_info)
    print('Faults   ------------------> ', data)
    print('cd_url   ------------------> ', cd_url)
    r = requests.post(url=cd_url + auth_token_info, data=data)
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

    metadata = get_metadata_info(j1939_file)

    if metadata:

        print("Retrieving Samples from the file:", j1939_file)

        samples = j1939_file["samples"] if "samples" in j1939_file else None

        print("File Samples:", samples)

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


# def processfaults(currentrow, index, fc_or_hb):
#
#     # print('-----------------------------------------  CURRENT ROW  ----------------------------------------------')
#     # print(index, currentrow[index]['dateTimestamp'])
#     # print(index, currentrow[index]['convertedEquipmentParameters'][0]['parameters'])
#     # print(index, currentrow[index]['convertedEquipmentFaultCodes'][0]['activeFaultCodes'])
#     # print('------------------------------------------------------------------------------------------------------')
#     # print('activeFaultCodes - length ', len(currentrow[index]['convertedEquipmentFaultCodes'][0]['activeFaultCodes']))
#
#     conv_eq_fault_codes = currentrow[index]['convertedEquipmentFaultCodes'][0] \
#         if "convertedEquipmentFaultCodes" in currentrow[index] and currentrow[index]['convertedEquipmentFaultCodes'] \
#         else []
#
#     active_fc_exist = False
#     inactive_fc_exist = False
#
#     # Checking if (in)active fault codes exist
#     if conv_eq_fault_codes and 'activeFaultCodes' in conv_eq_fault_codes:
#
#         active_fc_exist = True
#         active_fc = conv_eq_fault_codes['activeFaultCodes']
#
#     if conv_eq_fault_codes and 'activeFaultCodes' in conv_eq_fault_codes:
#         inactive_fc_exist = True
#
#     sdkjson(currentrow[index]['dateTimestamp'],
#             currentrow[index]['convertedDeviceParameters.Latitude'] if 'convertedDeviceParameters.Latitude' in currentrow[index] else None,
#             currentrow[index]['convertedDeviceParameters.Longitude'] if 'convertedDeviceParameters.Longitude' in currentrow[index] else None,
#             currentrow[index]['convertedDeviceParameters.Altitude'] if 'convertedDeviceParameters.Altitude' in currentrow[index] else None,
#             currentrow[index]['convertedDeviceParameters.Direction_Heading'] if 'convertedDeviceParameters.Direction_Heading' in currentrow[index] else None,
#             currentrow[index]['convertedDeviceParameters.Vehicle_Distance'] if 'convertedDeviceParameters.Vehicle_Distance' in currentrow[index] else None,
#             currentrow[index]['convertedDeviceParameters.Location_Text_Description'] if 'convertedDeviceParameters.Location_Text_Description' in currentrow[index] else None,
#             currentrow[index]['convertedDeviceParameters.GPS_Vehicle_Speed'] if 'convertedDeviceParameters.GPS_Vehicle_Speed' in currentrow[index] else None,
#             conv_eq_fault_codes['parameters'],
#             conv_eq_fault_codes['deviceId'],
#             conv_eq_fault_codes['activeFaultCodes'] if active_fc_exist
#             else conv_eq_fault_codes['activeFaultCodes'] if inactive_fc_exist
#             else conv_eq_fault_codes['inactiveFaultCodes']
#             if conv_eq_fault_codes and 'inactiveFaultCodes' in conv_eq_fault_codes
#             else None,
#             0, fc_or_hb)
#     elif (InactiveFaultsLen > 0):
#         sdkjson(currentrow[index]['dateTimestamp'],
#                 currentrow[index]['convertedDeviceParameters.Latitude'],
#                 currentrow[index]['convertedDeviceParameters.Longitude'],
#                 currentrow[index]['convertedDeviceParameters.Altitude'],
#                 currentrow[index]['convertedDeviceParameters.Direction_Heading'],
#                 currentrow[index]['convertedDeviceParameters.Vehicle_Distance'],
#                 currentrow[index]['convertedDeviceParameters.Location_Text_Description'],
#                 currentrow[index]['convertedDeviceParameters.GPS_Vehicle_Speed'],
#                 currentrow[index]['convertedEquipmentParameters'][0]['parameters'],
#                 currentrow[index]['convertedEquipmentParameters'][0]['deviceId'],
#                 currentrow[index]['convertedEquipmentFaultCodes'][0]['inactiveFaultCodes'],
#                 1, fc_or_hb)
#
#
# def sdkjson(datetime, latitude, longitude, altitiude, direction, distance, location, speed, parameters,
#             paramsourceaddres, faults, active, fcHb):
#     if active == 0:
#         # print('Snapshot_DateTimestamp : ', datetime,
#         #       ' Latitude : ', latitude,
#         #       ' Longitude : ', longitude,
#         #       ' Altitude : ', altitiude,
#         #       ' Direction_Heading : ', direction,
#         #       ' Vehicle_Distance : ', distance,
#         #       ' Location_Text_Description : ',location,
#         #       ' GPS_Vehicle_Speed : ', speed,
#         #       ' Snapshot_Parameters : ', parameters,
#         #       ' Active Faults :', faults)
#         finalmessage(datetime, latitude, longitude, altitiude, direction, distance, location, speed, parameters,
#                      paramsourceaddres, faults, 0, fcHb)
#     elif active == 1:
#         # print('Snapshot_DateTimestamp : ', datetime,
#         #       ' Latitude : ', latitude,
#         #       ' Longitude : ', longitude,
#         #       ' Altitude : ', altitiude,
#         #       ' Direction_Heading : ', direction,
#         #       ' Vehicle_Distance : ', distance,
#         #       ' Location_Text_Description : ',location,
#         #       ' GPS_Vehicle_Speed : ', speed,
#         #       ' Snapshot_Parameters : ', parameters,
#         #       ' InActive Faults :', faults)
#         finalmessage(datetime, latitude, longitude, altitiude, direction, distance, location, speed, parameters,
#                      paramsourceaddres, faults, 1, fcHb)
#
#
# def finalmessage(datetime, latitude, longitude, altitiude, direction, distance, location, speed, parameters,
#                  paramsourceaddres, faults, activeorinactive, FCHB):
#     parameter = []
#     snapshots = json.loads(json.dumps(parameters))
#     for key, value in snapshots.items():
#         # print('Name : ', key, ' Value : ', value, ' Parameter_Source_Address : ', paramsourceaddres)
#         parameter.append({"Name": spnparams[key], "Value": value, "Parameter_Source_Address": paramsourceaddres})
#
#     snapshotdata = []
#     snapshot = cdsdk.Snapshot(datetime, json.loads(json.dumps(parameter)))
#     snapshotdata.append(snapshot.__dict__)
#     # print(snapshotdata)
#     # snapshot.snapshot_date_timestamp = datetime
#     # snapshot.parameter = json.loads(json.dumps(parameter))
#
#     equimentgroups = []
#     equimentgroup = cdsdk.CustomerEquipmentGroup(equimentgroups)
#
#     spnfmi = json.loads(json.dumps(faults))
#     # print(spnfmi)
#     for spns in spnfmi:
#         spn = ''
#         fmi = ''
#         count = ''
#         for key, value in spns.items():
#             if key == 'spn':
#                 spn = value
#             if key == 'fmi':
#                 fmi = value
#             if key == 'count':
#                 count = value
#         # print('spn : ', spn)
#         # print('fmi : ', fmi)
#         # print('count : ', count)
#         # print('FCorHB : ', FCorHB)
#         sdk = cdsdk.Sdkclass(
#             '1.0',
#             FCHB,
#             metadata['telematicsDeviceId'],
#             get_a_uuid(),
#             metadata['telematicsPartnerName'],
#             metadata['customerReference'],
#             metadata['equipmentId'],
#             metadata['componentSerialNumber'],
#             metadata['vin'],
#             datetime,
#             datetime,
#             activeorinactive,
#             '',
#             paramsourceaddres,
#             spn,
#             fmi,
#             count,
#             latitude,
#             longitude,
#             altitiude,
#             direction,
#             distance,
#             location,
#             speed,
#             '',
#             'Cummins',
#             'Model',
#             'Unit Number',
#             '',
#             equimentgroup.__dict__,
#             snapshotdata)
#
#         jsonObj = json.dumps(sdk.__dict__)
#         post_cd_message(jsonObj)
#
#
# def get_a_uuid():
#     uuid_info = str(uuid.uuid4())
#     return uuid_info
#
#
# def serialize(obj):
#     if isinstance(obj, date):
#         serial = obj.isoformat()
#         return serial
#
#
# def set_pandas_options() -> None:
#
#     pd.options.display.max_columns = 1000
#     pd.options.display.max_rows = 1000
#     pd.options.display.max_colwidth = 199
#     pd.options.display.width = None
#     # pd.options.display.precision = 2  # set as needed


def lambda_handler(event, context):

    print("Lambda Event:")

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