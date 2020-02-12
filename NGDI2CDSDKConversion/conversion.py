import json
import CDSDKObject as cdsdk
import sObject as s3_object
import os
import pandas as pd
from pandas.io.json import json_normalize
import boto3
import requests
import uuid
import base64

metadata = {}
spn_bucket = os.getenv('spn_parameter_json_object')
spn_bucket_key = os.getenv('spn_parameter_json_object_key')
auth_token_url = os.getenv('auth_token_url')
cd_url = os.getenv('cd_url')
FCorHB = ''

PARAMS = {}
req = requests.get(url=auth_token_url, params=PARAMS)
auth_token = json.loads(req.text)
# print('AuthToken  -- >', auth_token['authToken'])
auth_token_info = auth_token['authToken']

s3 = boto3.client('s3')
object = s3.get_object(Bucket=spn_bucket, Key=spn_bucket_key)
serializedObject = object['Body'].read()
spnparams = json.loads(serializedObject)


# print(spnparams)

def metadatainformation(data):
    metadata['messageFormatVersion'] = data['messageFormatVersion'][0]
    metadata['telematicsPartnerName'] = data['telematicsPartnerName'][0]
    metadata['customerReference'] = data['customerReference'][0]
    metadata['componentSerialNumber'] = data['componentSerialNumber'][0]
    metadata['equipmentId'] = data['equipmentId'][0]
    metadata['vin'] = data['vin'][0]
    metadata['telematicsDeviceId'] = data['telematicsDeviceId'][0]
    metadata['dataSamplingConfigId'] = data['dataSamplingConfigId'][0]
    metadata['dataEncryptionSchemeId'] = data['dataEncryptionSchemeId'][0]


def process(bucket, key):
    set_pandas_options()
    response = s3.head_object(Bucket=bucket, Key=key)
    FCorHB = response['Metadata']['j1939type']
    print("FC or HB : " + response['Metadata']['j1939type'])
    if (not FCorHB):
        FCorHB = "HB"

    obj = s3.get_object(Bucket=bucket, Key=key)
    serializedObj = obj['Body'].read()
    Data = json.loads(serializedObj)
    # print(pd.DataFrame(Data))
    df = pd.DataFrame(Data)
    metadatainformation(df)
    works_data_samples = json_normalize(data=df['samples'])
    dataframe_samples = pd.DataFrame(works_data_samples)
    rowIndex = 0
    for index, row in dataframe_samples.iterrows():
        currentrow = {}
        if rowIndex > 0:
            currentrow[index] = row
            processfaults(currentrow, index, FCorHB)
        rowIndex = rowIndex + 1


def processfaults(currentrow, index, fcHb):
    # print('-----------------------------------------  CURRENT ROW  ------------------------------------------------------------------')
    # print(index, currentrow[index]['dateTimestamp'])
    # print(index, currentrow[index]['convertedEquipmentParameters'][0]['parameters'])
    # print(index, currentrow[index]['convertedEquipmentFaultCodes'][0]['activeFaultCodes'])
    # print('---------------------------------------------------------------------------------------------------------------------------')
    # print('activeFaultCodes - length ', len(currentrow[index]['convertedEquipmentFaultCodes'][0]['activeFaultCodes']))
    activeFaultsLen = len(currentrow[index]['convertedEquipmentFaultCodes'][0]['activeFaultCodes'])
    InactiveFaultsLen = len(currentrow[index]['convertedEquipmentFaultCodes'][0]['inactiveFaultCodes'])
    if (activeFaultsLen > 0):
        sdkjson(currentrow[index]['dateTimestamp'],
                currentrow[index]['convertedDeviceParameters.Latitude'],
                currentrow[index]['convertedDeviceParameters.Longitude'],
                currentrow[index]['convertedDeviceParameters.Altitude'],
                currentrow[index]['convertedDeviceParameters.Direction_Heading'],
                currentrow[index]['convertedDeviceParameters.Vehicle_Distance'],
                currentrow[index]['convertedDeviceParameters.Location_Text_Description'],
                currentrow[index]['convertedDeviceParameters.GPS_Vehicle_Speed'],
                currentrow[index]['convertedEquipmentParameters'][0]['parameters'],
                currentrow[index]['convertedEquipmentParameters'][0]['deviceId'],
                currentrow[index]['convertedEquipmentFaultCodes'][0]['activeFaultCodes'],
                0, fcHb)
    elif (InactiveFaultsLen > 0):
        sdkjson(currentrow[index]['dateTimestamp'],
                currentrow[index]['convertedDeviceParameters.Latitude'],
                currentrow[index]['convertedDeviceParameters.Longitude'],
                currentrow[index]['convertedDeviceParameters.Altitude'],
                currentrow[index]['convertedDeviceParameters.Direction_Heading'],
                currentrow[index]['convertedDeviceParameters.Vehicle_Distance'],
                currentrow[index]['convertedDeviceParameters.Location_Text_Description'],
                currentrow[index]['convertedDeviceParameters.GPS_Vehicle_Speed'],
                currentrow[index]['convertedEquipmentParameters'][0]['parameters'],
                currentrow[index]['convertedEquipmentParameters'][0]['deviceId'],
                currentrow[index]['convertedEquipmentFaultCodes'][0]['inactiveFaultCodes'],
                1, fcHb)


def sdkjson(datetime, latitude, longitude, altitiude, direction, distance, location, speed, parameters,
            paramsourceaddres, faults, active, fcHb):
    if active == 0:
        # print('Snapshot_DateTimestamp : ', datetime,
        #       ' Latitude : ', latitude,
        #       ' Longitude : ', longitude,
        #       ' Altitude : ', altitiude,
        #       ' Direction_Heading : ', direction,
        #       ' Vehicle_Distance : ', distance,
        #       ' Location_Text_Description : ',location,
        #       ' GPS_Vehicle_Speed : ', speed,
        #       ' Snapshot_Parameters : ', parameters,
        #       ' Active Faults :', faults)
        finalmessage(datetime, latitude, longitude, altitiude, direction, distance, location, speed, parameters,
                     paramsourceaddres, faults, 0, fcHb)
    elif active == 1:
        # print('Snapshot_DateTimestamp : ', datetime,
        #       ' Latitude : ', latitude,
        #       ' Longitude : ', longitude,
        #       ' Altitude : ', altitiude,
        #       ' Direction_Heading : ', direction,
        #       ' Vehicle_Distance : ', distance,
        #       ' Location_Text_Description : ',location,
        #       ' GPS_Vehicle_Speed : ', speed,
        #       ' Snapshot_Parameters : ', parameters,
        #       ' InActive Faults :', faults)
        finalmessage(datetime, latitude, longitude, altitiude, direction, distance, location, speed, parameters,
                     paramsourceaddres, faults, 1, fcHb)


def finalmessage(datetime, latitude, longitude, altitiude, direction, distance, location, speed, parameters,
                 paramsourceaddres, faults, activeorinactive, FCHB):
    parameter = []
    snapshots = json.loads(json.dumps(parameters))
    for key, value in snapshots.items():
        # print('Name : ', key, ' Value : ', value, ' Parameter_Source_Address : ', paramsourceaddres)
        parameter.append({"Name": spnparams[key], "Value": value, "Parameter_Source_Address": paramsourceaddres})

    snapshotdata = []
    snapshot = cdsdk.Snapshot(datetime, json.loads(json.dumps(parameter)))
    snapshotdata.append(snapshot.__dict__)
    # print(snapshotdata)
    # snapshot.snapshot_date_timestamp = datetime
    # snapshot.parameter = json.loads(json.dumps(parameter))

    equimentgroups = []
    equimentgroup = cdsdk.CustomerEquipmentGroup(equimentgroups)

    spnfmi = json.loads(json.dumps(faults))
    # print(spnfmi)
    for spns in spnfmi:
        spn = ''
        fmi = ''
        count = ''
        for key, value in spns.items():
            if key == 'spn':
                spn = value
            if key == 'fmi':
                fmi = value
            if key == 'count':
                count = value
        # print('spn : ', spn)
        # print('fmi : ', fmi)
        # print('count : ', count)
        # print('FCorHB : ', FCorHB)
        sdk = cdsdk.Sdkclass(
            '1.0',
            FCHB,
            metadata['telematicsDeviceId'],
            get_a_uuid(),
            metadata['telematicsPartnerName'],
            metadata['customerReference'],
            metadata['equipmentId'],
            metadata['componentSerialNumber'],
            metadata['vin'],
            datetime,
            datetime,
            activeorinactive,
            '',
            paramsourceaddres,
            spn,
            fmi,
            count,
            latitude,
            longitude,
            altitiude,
            direction,
            distance,
            location,
            speed,
            '',
            'Cummins',
            'Model',
            'Unit Number',
            '',
            equimentgroup.__dict__,
            snapshotdata)

        jsonObj = json.dumps(sdk.__dict__)
        postcdmessage(jsonObj)


def get_a_uuid():
    uuid_info = str(uuid.uuid4())
    return uuid_info


def postcdmessage(data):
    print('Auth Token ---------------->', auth_token_info)
    print('Faults   ------------------> ', data)
    print('cd_url   ------------------> ', cd_url)
    r = requests.post(url=cd_url + auth_token_info, data=data)
    response = r.text
    print('response ------------> ', response)


def serialize(obj):
    if isinstance(obj, date):
        serial = obj.isoformat()
        return serial


def set_pandas_options() -> None:
    pd.options.display.max_columns = 1000
    pd.options.display.max_rows = 1000
    pd.options.display.max_colwidth = 199
    pd.options.display.width = None
    # pd.options.display.precision = 2  # set as needed


def lambda_handler(event, context):
    print("NGDI JSON Object:", event['Records'][0]['s3']['object']['key'])
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    process(bucket, key)


'''
Main Method For Local Testing
'''
if __name__ == "__main__":
    event = ""
    context = ""
    process()