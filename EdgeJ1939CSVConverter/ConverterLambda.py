import csv
import boto3
import json
import os


def processSS(ssRows, ssMap, jsonSampleHead, ssConvertedProtHeader, ssRawProtHeader):
    values = ssRows[1]

    protocol = ""

    networkId = ""

    deviceId = ""

    parameters = {}

    convertedHeader = ssConvertedProtHeader.split("~")

    if (len(convertedHeader) >= 3):
        protocol = convertedHeader[1]

        networkId = convertedHeader[2]

        deviceId = convertedHeader[3]

    if ("telematicsPartnerName" in ssMap):
        jsonSampleHead["telematicsPartnerName"] = values[ssMap["telematicsPartnerName"]]

        ssMap.pop("telematicsPartnerName")

    if ("customerReference" in ssMap):
        jsonSampleHead["customerReference"] = values[ssMap["customerReference"]]

        ssMap.pop("customerReference")

    if ("equipmentId" in ssMap):
        jsonSampleHead["equipmentId"] = values[ssMap["equipmentId"]]

        ssMap.pop("equipmentId")

    if ("vin" in ssMap):
        jsonSampleHead["vin"] = values[ssMap["vin"]]

        ssMap.pop("vin")

    sample = {}

    if ("ssDateTimestamp" in ssMap):
        sample["dateTimestamp"] = values[ssMap["ssDateTimestamp"]]

        ssMap.pop("ssDateTimestamp")

    sample["convertedDeviceParameters"] = {}

    sample["convertedEquipmentParameters"] = []

    if ("messageID" in ssMap):
        sample["convertedDeviceParameters"]["messageID"] = values[ssMap["messageID"]]

        ssMap.pop("messageID")

    convEqObj = {}

    convEqObj["protocol"] = protocol
    convEqObj["networkId"] = networkId
    convEqObj["deviceId"] = deviceId

    print(ssMap)

    for param in ssMap:

        if (param):
            parameters[param] = values[ssMap[param]]
            # if (param.lower() == "1635"):
            #
            #     parameters[param] = []
            #
            #     val = values[ssMap[param]].split(" ")
            #
            #     parameters[param].append(val[0])
            #
            #     val.remove(val[0])
            #
            #     parameters[param].append(" ".join(val).strip())
            #
            # else:
            #     parameters[param] = values[ssMap[param]]

    convEqObj["parameters"] = parameters

    sample["convertedEquipmentParameters"].append(convEqObj)

    jsonSampleHead["samples"].append(sample)

    return jsonSampleHead


def processAS(asRows, asMap, jsonSampleHead, asConvertedProtHeader, asRawProtHeader):
    jsonSampleHead["numberOfSamples"] = len(asRows)

    for values in asRows:

        protocol = ""

        networkId = ""

        deviceId = ""

        parameters = {}

        convertedHeader = asConvertedProtHeader.split("~")

        if (len(convertedHeader) >= 3):
            protocol = convertedHeader[1]

            networkId = convertedHeader[2]

            deviceId = convertedHeader[3]

        sample = {}

        if ("asDateTimestamp" in asMap):
            sample["dateTimestamp"] = values[asMap["asDateTimestamp"]]
            asDateTimestamp = asMap.pop("asDateTimestamp")

        sample["convertedDeviceParameters"] = {}

        sample["rawEquipmentParameters"] = []

        sample["convertedEquipmentParameters"] = []

        sample["convertedEquipmentFaultCodes"] = []

        if ("Latitude" in asMap):
            sample["convertedDeviceParameters"]["Latitude"] = values[asMap["Latitude"]]
            Latitude = asMap.pop("Latitude")

        if ("Longitude" in asMap):
            sample["convertedDeviceParameters"]["Longitude"] = values[asMap["Longitude"]]
            Longitude = asMap.pop("Longitude")

        if ("Altitude" in asMap):
            sample["convertedDeviceParameters"]["Altitude"] = values[asMap["Altitude"]]
            Altitude = asMap.pop("Altitude")

        if ("Direction_Heading" in asMap):
            sample["convertedDeviceParameters"]["Direction_Heading"] = values[asMap["Direction_Heading"]]
            Direction_Heading = asMap.pop("Direction_Heading")

        if ("Vehicle_Distance" in asMap):
            sample["convertedDeviceParameters"]["Vehicle_Distance"] = values[asMap["Vehicle_Distance"]]
            Vehicle_Distance = asMap.pop("Vehicle_Distance")

        if ("Location_Text_Description" in asMap):
            sample["convertedDeviceParameters"]["Location_Text_Description"] = values[
                asMap["Location_Text_Description"]]
            Location_Text_Description = asMap.pop("Location_Text_Description")

        if ("GPS_Vehicle_Speed" in asMap):
            sample["convertedDeviceParameters"]["GPS_Vehicle_Speed"] = values[asMap["GPS_Vehicle_Speed"]]
            GPS_Vehicle_Speed = asMap.pop("GPS_Vehicle_Speed")

        convEqObj = {}

        convEqObj["protocol"] = protocol
        convEqObj["networkId"] = networkId
        convEqObj["deviceId"] = deviceId

        for param in asMap:

            if (param):

                if (param != "activeFaultCodes" and param != "inactiveFaultCodes" and param != "pendingFaultCodes"):
                    parameters[param] = values[asMap[param]]

        convEqObj["parameters"] = parameters

        sample["convertedEquipmentParameters"].append(convEqObj)

        jsonSampleHead["samples"].append(sample)

        asMap["Latitude"] = Latitude
        asMap["Longitude"] = Longitude
        asMap["Altitude"] = Altitude
        asMap["Direction_Heading"] = Direction_Heading
        asMap["Vehicle_Distance"] = Vehicle_Distance
        asMap["Location_Text_Description"] = Location_Text_Description
        asMap["GPS_Vehicle_Speed"] = GPS_Vehicle_Speed
        asMap["asDateTimestamp"] = asDateTimestamp

        convEqFCObj = {}

        convEqFCObj["protocol"] = protocol
        convEqFCObj["networkId"] = networkId
        convEqFCObj["deviceId"] = deviceId
        convEqFCObj["activeFaultCodes"] = []
        convEqFCObj["inactiveFaultCodes"] = []
        convEqFCObj["pendingFaultCodes"] = []

        if ("activeFaultCodes" in asMap):

            acFC = values[asMap["activeFaultCodes"]]

            print(acFC)

            if (acFC != ""):

                acFcArray = acFC.split("|")

                for fc in acFcArray:

                    if (fc):

                        fcObj = {}

                        fcArr = fc.split("~")

                        for fcVal in fcArr:
                            fcObj[fcVal.split(":")[0]] = fcVal.split(":")[1]

                        convEqFCObj["activeFaultCodes"].append(fcObj)

        if ("inactiveFaultCodes" in asMap):

            acFC = values[asMap["inactiveFaultCodes"]]

            if (acFC != ""):

                acFcArray = acFC.split("|")

                for fc in acFcArray:

                    if (fc):

                        fcObj = {}

                        fcArr = fc.split("~")

                        for fcVal in fcArr:
                            fcObj[fcVal.split(":")[0]] = fcVal.split(":")[1]

                        convEqFCObj["inactiveFaultCodes"].append(fcObj)

        if ("pendingFaultCodes" in asMap):

            acFC = values[asMap["pendingFaultCodes"]]

            if (acFC != ""):

                acFcArray = acFC.split("|")

                for fc in acFcArray:

                    if (fc):

                        fcObj = {}

                        fcArr = fc.split("~")

                        for fcVal in fcArr:
                            fcObj[fcVal.split(":")[0]] = fcVal.split(":")[1]

                        convEqFCObj["pendingFaultCodes"].append(fcObj)

        sample["convertedEquipmentFaultCodes"].append(convEqFCObj)

    return jsonSampleHead


def lambda_handler(event, context):
    s3 = boto3.client('s3')

    s3resource = boto3.resource('s3')

    CPPostBucket = os.environ["CPPostBucket"]

    print(json.dumps(event))

    bucketName = event['Records'][0]['s3']['bucket']['name']
    fileKey = event['Records'][0]['s3']['object']['key']

    print(bucketName)
    print(fileKey)

    obj = s3.get_object(Bucket=bucketName, Key=fileKey)

    csv_file = obj['Body'].read().decode('utf-8').splitlines(True)

    # Get CSV File
    # csv_file = open('CD-specificExampleDataFile Rev_2019-08-09.csv', 'r'))

    # Get Json File Format
    jsonSampleHead = json.loads(os.environ["NGDIBody"])

    print(jsonSampleHead)

    ssRow = False
    seenSS = False

    csvRows = []

    ssRows = []
    asRows = []

    # Get all the rows from the CSV
    for row in csv.reader(csv_file, delimiter=','):

        newRow = list(filter(lambda x: x != '', row))

        if (newRow):
            csvRows.append(row)

    for row in csvRows:

        if (ssRow == False and seenSS == False):

            if ("messageFormatVersion" in row):

                jsonSampleHead["messageFormatVersion"] = row[1]

            elif ("dataEncryptionSchemeId" in row):

                jsonSampleHead["dataEncryptionSchemeId"] = row[1]

            elif ("telematicsBoxId" in row):

                jsonSampleHead["telematicsDeviceId"] = row[1]

            elif ("componentSerialNumber" in row):

                jsonSampleHead["componentSerialNumber"] = row[1]

            elif ("dataSamplingConfigId" in row):

                jsonSampleHead["dataSamplingConfigId"] = row[1]

            elif ("ssDateTimestamp" in row):

                ssRow = True

                seenSS = True

                ssRows.append(row)

        elif (ssRow == True):

            ssRows.append(row)

            ssRow = False

        else:

            asRows.append(row)

    ssMap = {}
    asMap = {}
    ssConvertedProtHeader = ""
    asConvertedProtHeader = ""
    ssRawProtHeader = ""
    asRawProtHeader = ""

    count = 0

    for head in ssRows[0]:

        if ('j1939' in head.lower() and 'converted' in head.lower()):
            ssConvertedProtHeader = head

        if ('j1939' in head.lower() and 'raw' in head.lower()):
            ssRawProtHeader = head

        if ('~' in head):

            count = count + 1

            continue
        else:

            ssMap[head] = count

            count = count + 1

    count = 0

    for head in asRows[0]:

        if ('j1939' in head.lower() and 'converted' in head.lower()):
            asConvertedProtHeader = head

        if ('j1939' in head.lower() and 'raw' in head.lower()):
            asRawProtHeader = head

        if ('~' in head):
            count = count + 1

            continue
        else:
            asMap[head] = count

            count = count + 1

    print(ssMap)

    print(asMap)

    asRows.remove(asRows[0])

    jsonSampleHead = processSS(ssRows, ssMap, jsonSampleHead, ssConvertedProtHeader, ssRawProtHeader)

    jsonSampleHead = processAS(asRows, asMap, jsonSampleHead, asConvertedProtHeader, asRawProtHeader)

    print(jsonSampleHead)

    print("Posting file to S3...")

    filename = fileKey.split("/")[1]

    date = filename.split('_')[4].split('T')[0].split('-')
    
    print('Date: ', date)

    s3object = s3resource.Object(CPPostBucket, jsonSampleHead['componentSerialNumber'] + '/' + jsonSampleHead["telematicsDeviceId"] + '/' + date[0] + '/' + date[1] + '/' + date[2] + '/' + filename.split('.csv')[0] + '.json')

    s3object.put(
        Body=(bytes(json.dumps(jsonSampleHead).encode('UTF-8')))
    )


'''
Main Method For Local Testing
'''
if __name__ == "__main__":
    event = ""
    context = ""
    lambda_handler(event, context)