import json
import requests
import os
import boto3

edgeCommonAPIURL = os.environ['edgeCommonAPIURL']
currentProductAPIKey = os.environ['CurrentProdutCommonAPIKey']
endpointFile = os.environ["EndpointFile"]
ptURL = ""
CPPostBucket = os.environ["CPPostBucket"]
EndpointBucket = os.environ["EndpointBucket"]
JSONFormat = os.environ["JSONFormat"]


def lambda_handler(event, context):
    # json_file = open("EDGE_352953080329158_64000002_SC123_20190820045303_F2BA (3).json", "r")

    s3 = boto3.resource('s3')

    print(json.dumps(event))

    bucketName = event['Records'][0]['s3']['bucket']['name']
    fileKey = event['Records'][0]['s3']['object']['key']

    print(bucketName)
    print(fileKey)

    obj = s3.Object(bucketName, fileKey)

    file_content = obj.get()['Body'].read().decode('utf-8')

    print(file_content)

    jsonBody = json.loads(file_content)

    print(jsonBody)

    devId = jsonBody["thingName"] if "thingName" in jsonBody else None

    headers = {'Content-Type': 'application/json', 'x-api-key': currentProductAPIKey}

    payload = json.loads(os.environ['CPPTGetDevInfo'])

    print(payload)

    payload["input"]["Params"][0]["devId"] = devId

    response = requests.post(url=edgeCommonAPIURL, json=payload, headers=headers)

    dbResult = response.json()

    print(dbResult)

    if (dbResult):
        print("NotEmptyResult")

        print(dbResult[0])

        if ("device_type" in dbResult[0]):

            if (dbResult[0]["device_type"] is not None):

                print(dbResult[0]["device_type"].lower())

                if (dbResult[0]["device_type"].lower() == "nimbus" or dbResult[0]["device_type"].lower() == "psbu"):

                    if ("DOM" in dbResult[0]):

                        if (dbResult[0]["DOM"] is not None):

                            if (dbResult[0]["DOM"].lower() != "powergen"):
                                print("The PT value is: " + dbResult[0]["DOM"])

                                bucket = s3.Bucket(EndpointBucket)

                                bucketValues = list(bucket.objects.filter(Prefix=endpointFile))

                                if (len(bucketValues) > 0 and bucketValues[0].key == endpointFile):

                                    print("EndpointFile Exists!")

                                    obj = s3.Object(EndpointBucket, endpointFile)

                                    print(obj)

                                    ptURLRaw = obj.get()['Body'].read().decode('utf-8')

                                    ptURLBody = json.loads(ptURLRaw)

                                    print(ptURLBody)

                                    environment = os.environ["Environment"]

                                    ptURL = ptURLBody[environment]["PT"]

                                    if ("NGDIPosting1" in ptURL):

                                        print("From endpoint file for PT:" + str(ptURL))

                                        headers = ptURL["NGDIPosting1"]["headerJson"]
                                        ptURL = ptURL["NGDIPosting1"]["endpoint"]

                                        print("Posting to the PT Pipeline through NGDIPosting1 endpoint: " + str(ptURL))

                                        arrayBody = []

                                        arrayBody.append(jsonBody)

                                        print(arrayBody)

                                        response = requests.post(url=ptURL, json=arrayBody, headers=headers)

                                        print(response.json())
                                    else:
                                        print("Endpoint for PT posting is missing! Aborting")
                                        return

                                # else:
                                #     print("Creating EndpointFile")
                                #
                                #     endpointJson = json.loads(os.environ["EndpointJson"])
                                #
                                #     s3object = s3.Object(CPPostBucket, endpointFile)
                                #
                                #     s3object.put(Body=(bytes(json.dumps(endpointJson).encode('UTF-8'))))
                                #
                                #     obj = s3.Object(CPPostBucket, endpointFile)
                                #
                                #     print(obj)
                                #
                                #     ptURLRaw = obj.get()['Body'].read().decode('utf-8')
                                #
                                #     ptURLBody = json.loads(ptURLRaw)
                                #
                                #     print(ptURLBody)
                                #
                                #     environment = os.environ["Environment"]
                                #
                                #     ptURL = ptURLBody[environment]["PT"]
                                #
                                #     print("Posting to the PT Pipeline through " + ptURL)
                                #
                                #     response = requests.post(url=ptURL, json=jsonBody, headers=headers)
                                #
                                #     print(response.json())
                                else:
                                    print("Endpoint File Does not exist! Aborting")
                                    return
                            else:

                                print("This is a Nimbus device, but it is PCC, cannot send to PT")

                                return

                        else:

                            print("There is no boxApplication Value! Cannot determine if it is PT or PCC. Aborting!")

                            return

                    else:

                        print("There is no boxApplication Value! Cannot determine if it is PT or PCC. Aborting!")

                        return

                elif (dbResult[0]["device_type"].lower() == "onhighway"):

                    if (JSONFormat == "SDK"):
                        filename = fileKey.split("/")[2]

                        print(filename)

                        s3object = s3.Object(CPPostBucket,
                                             'NGDI/' + devId + '/' +
                                             filename)

                        print(s3object)

                        s3object.put(
                            Body=(bytes(json.dumps(jsonBody).encode('UTF-8'))),
                            Metadata={'J1939Type': 'FC'}
                        )
                    elif (JSONFormat == "NGDI"):
                        bucket = s3.Bucket(EndpointBucket)

                        bucketValues = list(bucket.objects.filter(Prefix=endpointFile))

                        if (len(bucketValues) > 0 and bucketValues[0].key == endpointFile):

                            print("EndpointFile Exists! ")

                            obj = s3.Object(EndpointBucket, endpointFile)

                            print(obj)

                            ptURLRaw = obj.get()['Body'].read().decode('utf-8')

                            ptURLBody = json.loads(ptURLRaw)

                            print(ptURLBody)

                            environment = os.environ["Environment"]

                            ptURL = ptURLBody[environment]["PT"]

                            if ("NGDIPosting2" in ptURL):

                                print(ptURL)

                                headers = ptURL["NGDIPosting2"]["headerJson"]
                                ptURL = ptURL["NGDIPosting2"]["endpoint"]

                                print("Posting to the PT Pipeline through NGDIPosting2 endpoint: " + ptURL)

                                arrayBody = []

                                arrayBody.append(jsonBody)

                                print(arrayBody)

                                response = requests.post(url=ptURL, json=arrayBody, headers=headers)

                                print(response.json())
                            else:
                                print("Endpoint for CD posting to PT is missing! Aborting")
                                return

                        # else:
                        #     print("Creating EndpointFile")
                        #
                        #     endpointJson = json.loads(os.environ["EndpointJson"])
                        #
                        #     s3object = s3.Object(CPPostBucket, endpointFile)
                        #
                        #     s3object.put(Body=(bytes(json.dumps(endpointJson).encode('UTF-8'))))
                        #
                        #     obj = s3.Object(CPPostBucket, endpointFile)
                        #
                        #     print(obj)
                        #
                        #     ptURLRaw = obj.get()['Body'].read().decode('utf-8')
                        #
                        #     ptURLBody = json.loads(ptURLRaw)
                        #
                        #     print(ptURLBody)
                        #
                        #     environment = os.environ["Environment"]
                        #
                        #     ptURL = ptURLBody[environment]["PT"]
                        #
                        #     print("Posting to the PT Pipeline through " + ptURL)
                        #
                        #     response = requests.post(url=ptURL, json=jsonBody, headers=headers)
                        #
                        #     print(response.json())

                        else:
                            print("Endpoint File Does not exist! Aborting")
                            return
                    else:
                        print(
                            "Cannot determine if the CD pipeline is up and running. Please use only 'SDK' or 'NGDI' as the values for the JSONFormat environment variable. Aborting!")

                        return
                else:

                    print(
                        "The device Type value is not edge(ebu)/nimbus(psbu)! Please verify the CP DB device record for deviceID " + devId + ". Aborting!")

                    return

            else:

                print(
                    "The device Type value is None! Please verify the CP DB device record for deviceID " + devId + ". Aborting!")

                return

        else:

            print(
                "The device Type value is wrong! Please verify the CP DB device record for deviceID " + devId + ". Aborting!")

            return

    else:
        print("Empty Result! DeviceID does not exist in Edge Device Information CP Database")


# Local Test Main

if __name__ == '__main__':
    lambda_handler("", "")