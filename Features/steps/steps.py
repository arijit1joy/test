from behave import *
import requests
import json
import boto3
from botocore.exceptions import ClientError
import os
import time

device_name = os.environ['device_name']
Upload_Url = os.environ['Upload_Url']
Assoc_Url = os.environ['Assoc_Url']
Query_Url = os.environ['CurrentProdutCommonAPI']

use_step_matcher('re')

print("************Entered Steps Script************")
print("\n")


def del_from_sql_table():
    del_var = requests.post(url=Query_Url, json=Del_query_json)


# ---------------------------------------- Pre-Run Actions -------------------------------------------------

print("Setting up . . .")
print("\n")

# Get Latest Authentication Token

authtoken = os.environ["AuthTokenURL"]
authtoken_response = requests.get(authtoken)

token = authtoken_response.json()["authToken"]
print("Current Authentication Token: " + token)

# Get Common DB Info

db_url = os.environ["CurrentProdutCommonAPI"]
db_headers = {"Content-Type": "application/json", "x-api-key": os.environ["CurrentProdutCommonAPIKey"]}

# Get WL Information

with open("WhitelistInput.json", "r") as jsonFile:
    data = jsonFile.read()

payload_wl = json.loads(data)

print("Payload_wl is: ", payload_wl)

device_id_wl = payload_wl['attributes']['imei']

print("Whitelist Device ID:" + device_id_wl)

whitelistUrl = os.environ["WhitelistUrl"]

whitelistUrl = whitelistUrl.replace("deviceID={devID}&authToken={authToken}",
                                    "authToken=" + token)

# Get Reg Information

with open("RegistrationInput.json", "r") as jsonFile:
    data = jsonFile.read()

payload_reg = json.loads(data)
device_id_reg = payload_reg['registrationInfo']['boxID']

print("Registration Device ID:" + device_id_reg)

registrationUrl = os.environ["RegistrationUrl"]

registrationUrl = registrationUrl.replace("authToken={authToken}", "&authToken=" + token)

# Get Cofig Association information

with open('cspec.json', 'r') as cspecjsonFile:
    t_data = cspecjsonFile.read()

test_cspec_json = json.loads(t_data)

key = 'raw/SCBDDT.json'
s3 = boto3.resource('s3')
bucket = s3.Bucket('da-edge-732927748536-datacontentspec-files-dev')

iot_client = boto3.client('iot')

Assoc_json = {
    "DataConfigSpecFileName": "SCBDDT.json",
    "template_id": "DataContentTemplate5",
    "deviceID": "AssocBDDdevice",
    "oem_flag": "Edge"
}

Query_json = {

    "method": "get",
    "query": "SELECT JOB_ID from CS_PARTNER_VAL.EDG_DEVICE_ACTIVITY WHERE JOB_ID = %(jobid)s;",
    "input": {
        "Params": [
            {
                "jobid": ""
            }
        ]
    }

}

Query2_json = {

    "method": "get",
    "query": "SELECT JOB_ID from CS_PARTNER_VAL.EDG_DEVICE_ACTIVITY WHERE ASSOCIATED_CONFIG_FILE_NAME = %(dev_name)s;",
    "input": {
        "Params": [
            {
                "dev_name": "SCBDDT.json"
            }
        ]
    }

}

Del_query_json = {
    "method": "post",
    "query": "DELETE from CS_PARTNER_VAL.EDG_DEVICE_ACTIVITY WHERE ASSOCIATED_CONFIG_FILE_NAME = %(dev_name)s;",
    "input": {
        "Params": [
            {
                "dev_name": "SCBDDT.json"
            }
        ]
    }
}

Assoc_body = None
responseCode = None
responseBody = None

# ---------------------------------------- Run BDD Steps -------------------------------------------------

print("Device Whitelisting BDD Beginning...")


@given("Edge device whitelist api")
def step_impl(context):
    print("Whitelist URL: ", whitelistUrl)


@when(u'Api is invoked with proper json body')
def step_impl(context):
    print("Making call to url: ", whitelistUrl)

    print("The payload is: ", payload_wl)

    global responseCode
    global responseBody

    reg_response = requests.post(url=whitelistUrl, data=json.dumps(payload_wl))
    responseCode = reg_response.status_code
    responseBody = reg_response.json()


@then(u'Check if status code is "200" or not -wl')
def step_impl(context):
    print("Whitelist Status Code is: " + str(responseCode))

    if responseCode == 200:
        context.execute_steps(u'''
            when "edsn" and "awsIotThingArn" are in body
            and "edsn" is deviceID 
            and "awsIotThingArn" is deviceID thing arn
            then device whitelisting is successful
        ''')
    else:
        print("Whitelist response code was not 200")
        raise AssertionError


@when(u'"edsn" and "awsIotThingArn" are in body')
def step_impl(context):
    print("Whitelist Response Body: " + str(responseBody))

    if "edsn" in responseBody and "awsIotThingArn" in responseBody:
        print("'edsn' and 'awsIotThingArn' are present in whitelist response")
    else:
        print("Improper format of the whitelist response body")
        raise AssertionError


@when(u'"edsn" is deviceID')
def step_impl(context):
    if responseBody["edsn"] == device_id_wl:
        print("'edsn' is present and is as expected")
    else:
        print("Error Verifying the returned ESN")
        raise AssertionError


@when(u'"awsIotThingArn" is deviceID thing arn')
def step_impl(context):
    if responseBody["awsIotThingArn"] == "arn:aws:iot:us-east-1:732927748536:thing/" + device_id_wl:
        print("'awsIotThingArn' is present and is as expected")
    else:
        print("Error Verifying the returned whitelist Thing ARN")
        raise AssertionError


@then(u'device whitelisting is successful')
def step_impl(context):
    print("Device Whitelisting was Successful!")
    delete_thing_record(device_id_wl)


print("\n")
print("EBU Device Registration BDD Beginning...")


@given("a whitelisted device with the registration API")
def step_impl(context):
    print("Registration URL: " + registrationUrl)


@when("Api is invoked with proper json body for that device")
def step_impl(context):
    print("Making call to url: " + registrationUrl)

    global responseCode
    global responseBody

    reg_response = requests.post(url=registrationUrl, json=payload_reg)
    responseCode = reg_response.status_code
    responseBody = reg_response.json()


@then(u'Check if status code is "200" or not -reg')
def step_impl(context):
    print("Registration Status Code is: " + str(responseCode))

    if responseCode == 200:
        context.execute_steps(u'''
            then check if entry is in DB in proper time
            then device registration is successful
        ''')
    else:
        print("Registration response code was not 200")
        raise AssertionError


@then(u'check if entry is in DB in proper time')
def step_impl(context):
    print("Sleeping for 30 Seconds to allow DB Update")
    time.sleep(30)

    print("Verifying Device Is Properly Registered")

    if get_db_record_ebu_reg(device_id_reg):

        print("Successfully verified the device record")

    else:

        print("Error Verifying the device record")
        raise AssertionError


@then(u'device registration is successful')
def step_impl(context):
    print("Device Registration was Successful!")

    unregister_device(device_id_reg)


@given(u'A valid config spec')
def step_impl(context):
    print(test_cspec_json)


@given(u'Upload URL')
def step_impl(context):
    print(Upload_Url)


@when(u'upload URL is invoked')
def step_impl(context):
    if (check_for_config_spec(bucket, key) == 1):
        resp_code = update_in_s3(Upload_Url, test_cspec_json)
        if resp_code == 200:
            print('Success.')
        else:
            raise AssertionError


@then(u'File should be in S3')
def step_impl(context):
    if (check_for_config_spec(bucket, key) == 1):
        print('Success.')
    else:
        raise AssertionError


del_from_sql_table()


@given(u'Config spec is in S3')
def step_impl(context):
    print(test_cspec_json)


@given(u'Endpoint URL')
def step_impl(context):
    print(Assoc_Url)


@given(u'Device ID')
def step_impl(context):
    print(device_name)


@when(u'Association URL is invoked')
def step_impl(context):
    global Assoc_body
    Assoc_response = requests.post(url=Assoc_Url, json=Assoc_json)
    Assoc_body = json.loads(json.dumps(Assoc_response.json()))
    if Assoc_body:
        print(Assoc_body)
        print("Sleeping for 30 Seconds to allow DB Update")
        time.sleep(30)
    else:
        raise AssertionError


@then(u'A new Job ID for that Device in IOT and EDG_DEVICE_ACTIVITY table')
def step_impl(context):
    print(Assoc_body)
    if str(Assoc_body) == str({'message': 'Endpoint request timed out'}):
        query2_resp = requests.post(url=Query_Url, json=Query2_json)
        resp_query2 = query2_resp.json()
        print(resp_query2)
        if resp_query2:
            print('Job ID created in EDG_DEVICE_ACTIVITY table.')
            cancelJob(resp_query2[0]['job_id'])
        else:
            raise AssertionError
    elif Assoc_body['StatusCode'] == '200' and Assoc_body['jobid']:
        print('New Job ID for device in IOT has been created')
        Query_json['input']['Params'][0]['jobid'] = Assoc_body['jobid']
        query_resp = requests.post(url=Query_Url, json=Query_json)
        query_jobid_json = query_resp.json()
        if query_jobid_json:
            print('Entry for Job ID is created in the EDG_DEVICE_ACTIVITY table.')
            cancelJob(Assoc_body['jobid'])
        else:
            raise AssertionError
    else:
        raise AssertionError


# ---------------------------------------- Non-Behave Methods-------------------------------------------------

def delete_thing_record(device_id):
    pay_load = {
        "method": "post",
        "query": "DELETE FROM CS_PARTNER_VAL.EDG_DEVICE_INFORMATION where DEVICE_ID = %(devId)s;",
        "input": {
            "Params": [
                {
                    "devId": device_id
                }
            ]
        }
    }

    print("Whitelist Record Deletion Payload: " + str(pay_load))

    response = requests.post(url=db_url, json=pay_load, headers=db_headers)

    if response.text == "Successfully performed operation":
        print("Successfully deleted the device record")
    else:
        print("Error deleting the device record")
        print(response.text)
        raise AssertionError


def get_db_record_ebu_reg(device_id):
    pay_load = {
        "method": "get",
        "query": "SELECT DEVICE_TYPE, DEVICE_OWNER, ENGINE_SERIAL_NUMBER, ENGINE_FK, FIRMWARE_VERSION, "
                 "CAL_ID, SW_ID, TELEMATICS_VERSION, VIN, EQUIP_ID, CUST_REF, MAKE, MODEL, UNIT_NUMBER, CD, CSU, "
                 "CA,FO, DEVICE_STATUS FROM CS_PARTNER_VAL.EDG_DEVICE_INFORMATION WHERE DEVICE_ID = %(devId)s;",
        "input": {
            "Params": [
                {
                    "devId": device_id
                }
            ]
        }
    }

    print("Registration Record Verification Payload: " + str(pay_load))

    response = requests.post(url=db_url, json=pay_load, headers=db_headers)

    response_json = response.json()[0]

    print("Registration Record Verification Response: " + str(response_json))

    if response_json["device_type"] and response_json["device_owner"] \
            and response_json["engine_serial_number"] and response_json["engine_fk"] \
            and response_json["firmware_version"] and response_json["cal_id"] and response_json["sw_id"] \
            and response_json["telematics_version"] and response_json["vin"] and response_json["equip_id"] \
            and response_json["cust_ref"] and response_json["make"] and response_json["model"] \
            and response_json["unit_number"] and response_json["cd"] and response_json["csu"] \
            and response_json["ca"] and response_json["fo"] and response_json["device_status"]:
        return True
    else:
        return False


def unregister_device(device_id):
    pay_load = {
        "method": "post",
        "query": "UPDATE CS_PARTNER_VAL.EDG_DEVICE_INFORMATION SET FIRMWARE_VERSION = NULL, DEVICE_STATUS = 'WHITELISTED', DEVICE_TYPE = NULL, CA = NULL, CD = NULL, CSU = NULL, FO = NULL, CAL_ID = NULL, VIN = NULL, ENGINE_FK = NULL, DEVICE_OWNER = NULL, ENGINE_SERIAL_NUMBER = NULL, MAKE = NULL, DOM = NULL, MODEL = NULL, UNIT_NUMBER = NULL, SW_ID = NULL, TELEMATICS_VERSION = NULL, EQUIP_ID = NULL, CUST_REF = NULL where DEVICE_ID = %(devId)s;",
        "input": {
            "Params": [
                {
                    "devId": device_id
                }
            ]
        }
    }

    print("Record registration removal Payload, to be executed after 30 seconds: " + str(pay_load))

    time.sleep(30)

    response = requests.post(url=db_url, json=pay_load)

    if response.text == "Successfully performed operation":
        print("Successfully updated the device record")
    else:
        print("Error deleting the device record")
        print(response.text)
        raise AssertionError

    # re_wl_url = whitelistUrl.replace(device_id_wl, device_id_reg)

    # re_wl_payload = payload_wl

    # re_wl_payload["attributes"]["imei"] = device_id_reg

    # requests.post(url=re_wl_url, json=re_wl_payload)

    # print("Making call to url: " + re_wl_url + " for re-whitelisting with the payload: ", re_wl_payload)


def check_for_config_spec(bucket, key):
    objs = list(bucket.objects.filter(Prefix=key))
    if objs[0].key == key:
        print("Exists!")
        return (1)
    else:
        print("Doesn't exist")
        return (0)


def update_in_s3(up_url, up_json):
    up_response = requests.post(url=up_url, json=up_json)
    T_responseCode = up_response.status_code
    return T_responseCode


def cancelJob(jobID):
    cancellation_resp = iot_client.cancel_job(jobId="cdf-" + jobID)
    if cancellation_resp['jobId']:
        print('Successfully deleted job ID: ' + jobID)
    else:
        raise AssertionError
