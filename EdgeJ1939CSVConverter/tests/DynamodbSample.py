from pprint import pprint
import boto3
from botocore.exceptions import ClientError
import re

TABLE_NAME='fault_codes'

def create_active_fault_codes_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

    table = dynamodb.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {
                'AttributeName': 'esn',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'esn',
                'AttributeType': 'N'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )

    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=TABLE_NAME)
    assert table.table_status == 'ACTIVE'

    return table

def put_active_fault_codes(esn, ts, ac_fc, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table(TABLE_NAME)
    print("put_active_fault_codes esn:",esn)
    response = table.put_item(
       Item={
            'esn': esn,
            'timestamp': ts,
            'fcs': ac_fc
        }
    )
    print('put_active_fault_codes :response:',response)
    return response

def get_active_fault_codes_from_dynamodb(esn, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table(TABLE_NAME)
    print('get_active_fault_codes :esn:',esn)

    try:
        response = table.get_item(Key={'esn': esn})
        print('response :',response)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        #return response['Item']
        return response

def generate_spn_fmi_fc_obj( actual_ac_fc, conc_eq_fc_obj):
    fc_obj = {}
    fc_arr = actual_ac_fc.split("~")
    for fc_val in fc_arr:
        fc_obj[fc_val.split(":")[0]] = fc_val.split(":")[1]
    print(f"fc_obj : {fc_obj}")
    conc_eq_fc_obj['activeFaultCodes'].append(fc_obj)
    print(f"conc_eq_fc_obj : {conc_eq_fc_obj}")

def check_active_fault_codes_timestamp(db_esn_ac_fcs,timestamp):
    if db_esn_ac_fcs is None:
        print("new esn")
        return True
    else:
        print("existing esn")
        db_timestamp = db_esn_ac_fcs.get('timestamp')
        print("db_timestamp :",db_timestamp)
        print("timestamp :", timestamp)
        if db_timestamp < timestamp:
            print("db_timestamp < timestamp")
            return True
        else:
            print("False")
            return False

def generate_active_fault_codes(esn, ac_fc, conc_eq_fc_obj, db_esn_ac_fcs,timestamp, dynamodb=None):
    print(f"ESN : {esn}")
    print(f"AC_FC : {ac_fc}")
    print(f"conc_eq_fc_obj : {conc_eq_fc_obj}")
    print(f"fault_codes from database for esn : {db_esn_ac_fcs}")
    print(f"timestamp from database for esn : {timestamp}")

    spn_fmi_combo_list = re.split("\|", ac_fc)
    if spn_fmi_combo_list and not spn_fmi_combo_list[-1].strip():
        spn_fmi_combo_list.pop()

    print(f"spn_fmi_combo_list : {spn_fmi_combo_list}")
    sorted_spn_fmi_combo_list = sorted(spn_fmi_combo_list)
    print(f"sorted_spn_fmi_combo_list : {sorted_spn_fmi_combo_list}")
    insert_spn_fmi_fcs_db = {}
    update_spn_fmi_fcs_db = {}

    for actual_ac_fc in sorted_spn_fmi_combo_list:
        print(f"actual_ac_fc : {actual_ac_fc}")
        db_ac_fc = actual_ac_fc.rsplit('~', 1)[0]
        print(f"db_ac_fc : {db_ac_fc}")
        ac_fc_cnt=actual_ac_fc.split('~', 2)[2].split(":")[1]
        print("ac_fc_cnt :",ac_fc_cnt)


        if db_esn_ac_fcs == None:
            print(f"new esn found does not exist in database : {esn}")
            insert_spn_fmi_fcs_db[db_ac_fc] = ac_fc_cnt
            generate_spn_fmi_fc_obj(actual_ac_fc, conc_eq_fc_obj)
        else:
            existing_fc_db = db_esn_ac_fcs.get('fcs')
            print(f"existing fault_codes from database for esn: {existing_fc_db}")
            ac_fc_db_cnt=existing_fc_db.get(db_ac_fc)
            print("ac_fc_db_cnt:",ac_fc_db_cnt)
            #checking if the fault_codes contains  in the database
            if existing_fc_db.get(db_ac_fc) == None:
                print(f"fault_code not found in database for exiting esn : {actual_ac_fc}")
                update_spn_fmi_fcs_db[db_ac_fc] = ac_fc_cnt
                generate_spn_fmi_fc_obj(actual_ac_fc, conc_eq_fc_obj)
            elif int(ac_fc_cnt) != int(ac_fc_db_cnt):
                print(f"fault_code found in database for exiting esn and count not matching: {actual_ac_fc}")
                #update_spn_fmi_fcs_db[db_ac_fc] = ac_fc_cnt
                insert_spn_fmi_fcs_db[db_ac_fc] = ac_fc_cnt
                generate_spn_fmi_fc_obj(actual_ac_fc, conc_eq_fc_obj)
            else:
                print(f"duplicate fault_code for exiting esn : {actual_ac_fc}")

    if len(insert_spn_fmi_fcs_db) > 0:
        print(f"insert fault_code into database for new esn : {insert_spn_fmi_fcs_db}")
        print(f"insert esn into database for new esn : {esn}")
        print(f"insert timestamp into database for new esn : {timestamp}")
        put_active_fault_codes(esn,timestamp,insert_spn_fmi_fcs_db,dynamodb)
        print("fault_codes inserted successfully into the database for new esn:",insert_spn_fmi_fcs_db)


    if len(update_spn_fmi_fcs_db) > 0:
        print(f"insert fault_code into database for existing esn : {update_spn_fmi_fcs_db}")
        print(f"insert esn into database for existing  esn : {esn}")
        print(f"insert timestamp into database for existing  esn : {timestamp}")

        existing_spn_fmi_fcs = db_esn_ac_fcs.get('fcs')
        for key, value in update_spn_fmi_fcs_db.items():
            existing_spn_fmi_fcs[key] = value
        put_active_fault_codes(esn, timestamp, existing_spn_fmi_fcs, dynamodb)
        print("new fault_codes inserted successfully into the database for existing esn:", esn)



    print(f'conc_eq_fc_obj is:{conc_eq_fc_obj}')
    return conc_eq_fc_obj



def process_as(as_rows, as_dict, ngdi_json_template, as_converted_prot_header, as_converted_device_parameters):
    old_as_dict = as_dict
    json_sample_head = ngdi_json_template
    json_sample_head["numberOfSamples"] = len(as_rows)
    converted_prot_header = as_converted_prot_header.split("~")
    esn = ngdi_json_template["componentSerialNumber"]
    timestamp = ""

    print(f"process_as as_rows:{as_rows}")
    print(f"process_as as_dict:{as_dict}")
    print(f"process_as ngdi_json_template:{ngdi_json_template}")
    print(f"process_as json_sample_head:{json_sample_head}")
    print(f"process_as as_converted_prot_header:{as_converted_prot_header}")
    print(f"process_as as_converted_device_parameters:{as_converted_device_parameters}")
    print(f"process_as esn:{esn}")

    try:
        protocol = converted_prot_header[1]
        network_id = converted_prot_header[2]
        address = converted_prot_header[3]
    except IndexError as e:
        print(f"An exception occurred while trying to retrieve the AS protocols network_id and Address: {e}")
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
                    timestamp = values[new_as_dict[param]]
                elif param != "activeFaultCodes" and param != "inactiveFaultCodes" and param != "pendingFaultCodes":
                    parameters[param] = values[new_as_dict[param]]

        conv_eq_obj["parameters"] = parameters
        sample["convertedEquipmentParameters"].append(conv_eq_obj)
        conv_eq_fc_obj = {"protocol": protocol, "networkId": network_id, "deviceId": address, "activeFaultCodes": [],
                          "inactiveFaultCodes": [], "pendingFaultCodes": []}

        # new code start
        print(f"ESN is {esn}")
        print(f"TimeStamp is {timestamp}")
        active_fc_from_db = get_active_fault_codes_from_dynamodb(esn)
        db_esn_ac_fcs = None
        if 'Item' in active_fc_from_db:
            db_esn_ac_fcs = active_fc_from_db['Item']

        db_timestamp_check = check_active_fault_codes_timestamp(db_esn_ac_fcs, timestamp)
        if db_timestamp_check:
            if "activeFaultCodes" in new_as_dict:
                ac_fc = values[new_as_dict["activeFaultCodes"]]
                generate_active_fault_codes(esn, ac_fc, conv_eq_fc_obj, db_esn_ac_fcs, timestamp)
        else:
            print(f"db_timestamp is greater than timestamp")
        print(f"conv_eq_fc_obj with activeFaultCodes {conv_eq_fc_obj}")
        # new code end

        '''if "activeFaultCodes" in new_as_dict:
            ac_fc = values[new_as_dict["activeFaultCodes"]]

            if ac_fc:
                ac_fc_array = ac_fc.split("|")
                for fc in ac_fc_array:
                    if fc:
                        fc_obj = {}
                        fc_arr = fc.split("~")
                        for fc_val in fc_arr:
                            fc_obj[fc_val.split(":")[0]] = fc_val.split(":")[1]

                        conv_eq_fc_obj["activeFaultCodes"].append(fc_obj)'''

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
        print(f"Process AS JSON Sample Head: {json_sample_head}")

    return json_sample_head


if __name__ == '__main__':
    movie_table = create_active_fault_codes_table()
    print("Table status:", movie_table.table_status)