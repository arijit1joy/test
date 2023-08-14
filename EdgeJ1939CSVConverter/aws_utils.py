import json
import boto3


def _fetch_bdd_esn():
    ssm_client = boto3.client('ssm')
    bdd_esn = ssm_client.get_parameter(Name='da-edge-j1939-bdd-esn-list', WithDecryption=False)
    bdd_esn = json.loads(bdd_esn['Parameter']['Value'])
    bdd_esn = bdd_esn['esn']
    return bdd_esn

BDD_ESN = _fetch_bdd_esn()