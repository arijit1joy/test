import sys, os

os.environ['AWS_DEFAULT_REGION'] = 'region'
os.environ['LoggingLevel'] = 'debug'
os.environ['KafkaApiVersionTuple'] = '(3,6,0)'





os.environ['APPLICATION_ENVIRONMENT'] = 'dev'
os.environ['APPLICATION_NAME'] = 'da-edge-j1939'
os.environ['AuditTrailQueueUrl'] = 'https://sqs.us-east-1.amazonaws.com/732927748536/da-edge-common-lib-AuditTrailerQueue-dev'
os.environ['CDPTJ1939Header'] = '{"Content-Type":"application/json"}'
os.environ['CDPTJ1939PostURL'] = 'intentionally-disabling-https://mt5afmpt32.execute-api.us-east-1.amazonaws.com/Test/ngdi'
os.environ['CPPTGetDevInfo'] = '{"method": "get", "query": "SELECT DEVICE_TYPE, DEVICE_OWNER, DOM FROM da_edge_olympus.DEVICE_INFORMATION WHERE DEVICE_ID = %(devId)s;", "input": {"Params": [{"devId": "devId"}]}}'
os.environ['CPPostBucket'] = 'edge-j1939-dev'
os.environ['DataQualityLambda'] = 'da-EDGE-Olympus-BL-DataQuality-dev'
os.environ['EBUSpecifier'] = 'onhighway'
os.environ['EDGEDBCommonAPI_ARN'] = 'arn:aws:lambda:us-east-1:732927748536:function:EdgeCommonAPI-dev'
os.environ['EDGEDBReader_ARN'] = 'arn:aws:lambda:us-east-1:732927748536:function:da-edge-common-lib-EDGEDBReader-dev'
os.environ['EndpointBucket'] = 'da-edge-j1939-endpoint-bucket-dev'
os.environ['EndpointFile'] = 'EndpointJson.json'
os.environ['Environment'] = 'dev'
os.environ['JSONFormat'] = 'SDK'
os.environ['MaxAttempts'] = '3'
os.environ['PSBUSpecifier'] = 'psbu'
os.environ['PTJ1939Header'] = '{"Content-Type": "application/json", "Prefer": "param=single-object", "x-api-key": ""}'
os.environ['PTJ1939PostURL'] = 'intentionally-disabling-https://preventech-test.aws.cummins.com/Test/ngdi'
os.environ['PTxAPIKey'] = 'pt_xapi_key'
os.environ['PowerGenValue'] = 'powerGen'
os.environ['ProcessDataQuality'] = 'no'
os.environ['QueueUrl'] = 'https://sqs.us-east-1.amazonaws.com/732927748536/da-edge-j1939-CPPTPosterQueue-dev'
os.environ['RedisSecretName'] = '/da-EDGE-Olympus/elasticache/edge-rw'
os.environ['Region'] = 'us-east-1'
os.environ['UseEndpointBucket'] = 'Y'
os.environ['cd_device_owners'] = '{"EBU": "EBU", "TATA": "TATA", "TataMotors":"TataMotors", "Cosmos":"Cosmos"}'
os.environ['delivery_stream_name'] = 'da-edge-aai-service-metadata-stream'
os.environ['environment'] = 'dev'
os.environ['j1939_stream_arn'] = 'arn:aws:kinesis:us-east-1:884739393817:stream/J1939Events'
os.environ['mapTspFromOwner'] = '{"EBU": "Cummins", "PSBU": "Cummins" ,"TATA": "India_Edge", "Navistar": "Navistar", "N2": "Navistar" ,"Paccar":"Paccar", "Siemens":"Siemens", "TataMotors":"Accolade", "Cosmos":"COSPA"}'
os.environ['metaWriteQueueUrl'] = 'https://sqs.us-east-1.amazonaws.com/732927748536/da-edge-common-lib-DatalogMetadata-dev'
os.environ['mskClusterArn'] = 'arn:aws:kafka:us-east-1:732927748536:cluster/cda-data-hub-msk-kafka-cluster-v2-dev/42004362-0711-443f-85ad-8017c9d52711-13'
os.environ['mskSecretArn'] = 'arn:aws:secretsmanager:us-east-1:732927748536:secret:AmazonMSK_cda-data-hub-msk-cluster-authkey-dev-wh0wa9'
os.environ['pcc2_j1939_stream_arn'] = 'arn:aws:kinesis:us-east-1:590183752454:stream/psbu-poc-acumen-j1939-telemetry-inputstream'
os.environ['pcc2_region'] = 'us-east-1'
os.environ['pcc2_role_arn'] = 'arn:aws:iam::590183752454:role/PccInputStreamRole'
os.environ['pcc_region'] = 'us-east-1'
os.environ['pcc_role_arn'] = 'arn:aws:iam::884739393817:role/EdgeKinesisProducerRole'
os.environ['psbu_device_owner'] = '{"PSBU": "PSBU", "Siemens":"Siemens"}'
os.environ['ptTopicInfo'] = '{"topicName": "nimbuspt-j1939-{j1939_type}", "bu":"PSBU","file_type":"JSON"}'
os.environ['publishKafka'] = 'true'
os.environ['region'] = 'us-east-1'
os.environ['topicName'] = 'j1939-pt-topic'




sys.path.append('..')


from PosterLambda import lambda_handler