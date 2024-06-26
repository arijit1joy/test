import io
import sys
import json
import unittest
from unittest.mock import ANY, patch, MagicMock

sys.path.append("../")

from tests.cda_module_mock_context import CDAModuleMockingContext

with  CDAModuleMockingContext(sys) as cda_module_mock_context, patch.dict("os.environ", {
    "ptTopicInfo":'{"topicName": "nimbuspt_j1939-{j1939_type}", "bu":"PSBU","file_type":"JSON"}',
    "LoggingLevel": "debug",
    "PTxAPIKey": "testKey",
    "Region": "us-east-1",
    "region": "us-east-1",
    'EDGEDBReader_ARN': 'arn:::12345',
    "edgeCommonAPIURL": "testurl",
    "publishKafka": "true",
    "CDPTJ1939PostURL": "testurl",
    "CDPTJ1939Header": "testheader",
    "RedisSecretName": "testsecret",
    "EndpointFile": "EndpointFile",
    "CPPostBucket": "CPPostBucket",
    "EndpointBucket": "EndpointBucket",
    "JSONFormat": "JSONFormat",
    "PSBUSpecifier": "psbu",
    "EBUSpecifier": "ebu",
    "UseEndpointBucket": "UseEndpointBucket",
    "PTJ1939PostURL": "PTJ1939PostURL",
    "PTJ1939Header": "PTJ1939Header",
    "PowerGenValue": "PowerGenValue",
    "mapTspFromOwner": json.dumps({"PSBU": True}),
    "ProcessDataQuality": "YES",
    "DataQualityLambda": "DataQualityLambda",
    "MaxAttempts": "2",
    "EngineStatOverride":"EngineStat_9",
    "LoadFactorOverride":"LoadFactor_9",
    "EngineStatSc":"SC8091",
    "LoadFactorSc":"SC8093",
    "pcc_role_arn": "arn",
    "j1939_stream_arn": "arn",
    "pcc_region": "us-east-1",
    "pcc2_role_arn": "test",
    "pcc2_j1939_stream_arn": "test",
    "pcc2_region": "us-east-1"

}):
    cda_module_mock_context.mock_module("boto3")
    cda_module_mock_context.mock_module("post")
    cda_module_mock_context.mock_module("pt_poster")
    cda_module_mock_context.mock_module("pcc_poster")
    cda_module_mock_context.mock_module("utility")
    cda_module_mock_context.mock_module("edge_db_lambda_client"),
    cda_module_mock_context.mock_module("edge_sqs_utility_layer.sqs_utility")
    cda_module_mock_context.mock_module("update_scheduler")
    cda_module_mock_context.mock_module("EdgeDbLambdaClient")

    import PosterLambda


class TestPosterLambda(unittest.TestCase):
    """
    Test module for PosterLambda.py
    """
    
    sample_device_id = "352953081637849"
    bucket_name = "edge-j1939-test"
    file_key = "ConvertedFiles/64200027/352953081637849/2024/01/17/EDGE_352953081637849_64200027_SC8153_1705470843.json"
    file_size = 1026
    s3_event_body = {
        'Records': [
            {
                's3': {
                    's3SchemaVersion': '1.0',
                    'configurationId': '93d06a9d-d1a7-45f5-b0b6-5d763f24ac90',
                    'bucket': {
                        'name': bucket_name,
                        'ownerIdentity': {'principalId': 'A2LE772XLDSELB'},
                        'arn': 'arn:aws:s3:::edge-j1939-test'
                    },
                    'object': {
                        'key': file_key,
                        'size': file_size,
                        'eTag': '2a80137307ca8181f3758b99884cbd3f',
                        'versionId': 'YPTSqoyY544bD8sDX8D9h1WoRaSdFyek',
                        'sequencer': '0065A76B7B36C90955'
                    }
                },
                "body": json.dumps({"test": "body"}),
                "receiptHandle": "test-receipt-handle"
            }
        ]
    }

    serialized_file = {
        'componentSerialNumber': '64200027',
        'telematicsPartnerName': None,
        'dataSamplingConfigId': 'SC8091',
        'messageFormatVersion': '1.1.1',
        'customerReference': 'Cummins',
        'telematicsDeviceId': '352953081637849',
        'dataEncryptionSchemeId': 'ES1',
        'numberOfSamples': 1,
        'samples': [
            {
                'dateTimestamp': '2024-01-17T05:54:00.503Z',
                'convertedDeviceParameters': {
                    'PDOP': '0.0',
                    'messageID': '7033dcc2-c67d-4dfd-abae-ce0a283f2646',
                    'Latitude': '0.36386',
                    'Satellites_Used': '0',
                    'Longitude': '0.0',
                    'Altitude': '0.0'
                },
                'convertedEquipmentParameters': [
                    {
                        'protocol': 'J1939',
                        'networkId': 'CAN1',
                        'deviceId': '00',
                        'parameters': {
                            '2434': '100.0',
                            '2433': '200.0',
                            '1152': '-273.0',
                            '1150': '1500.0',
                            '1208': '552.0'
                        }
                    },
                    {
                        'protocol': 'J1939',
                        'networkId': 'CAN1',
                        'deviceId': '01',
                        'parameters': {
                            '1149': '150.0',
                            '1147': '250.0',
                            '1145': '170.0',
                            '1151': '110.0'
                        }
                    },
                    {
                        'protocol': 'J1939',
                        'networkId': 'CAN1',
                        'deviceId': '39',
                        'parameters': {
                            '2432': '-125.0',
                            '513': '100.0',
                            '190': '0.0',
                            '899': '4.0',
                            '91': '60.0',
                            '92': '70.0'
                        }
                    }
                ]
            }
        ]
    }

    file_object = {
        'ResponseMetadata': {},
        'LastModified': "1981-08-03T01:17:04.000Z",
        'ContentLength': file_size,
        'ContentType': 'binary/octet-stream',
        'Metadata': {},
        'Body': io.BytesIO(json.dumps(serialized_file).encode("utf-8"))
    }

    
    @patch.dict("os.environ", {"QueueUrl": "test-url"})
    @patch("PosterLambda.boto3.client")
    def test_delete_message_from_sqs_queue_successful(self, mock_client):
        """
        Test for delete_message_from_sqs_queue() running successfully.
        """
        mock_client.return_value.delete_message.return_value = "test-response"

        response = PosterLambda.delete_message_from_sqs_queue("test-handle")

        mock_client.return_value.delete_message.assert_called_with(QueueUrl="test-url", ReceiptHandle="test-handle")
        self.assertEqual(response, "test-response")


    @patch("PosterLambda.EDGE_DB_CLIENT")
    def test_getDeviceInfo_success(self, mock_db_reader):
        mock_db_reader.execute.return_value = [{'test': 'value'}]
        result = PosterLambda.get_device_info(self.sample_device_id)
        self.assertEqual(result, {'test': 'value'})
        mock_db_reader.execute.assert_called_once()


    @patch("PosterLambda.EDGE_DB_CLIENT")
    def test_getDeviceInfo_uncaughtException(self, mock_db_reader):
        mock_db_reader.execute.side_effect = Exception("Mock db reader exception")
        result = PosterLambda.get_device_info(self.sample_device_id)
        self.assertEqual(result, False)
        mock_db_reader.execute.assert_called_once()


    @patch("PosterLambda.EDGE_DB_CLIENT")
    def test_getDeviceInfo_caughtException(self, mock_db_reader):
        mock_db_reader.execute.return_value = None
        result = PosterLambda.get_device_info(self.sample_device_id)
        self.assertEqual(result, False)
        self.assertEqual(mock_db_reader.execute.call_count, 2)


    def test_get_business_partner_ebu(self):
        """
        Test for get_business_partner() returning `EBU` when EBUSpecifier is supplied.
        """
        response = PosterLambda.get_business_partner("EBU")
        self.assertEqual(response, "EBU")


    def test_get_business_partner_psbu(self):
        """
        Test for get_business_partner() returning `PSBU` when PSBUSpecifier is supplied.
        """
        response = PosterLambda.get_business_partner("PSBU")
        self.assertEqual(response, "PSBU")


    def test_get_business_partner_other(self):
        """
        Test for get_business_partner() returning False when other device types are supplied.
        """
        response = PosterLambda.get_business_partner("other")
        self.assertEqual(response, False)


    @patch.dict(
        "os.environ",
        {
            "cd_device_owners": json.dumps({"CD": True}),
            "psbu_device_owner": json.dumps({"PSBU": True}),
            "metaWriteQueueUrl": "queue-url"
        }
    )
    @patch("PosterLambda.delete_message_from_sqs_queue")
    @patch("PosterLambda.ssm_client")
    @patch("PosterLambda.get_device_info")
    @patch("PosterLambda.data_quality")
    @patch("PosterLambda.s3_client")
    @patch("PosterLambda.post")
    @patch("PosterLambda.pt_poster")
    @patch("PosterLambda.pcc_poster")
    @patch("PosterLambda.get_request_id_from_consumption_view")
    @patch("PosterLambda.update_scheduler_table")
    @patch("PosterLambda.sqs_send_message")
    def test_retrieve_and_process_file_hb_pcc_claimed(
        self,
        mock_sqs_send_message,
        mock_update_scheduler,
        mock_get_request_id,
        mock_pcc_poster,
        mock_pt_poster,
        mock_post,
        mock_s3_client,
        mock_data_quality,
        mock_get_device_info,
        mock_ssm_client,
        mock_delete_sqs_message
    ):
        """
        Test for retrieve_and_process_file() when:
        - J1939 type is HB
        - TSP name is missing in payload
        - device owner is present
        - PCC claim status is `claimed`
        """
        s3_event_str = json.dumps(self.s3_event_body)

        mock_s3_client.get_object.return_value = self.file_object
        mock_post.get_cspec_req_id.return_value = ("config-spec-name", "req-id")
        mock_get_request_id.return_value = "request-id"
        mock_get_device_info.return_value = {
            "device_owner": "PSBU",
            "pcc_claim_status": "CLAIMED",
            "cust_ref": "cust-ref",
            "equip_id": "equip-id",
            "vin": "vin"
        }
        mock_ssm_client.get_parameter.return_value = {
            "Parameter": {
                "Value": json.dumps({
                    "EngineStatOverride": "EngineStat_9",
                    "LoadFactorOverride": "LoadFactor_9",
                    "EngineStatSc": "SC8091",
                    "LoadFactorSc": "SC8093"
                })
            }
        }

        PosterLambda.retrieve_and_process_file(self.s3_event_body, "test-receipt-handle")

        mock_data_quality.assert_called_with(s3_event_str)
        mock_s3_client.get_object.assert_called_with(Bucket=self.bucket_name, Key=self.file_key)
        mock_post.get_cspec_req_id.assert_called_with("SC8091")
        mock_get_request_id.assert_called_with("J1939_HB", "EDGE_352953081637849_64200027_config-spec-name")
        mock_update_scheduler.assert_called_with("request-id", "352953081637849")

        mock_sqs_send_message.assert_called()
        mock_pt_poster.send_to_pt.assert_not_called()
        mock_post.send_to_cd.assert_not_called()
        mock_pcc_poster.send_to_pcc.assert_called()
        mock_delete_sqs_message.assert_called()


    @patch("PosterLambda.boto3.client")
    def test_data_quality_successful(self, mock_boto3_client):
        """
        Test for data_quality() running successfully.
        """
        mock_lambda_client = mock_boto3_client.return_value
        mock_lambda_client.invoke.return_value = {"StatusCode": 200}
        
        with self.assertRaises(RuntimeError):
            PosterLambda.data_quality("test-event")

        mock_lambda_client.invoke.assert_called_with(
            FunctionName="DataQualityLambda",
            InvocationType="Event",
            Payload="test-event"
        )


    @patch("PosterLambda.Process")
    def test_lambda_handler_successful(self, mock_process):
        """
        Test for lambda_handler() running successfully.
        """
        PosterLambda.lambda_handler(self.s3_event_body, None)

        mock_process.assert_called_with(
            target=PosterLambda.retrieve_and_process_file,
            args=({"test": "body"}, "test-receipt-handle")
        )
        mock_process.return_value.start.assert_called()
        mock_process.return_value.join.assert_called()


if __name__ == '__main__':
    unittest.main()
