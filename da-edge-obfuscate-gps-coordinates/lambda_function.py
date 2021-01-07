from obfuscate_gps_handler import obfuscate_gps
import edge_logger as logging


logger = logging.logging_framework("DaEdgeObfuscateGPSCoordinates.LambdaFunction")


def lambda_handler(event, context):
    body = event
    obfuscate_gps(body)
