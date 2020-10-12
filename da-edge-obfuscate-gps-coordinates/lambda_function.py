from obfuscate_gps_handler import obfuscate_gps


def lambda_handler(event, context):
    print("Event: ", event)
    body = event
    obfuscate_gps(body)
