import json
import uuid
import edge_logger as logging


logger = logging.logging_framework("EdgeNGDI2CDSDKConversion.CDSDKHB")

def get_message_id():

    message_id = str(uuid.uuid4())

    logger.info(f"MessageID: {message_id}")

    return message_id


def get_json_payload():

    with open("cd_hb_sdk_payload.json", "r") as json_file_stream:

        return json.loads(json_file_stream.read())


class CDHBSDK:

    """CD SDK Class for HB Payload to CD"""

    payload = {}  # Payload to send to CP
    values = {}  # Non-None values in class

    def __init__(self, var_dict):

        # Add variables to this class

        for variable in var_dict:

            vars(self)[variable] = var_dict[variable]

        logger.info(f"Class variables: {vars(self)}")

        # Creating JSON payload to send to CD

        self.payload = self.create_payload()

    def create_payload(self):

        # Check which of the variables have been populated

        class_variables = vars(self)

        json_payload = get_json_payload()

        for var in class_variables:

            if class_variables[var]:

                logger.info(f"Variable: {var} Value: {class_variables[var]}")

                self.values[var] = class_variables[var]

        for param in json_payload:

            if param.lower() in self.values:

                json_payload[param] = self.values[param.lower()]

        if not json_payload["Telematics_Partner_Message_ID"]:

            json_payload["Telematics_Partner_Message_ID"] = get_message_id()

        if not json_payload["Sent_Date_Time"]:

            json_payload["Sent_Date_Time"] = json_payload["Occurrence_Date_Time"] if "Occurrence_Date_Time" \
                                                                                     in json_payload else ""

        return json.loads(json.dumps(json_payload))

    def get_payload(self):

        return self.payload















