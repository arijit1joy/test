import json


def get_json_payload():

    with open("cd_fc_sdk_payload.json", "r") as json_file_stream:

        return json.loads(json_file_stream.read())


class CDFCFCSDK:

    """CD SDK Class for Payload to CD"""

    payload = {}  # Payload to send to CP
    values = {}  # Non-None values in class

    def __init__(self, notification_version, telematics_partner_message_id, telematics_partner_name,
                 customer_reference, occurrence_date_time, active, datalink_bus, source_address, spn, fmi,
                 occurrence_count,
                 latitude=None, longitude=None, altitude=None, direction_heading=None, vehicle_distance=None, make=None,
                 location_text_description=None, gps_vehicle_speed=None, active_faults=None, equipment_id=None,
                 customer_equipment_group=None, calibration_identification=None, calibration_verification_number=None,
                 number_of_software_id_fields=None, software_identification=None, model=None, unit_number=None,
                 sent_date_time=None, vin=None, engine_serial_number=None, snapshots=None, telematics_box_id=None):

        # Optional Information
        self.occurrence_count = occurrence_count
        self.fmi = fmi
        self.spn = spn
        self.telematics_box_id = telematics_box_id
        self.snapshots = snapshots
        self.engine_serial_number = engine_serial_number
        self.vin = vin
        self.sent_date_time = sent_date_time
        self.unit_number = unit_number
        self.model = model
        self.software_identification = software_identification
        self.number_of_software_id_fields = number_of_software_id_fields
        self.calibration_verification_number = calibration_verification_number
        self.calibration_identification = calibration_identification
        self.customer_equipment_group = customer_equipment_group
        self.equipment_id = equipment_id
        self.active_faults = active_faults
        self.gps_vehicle_speed = gps_vehicle_speed
        self.location_text_description = location_text_description
        self.make = make
        self.vehicle_distance = vehicle_distance
        self.direction_heading = direction_heading
        self.altitude = altitude
        self.longitude = longitude
        self.latitude = latitude

        # Mandatory Information
        self.source_address = source_address
        self.datalink_bus = datalink_bus
        self.active = active
        self.occurrence_date_time = occurrence_date_time
        self.customer_reference = customer_reference
        self.telematics_partner_name = telematics_partner_name
        self.telematics_partner_message_id = telematics_partner_message_id
        self.notification_version = notification_version

        self.payload = self.create_payload()

    def create_payload(self):

        # Check which of the variables have been populated

        class_variables = vars(self)

        json_payload = get_json_payload()

        for var in class_variables:

            if class_variables[var]:

                print("Variable:", var, "Value:", class_variables[var])

                self.values[var] = class_variables[var]

        for param in json_payload:

            if param.lower() in self.values:

                json_payload[param] = self.values[param.lower()]

        return json_payload















