from typing import Optional, List, Any
from datetime import datetime
from json import JSONEncoder
import json

class CustomerEquipmentGroup:
    value: Optional[str]

    def __init__(self, value: Optional[str]) -> None:
        self.value = value


class Parameter:
    Name: Optional[str]
    Value: Optional[str]
    Parameter_Source_Address: Optional[int]

    def __init__(self, name: Optional[str], value: Optional[str], parameter_source_address: Optional[int]) -> None:
        self.Name = name
        self.Value = value
        self.Parameter_Source_Address = parameter_source_address




class Sdkclass:
    Notification_Version: Optional[str]
    Message_Type: Optional[str]
    Telematics_Box_ID: Optional[str]
    Telematics_Partner_Message_ID: Optional[int]
    Telematics_Partner_Name: Optional[str]
    Customer_Reference: Optional[str]
    Equipment_ID: Optional[int]
    Engine_Serial_Number: Optional[str]
    VIN: Optional[str]
    Occurrence_Date_Time: Optional[str]
    Sent_Date_Time: Optional[datetime]
    Active: Optional[int]
    Datalink_Bus: Optional[str]
    Source_Address: Optional[int]
    SPN: Optional[int]
    FMI: Optional[int]
    Occurrence_Count: Optional[int]
    Latitude: Optional[str]
    Longitude: Optional[str]
    Altitude: Optional[int]
    Direction_Heading: Optional[int]
    Vehicle_Distance: Optional[int]
    Location_Text_Description: Optional[str]
    GPS_Vehicle_Speed: Optional[int]
    Software_Identification: Optional[str]
    Make: Optional[str]
    Model: Optional[str]
    Unit_number: Optional[str]
    Active_Faults: Optional[List[Any]]
    Customer_Equipment_Group: Optional[List[CustomerEquipmentGroup]]
    Snapshots: Optional[List[Snapshot]]

    def __init__(self, notification_version: Optional[str], message_type: Optional[str],
                 telematics_box_id: Optional[str], telematics_partner_message_id: Optional[int],
                 telematics_partner_name: Optional[str], customer_reference: Optional[str],
                 equipment_id: Optional[int], engine_serial_number: Optional[str], vin: Optional[str],
                 occurrence_date_time: Optional[str], sent_date_time: Optional[datetime],
                 active: Optional[int], datalink_bus: Optional[str], source_address: Optional[int],
                 spn: Optional[int], fmi: Optional[int], occurrence_count: Optional[int],
                 latitude: Optional[str], longitude: Optional[str], altitude: Optional[int],
                 direction_heading: Optional[int], vehicle_distance: Optional[int],
                 location_text_description: Optional[str], gps_vehicle_speed: Optional[int],
                 software_identification: Optional[str], make: Optional[str], model: Optional[str],
                 unit_number: Optional[str], active_faults: Optional[List[Any]],
                 customer_equipment_group: Optional[List[CustomerEquipmentGroup]],
                 snapshots: Optional[List[Snapshot]]) -> None:
        self.Notification_Version = notification_version
        self.Message_Type = message_type
        self.Telematics_Box_ID = telematics_box_id
        self.Telematics_Partner_Message_ID = telematics_partner_message_id
        self.Telematics_Partner_Name = telematics_partner_name
        self.Customer_Reference = customer_reference
        self.Equipment_ID = equipment_id
        self.Engine_Serial_Number = engine_serial_number
        self.VIN = vin
        self.Occurrence_Date_Time = occurrence_date_time
        self.Sent_Date_Time = sent_date_time
        self.Active = active
        self.Datalink_Bus = datalink_bus
        self.Source_Address = source_address
        self.SPN = spn
        self.FMI = fmi
        self.Occurrence_Count = occurrence_count
        self.Latitude = latitude
        self.Longitude = longitude
        self.Altitude = altitude
        self.Direction_Heading = direction_heading
        self.Vehicle_Distance = vehicle_distance
        self.Location_Text_Description = location_text_description
        self.GPS_Vehicle_Speed = gps_vehicle_speed
        self.Software_Identification = software_identification
        self.Make = make
        self.Model = model
        self.Unit_number = unit_number
        self.Active_Faults = active_faults
        self.Customer_Equipment_Group = customer_equipment_group
        self.Snapshots = snapshots
