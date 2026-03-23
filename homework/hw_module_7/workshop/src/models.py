from dataclasses import dataclass
import dataclasses
import json
import pandas as pd

@dataclass
class GreenRide:
    """
    Task: Define the Data Schema
    Represents a single Green Taxi ride with 8 core fields.
    """
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float

def green_ride_from_row(row):
    """
    Task: Data Transformation (Pandas -> GreenRide)
    1. Extract fields from a single Pandas DataFrame row.
    2. Handle potential NaN/null values with default types.
    3. Standardize timestamps as strings for Kafka serialization.
    """
    def get_int(val, default=0):
        return int(val) if pd.notna(val) else default

    def get_float(val, default=0.0):
        return float(val) if pd.notna(val) else default

    return GreenRide(
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        PULocationID=get_int(row['PULocationID']),
        DOLocationID=get_int(row['DOLocationID']),
        passenger_count=get_int(row['passenger_count']),
        trip_distance=get_float(row['trip_distance']),
        tip_amount=get_float(row['tip_amount']),
        total_amount=get_float(row['total_amount'])
    )

def ride_serializer(ride):
    """
    Task: Kafka Serialization (Model -> Bytes)
    Converts a GreenRide dataclass into a UTF-8 encoded JSON byte stream.
    """
    return json.dumps(dataclasses.asdict(ride)).encode('utf-8')

def ride_deserializer(data):
    """
    Task: Kafka Deserialization (Bytes -> Model)
    Reconstructs a GreenRide dataclass from a UTF-8 encoded JSON byte stream.
    """
    ride_dict = json.loads(data.decode('utf-8'))
    return GreenRide(**ride_dict)
