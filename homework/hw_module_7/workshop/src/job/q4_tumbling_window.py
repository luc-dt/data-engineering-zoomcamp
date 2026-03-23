import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

"""
Task Specifications:
Source:
- Kafka topic: green-trips
- Bootstrap server: redpanda:29092
- Column: lpep_pickup_datetime as VARCHAR
- Transformation: TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss') as event_timestamp
- Watermark: event_timestamp - INTERVAL '5' SECOND
- Scan startup mode: earliest-offset

Window:
- Type: Tumbling, 5 minutes
- Group by: window_start, pulocationid
- Aggregation: COUNT(*) as num_trips

Sink:
- Type: PostgreSQL via JDBC
- Table: trips_per_zone_5min
- Primary Key: (window_start, pulocationid) NOT ENFORCED
- Connection: jdbc:postgresql://postgres:5432/postgres

Job Setup:
- Checkpointing: 10 seconds
- Parallelism: 1
"""

def create_trips_source_kafka(t_env):
    """
    Defines the Kafka source table 'green_trips'.
    Includes 'lpep_pickup_datetime' conversion to event time
    and a 5-second watermark delay.
    """
    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            PULocationID INTEGER,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'q4-group-fresh',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return "green_trips"

def create_trips_sink_postgres(t_env):
    """
    Defines the JDBC sink table 'trips_per_zone_5min' in PostgreSQL.
    Maps PULocationID (int) and window timestamps.
    """
    sink_ddl = """
        CREATE TABLE trips_per_zone_5min (
            window_start TIMESTAMP(3),
            pulocationid INTEGER,
            num_trips BIGINT,
            PRIMARY KEY (window_start, pulocationid) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'trips_per_zone_5min',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return "trips_per_zone_5min"

def run_tumbling_window_job():
    """
    Main job execution logic:
    1. Sets up the StreamTableEnvironment and checkpointing.
    2. Registers the source and sink tables.
    3. Executes the 5-minute tumbling window aggregation.
    """
    # 1. Initialize Flink Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # Checkpoint every 10 seconds
    env.set_parallelism(1)

    # 2. Initialize Table Environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 3. Create Source and Sink
    source_table = create_trips_source_kafka(t_env)
    sink_table = create_trips_sink_postgres(t_env)

    # 4. Define and Execute the Job
    t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            PULocationID AS pulocationid,
            COUNT(*) AS num_trips
        FROM TABLE(
            TUMBLE(
                TABLE {source_table},
                DESCRIPTOR(event_timestamp),
                INTERVAL '5' MINUTE
            )
        )
        GROUP BY window_start, PULocationID
    """).wait()

if __name__ == '__main__':
    run_tumbling_window_job()
