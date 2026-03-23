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
- Type: Session, 5 minute gap
- Group by: pulocationid
- Aggregation: COUNT(*) as num_trips

Sink:
- Type: PostgreSQL via JDBC
- Table: trips_per_zone_sessions
- Primary Key: (window_start, pulocationid) NOT ENFORCED
- Connection: jdbc:postgresql://postgres:5432/postgres
"""

def create_trips_source_kafka(t_env):
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
            'properties.group.id' = 'q5-group-v3',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return "green_trips"

def create_trips_sink_postgres(t_env):
    sink_ddl = """
        CREATE TABLE trips_per_zone_sessions (
            window_start TIMESTAMP(3),
            window_end   TIMESTAMP(3),
            pulocationid INTEGER,
            num_trips    BIGINT,
            PRIMARY KEY (window_start, window_end, pulocationid) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'trips_per_zone_sessions',
            'username'   = 'postgres',
            'password'   = 'postgres',
            'driver'     = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return "trips_per_zone_sessions"

def run_session_window_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_trips_source_kafka(t_env)
    sink_table = create_trips_sink_postgres(t_env)

    # Use Window TVF for Session Window
    # SESSION(TABLE table_name, DESCRIPTOR(time_column), gap_interval)
    t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            window_end,
            PULocationID AS pulocationid,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(
                TABLE {source_table} PARTITION BY PULocationID,
                DESCRIPTOR(event_timestamp),
                INTERVAL '5' MINUTE
            )
        )
        GROUP BY window_start, window_end, PULocationID
    """).wait()

if __name__ == '__main__':
    run_session_window_job()
