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
- Column: tip_amount as DOUBLE
- Watermark: event_timestamp - INTERVAL '5' SECOND
- Scan startup mode: earliest-offset

Window:
- Type: Tumbling, 1 hour
- Aggregation: SUM(tip_amount) as total_tips

Sink:
- Type: PostgreSQL via JDBC
- Table: total_tips_per_hour
- Connection: jdbc:postgresql://postgres:5432/postgres
"""

def create_trips_source_kafka(t_env):
    source_ddl = """
        CREATE TABLE green_trips (
            lpep_pickup_datetime VARCHAR,
            tip_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'q6-group-fresh',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return "green_trips"

def create_tips_sink_postgres(t_env):
    sink_ddl = """
        CREATE TABLE total_tips_per_hour (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            total_tips DOUBLE PRECISION,
            PRIMARY KEY (window_start, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'total_tips_per_hour',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return "total_tips_per_hour"

def run_tip_aggregation_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table = create_trips_source_kafka(t_env)
    sink_table = create_tips_sink_postgres(t_env)

    t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            window_end,
            SUM(tip_amount) AS total_tips
        FROM TABLE(
            TUMBLE(
                TABLE {source_table},
                DESCRIPTOR(event_timestamp),
                INTERVAL '1' HOUR
            )
        )
        GROUP BY window_start, window_end
    """).wait()

if __name__ == '__main__':
    run_tip_aggregation_job()
