import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Tumble
from pyflink.datastream.window import SessionWindow
from pyflink.datastream.functions import AggregateFunction
from pyflink.table.window import Session

def create_aggregated_events_sink_postgres(t_env):
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip_address VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            event_count BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip_address VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Define the session window with a 5-minute gap
    session_gap = 5 * 60 * 1000  # 5 minutes in milliseconds

    try:
        # Create Kafka table
        source_table = create_processed_events_source_kafka(t_env)

        aggregated_table = create_aggregated_events_sink_postgres(t_env)
        
        t_env.from_path(source_table) \
            .window(Session.with_gap(session_gap).on("event_time").alias("session_window")) \
            .group_by("session_window, ip_address, host") \
            .select("ip_address, host, session_window.start as session_start, session_window.end as session_end, COUNT(*) as event_count") \
            .execute_insert(aggregated_table)
        
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()
