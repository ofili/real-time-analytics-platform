from real_time_analytics.core.config import Settings
from real_time_analytics.core.logging import logger
from real_time_analytics.ingest.iot_to_s3 import run_iot_to_s3
from real_time_analytics.ingest.s3_to_timescaledb import run_s3_to_timescaledb
from real_time_analytics.spark_session import (
    create_spark_session,
)


if __name__ == "__main__":
    app_name = Settings.spark_app_name
    master = Settings.spark_master

    bucket_name = Settings.aws_bucket

    bootstrap_servers = Settings.kafka_bootstrap_servers
    topic_name = Settings.kafka_topic

    database = Settings.timescaledb_database
    username = Settings.timescaledb_username
    password = Settings.timescaledb_password
    host = Settings.timescaledb_host
    port = Settings.timescaledb_port
    schema = Settings.timescaledb_schema
    tablename = Settings.timescaledb_tablename

    spark = create_spark_session(app_name, master)

    logger.info(f"Spark session created with name: {app_name} and master: {master}")

    run_iot_to_s3(
        spark,
        bootstrap_servers,
        "iot-data",
        "iot-data-consumer-group",
        bucket_name,
        "iot_data",
    )

    logger.info("Finished ingesting data from IoT to S3")

    run_s3_to_timescaledb(
        spark, database, username, password, host, port, schema, tablename
    )

    logger.info("Finished loading data from S3 to TimescaleDB")

    spark.stop()
