import json

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession, DataFrame

from real_time_analytics.core.services.consumer import (
    create_and_subscribe_kafka_consumer,
)
from real_time_analytics.core.logging import logger


def process_s3_object(spark, bucket_name, file_key):
    """Read data from S3 and return a Spark DataFrame"""
    df = (
        spark.readStream.format("parquet")
        .option("checkpointLocation", f"{bucket_name}/_checkpoint")
        .load(f"s3://{bucket_name}/{file_key}")
    )
    return df


async def retrieve_and_validate_iot_data(spark: SparkSession) -> DataFrame:
    """Retrieve and validate IoT data from Kafka and S3"""

    consumer = await create_and_subscribe_kafka_consumer(
        "s3-events-consumer", "s3-events"
    )
    logger.info("Subscribed to Kafka topic: s3-events")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        message_value = msg.value().decode("utf-8")
        message_json = json.loads(message_value)
        bucket_name = message_json["bucket_name"]
        file_key = message_json["file_key"]

        logger.info(f"Received message from Kafka topic: {bucket_name}/{file_key}")

        # Check for "processed" metadata flag before processing
        s3_client = boto3.client("s3")
        try:
            response = s3_client.head_object(Bucket=bucket_name, Key=file_key)
            if (
                "Metadata" in response
                and "processed" in response["Metadata"]
                and response["Metadata"]["processed"] == "true"
            ):
                logger.info(f"Object already processed: {bucket_name}/{file_key}")
                continue  # Skip processing if already marked as processed
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.error(f"Object not found: {bucket_name}/{file_key}")
            else:
                logger.error(f"Error checking S3 object metadata: {e}")

        # Read data from S3
        df = process_s3_object(spark, bucket_name, file_key)

        yield df  # Yield the DataFrame for further processing


def process_iot_data(df: DataFrame):
    # Convert timestamp to timestamptz
    df = df.withColumn("timestamp", df["timestamp"].cast("timestamptz"))

    # Add device_id to dataframe
    df = df.withColumn("device_id", df["device_id"].cast("string"))

    # Convert data to JSON
    df = df.withColumn("data", df["data"].cast("string"))

    # Convert data to JSON
    df = df.withColumn("data", df["data"].cast("string"))

    return df


def write_to_timescaledb(
    df: DataFrame, database, username, password, host, port, schema, tablename
):
    (
        df.write.format("jdbc")
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
        .option("dbtable", f"{schema}.{tablename}")
        .option("user", username)
        .option("password", password)
        .save()
    )


def run_s3_to_timescaledb(
    spark, database, username, password, host, port, schema, tablename
):
    # Retrieve S3 data
    df = retrieve_and_validate_iot_data(
        spark,
    )

    # Write data to TimescaleDB
    write_to_timescaledb(
        df,
        database,
        username,
        password,
        host,
        port,
        schema,
        tablename,
    )
