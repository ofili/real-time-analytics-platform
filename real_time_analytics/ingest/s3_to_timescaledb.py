import json

import boto3
from botocore.exceptions import ClientError
from confluent_kafka import Producer
from pyspark.sql import SparkSession

from real_time_analytics.core.config import Settings
from real_time_analytics.core.logging import logger


def s3_event_to_kafka_handler(event, context):

    s3_client = boto3.client("s3")  # For validation purposes
    producer: Producer = Producer(
        {"bootstrap.servers": Settings.kafka_bootstrap_servers}
    )

    try:
        for record in event["Records"]:
            bucket_name = record["s3"]["bucket"]["name"]
            file_key = record["s3"]["object"]["key"]

            logger.info(f"New object detected: {bucket_name}/{file_key}")

            # Prepare Kafka message
            message = json.dumps({"bucket_name": bucket_name, "file_key": file_key})

            # Send message to Kafka topic
            producer.produce("s3-events", message.encode("utf-8"))
            producer.poll(0)  # Flush messages without waiting

            logger.info(f"Sent message to Kafka topic: s3-events")

            # Set "processed" flag in S3 object metadata
            s3_client.put_object_acl(
                Bucket=bucket_name, Key=file_key, Metadata={"processed": "true"}
            )

        producer.flush()  # Ensure all messages are delivered before returning

        return {"statusCode": 200, "body": "Successfully processed S3 event(s)"}

    except Exception as e:
        logger.error(f"Error processing S3 event: {e}")
        return {"statusCode": 500, "body": f"Failed to process S3 event: {e}"}


def retrieve_and_validate_iot_data(
    spark: SparkSession, bucket_name: str, file_key: str
):
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
            return  # Skip processing if already marked as processed
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.error(f"Object not found: {bucket_name}/{file_key}")
        else:
            logger.error(f"Error checking S3 object metadata: {e}")

    # Read data from S3
    df = (
        spark.readStream.format("parquet")
        .option("checkpointLocation", f"{bucket_name}/_checkpoint")
        .load(f"s3://{bucket_name}/{file_key}")
    )

    return df
