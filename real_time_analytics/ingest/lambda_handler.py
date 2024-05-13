import json

import boto3
from confluent_kafka.cimpl import KafkaException

from real_time_analytics.core.logging import logger
from real_time_analytics.core.services.producer import get_kafka_producer


async def s3_lambda_handler(event, context):
    try:
        producer = await get_kafka_producer()
    except KafkaException as e:
        logger.error(f"Error creating Kafka producer: {e}")
        return {"statusCode": 500, "body": f"Failed to create Kafka producer: {e}"}

    s3_client = boto3.client("s3")

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
