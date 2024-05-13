from datetime import datetime

from elasticsearch import logger
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

from real_time_analytics.core.config import Settings
from real_time_analytics.core.logging import logger
from real_time_analytics.spark_session import create_spark_session


def write_to_s3(
    data: DataFrame, bucket_name: str, path: str, format: str = "parquet"
) -> None:
    """
    Writes the DataFrame data to S3 in the specified format with a timestamp in the filename.

    Args:
        data (pyspark.sql.DataFrame): The DataFrame containing the data to write.
        bucket_name (str): S3 bucket where the data will be written.
        path (str): The base S3 path where the data will be written.
        format (str, optional): The format to use for writing the data (e.g., "parquet", "json"). Defaults to "parquet".
    """
    # Get current timestamp in desired format (e.g., YYYY-MM-DD_HH)
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H")

    # Combine base path with timestamp and format for filename
    filename = f"{path}/{current_timestamp}.{format}"

    # Trigger processing and write to S3 with timestamped filename
    data.write.format(format).option("path", f"s3a://{bucket_name}/{filename}").mode(
        "append"
    ).saveAsTable("s3_data")


def read_iot_stream(
    spark: SparkSession,
    kafka_brokers,
    kafka_topic,
    consumer_group_id,
    streaming_duration,
    json_schema,
) -> DataFrame:
    iot_batched_data = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", kafka_topic)
        .option("group.id", consumer_group_id)
        .option("processingTime", streaming_duration)
        .option("startingOffsets", "earliest")
        .load()
        .select(
            from_json(col("value").cast("string"), schema=json_schema).alias("data")
        )
    )
    return iot_batched_data


def run(spark: SparkSession, brokers, topic, group_id, bucket, streaming_duration=80):
    json_schema = """{
        "type": "struct",
        "fields": [
            {"name": "device_id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "data", "type": "string"}
        ]
    }"""

    iot_batched_data = read_iot_stream(
        spark, brokers, topic, group_id, streaming_duration, json_schema
    )

    write_to_s3(iot_batched_data, bucket, "iot-data", format="parquet")

    # Start the Spark Streaming query
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    app_name = Settings.spark_app_name
    master = Settings.spark_master
    spark = create_spark_session(app_name, "local[*]")
    logger.info("IOT ingestion spark application started")
