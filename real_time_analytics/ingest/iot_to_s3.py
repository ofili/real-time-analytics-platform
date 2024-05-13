from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col

from real_time_analytics.ingest.schema import data_schema

DEFAULT_FORMAT = "parquet"
DEFAULT_STREAMING_DURATION = 80


def write_data_to_s3(
    data: DataFrame, bucket_name: str, path: str, format: str = DEFAULT_FORMAT
) -> None:
    """
    Writes the DataFrame data to S3 in the specified format with a timestamp in the filename.

    Args:
        data (pyspark.sql.DataFrame): The DataFrame containing the data to write.
        bucket_name (str): S3 bucket where the data will be written.
        path (str): The base S3 path where the data will be written.
        format (str, optional): The format to use for writing the data (e.g., "parquet", "json"). Defaults to "parquet".
    """
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H")
    filename = f"{path}/{current_timestamp}.{format}"
    data.write.format(format).option("path", f"s3a://{bucket_name}/{filename}").mode(
        "append"
    ).saveAsTable("s3_data")


def read_iot_stream(
    spark: SparkSession,
    kafka_brokers: str,
    kafka_topic: str,
    consumer_group_id: str,
    streaming_duration: int = DEFAULT_STREAMING_DURATION,
    schema: str = data_schema,
) -> DataFrame:
    """
    Reads data from a Kafka topic using Spark Structured Streaming, applies a JSON data_schema to the data, and returns a PySpark DataFrame.

    Args:
        spark (pyspark.sql.SparkSession): The Spark session.
        kafka_brokers (str): The Kafka brokers.
        kafka_topic (str): The Kafka topic.
        consumer_group_id (str): The consumer group ID.
        streaming_duration (int, optional): The streaming duration in seconds. Defaults to 80.
        schema (str, optional): The JSON data_schema to apply to the data. Defaults to the defined JSON_SCHEMA constant.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", kafka_topic)
        .option("group.id", consumer_group_id)
        .option("processingTime", streaming_duration)
        .option("startingOffsets", "earliest")
        .load()
        .select(from_json(col("value").cast("string"), schema=schema).alias("data"))
    )


def run_iot_to_s3(
    spark: SparkSession, brokers: str, topic: str, group_id: str, bucket: str, path: str
) -> None:
    """
    Runs the Spark application to read data from Kafka, process it, and write it to S3.

    Args:
        spark (pyspark.sql.SparkSession): The Spark session.
        brokers (str): The Kafka brokers.
        topic (str): The Kafka topic.
        group_id (str): The consumer group ID.
        bucket (str): The S3 bucket.
        path (str): The directory in S3 bucket.
    """
    iot_batched_data = read_iot_stream(spark, brokers, topic, group_id)
    write_data_to_s3(iot_batched_data, bucket, path)
    spark.streams.awaitAnyTermination()
