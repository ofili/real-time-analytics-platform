from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType

from real_time_analytics.ingest.iot_to_s3 import read_iot_stream


class TestReadIotStream(object):

    def test_read_iot_stream(self):
        # Create a SparkSession
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("TestReadIotStream")
            .getOrCreate()
        )

        # Mock Kafka data
        kafka_data = [b'{"device_id": "sensor1", "timestamp": 123, "data": "value1"}']
        kafka_data_stream = spark.sparkContext.parallelize(kafka_data)

        # Expected data_schema
        json_schema = StructType(
            [
                StructField("device_id", StringType(), True),
                StructField("timestamp", LongType(), True),
                StructField("data", StringType(), True),
            ]
        )

        # Expected output
        expected_output = [("sensor1", 123, "value1")]

        # Read stream and select data
        iot_data = read_iot_stream(
            spark, "dummy:9092", "iot-topic", "test-group", 1, json_schema
        ).select("data.*")

        # Start the streaming query and verify results
        query = (
            iot_data.writeStream.format("memory").option("queryName", "test").start()
        )
        query.awaitTermination(5)  # Wait for 5 seconds

        # Convert to a regular DataFrame
        result = spark.sql(query.lastProgress.progress.sql).toDF()

        assert (
            result == expected_output
        ), f"Actual result: {result}, Expected: {expected_output}"

        # Stop the SparkSession
        spark.stop()
