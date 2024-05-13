import datetime
from unittest.mock import patch

from pyspark.shell import spark

from real_time_analytics.ingest.iot_to_s3 import write_to_s3

# Mock Spark DataFrame and functions
mock_data = spark.createDataFrame([(1, "value")], ["col1", "col2"])
mock_write = mock_data.write.format("parquet")


@patch("write_to_s3.datetime")
def test_write_to_s3_success(mock_datetime):
    # Mock timestamp
    mock_datetime.now.return_value = datetime.datetime(
        2024, 5, 13, 10
    )  # Set a fixed time

    # Call the function
    write_to_s3(mock_data, "my-bucket", "data/path", format="parquet")

    # Verify call to mock_write with expected arguments
    mock_write.option("path", f"s3a://my-bucket/2024-05-13_10.parquet").mode(
        "append"
    ).saveAsTable.assert_called_once()


@patch("write_to_s3.datetime")
def test_write_to_s3_custom_format(mock_datetime):
    # Mock timestamp
    mock_datetime.now.return_value = datetime.datetime(2024, 5, 13, 10)

    # Call with custom format
    write_to_s3(mock_data, "my-bucket", "data/path", format="json")

    # Verify call with ".json" extension
    mock_write.option("path", f"s3a://my-bucket/2024-05-13_10.json").mode(
        "append"
    ).saveAsTable.assert_called_once()
