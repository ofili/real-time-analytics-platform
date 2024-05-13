from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, struct, from_json, col


def convert_to_json(json_files: DataFrame) -> DataFrame:
    """
    Converts the DataFrame to JSON format.
    """
    return json_files.select(to_json(struct(*json_files.columns)).alias("value"))


def parse_json_data(df, json_schema):
    """
    Parses JSON data within a Kafka DataFrame.

    Args:
      df (pyspark.sql.DataFrame): The Kafka DataFrame containing the data stream.
      json_schema (str): The data_schema of the JSON data.

    Returns:
      pyspark.sql.DataFrame: A DataFrame with the parsed JSON data.
    """
    return df.select(from_json(col("value").cast("string"), schema=json_schema).alias("data"))
