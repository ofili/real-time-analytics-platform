from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    TimestampType,
    DoubleType,
)

data_schema = StructType(
    [
        StructField("device_id", IntegerType(), False),
        StructField("location", IntegerType(), True),
        StructField("time", TimestampType(), True),
        StructField("device_type", IntegerType(), True),
        StructField("cpu", DoubleType(), True),
        StructField("memory", DoubleType(), True),
        StructField("disk_io", DoubleType(), True),
        StructField("energy_consumption", DoubleType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
    ]
)
