from pyspark.sql import SparkSession, DataFrame

import os


def create_spark_session(app_name: str, master: str) -> SparkSession:
    """
    Creates a Spark session with the given app name and master URL.
    """
    hadoop_aws_jar = os.path.join(
        os.environ["SPARK_HOME"], "jars", "hadoop-aws-3.2.2.jar"
    )
    aws_java_sdk_bundle_jar = os.path.join(
        os.environ["SPARK_HOME"], "jars", "aws-java-sdk-bundle-1.11.1000.jar"
    )

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .config("spark.cores.max", "4")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.jars", f"{hadoop_aws_jar},{aws_java_sdk_bundle_jar}")
        .getOrCreate()
    )
    return spark
