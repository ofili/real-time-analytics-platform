import os
from typing import Optional

import yaml


class Settings:
    postgres_host: Optional[str] = None
    postgres_username: Optional[str] = None
    postgres_password: Optional[str] = None
    postgres_database: Optional[str] = None
    postgres_port: Optional[int] = None

    kafka_bootstrap_servers: Optional[str] = None
    kafka_topic: Optional[str] = None

    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: Optional[str] = None

    spark_master: Optional[str] = None
    spark_app_name: Optional[str] = None

    elasticsearch_host: Optional[str] = None

    @classmethod
    def load_config(cls) -> None:
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        cls.timescaledb_host = config.get("postgres", {}).get("host")
        cls.timescaledb_username = config.get("postgres", {}).get("username")
        cls.timescaledb_password = config.get("postgres", {}).get("password")
        cls.timescaledb_database = config.get("postgres", {}).get("database")
        cls.timescaledb_port = config.get("postgres", {}).get("port")
        cls.timescaledb_schema = config.get("data_schema", {}).get("data_schema")
        cls.timescaledb_tablename = config.get("postgres", {}).get("table")

        cls.kafka_bootstrap_servers = config.get("kafka", {}).get("bootstrap_servers")
        cls.kafka_topic = config.get("kafka", {}).get("topic")

        cls.aws_access_key_id = config.get("aws_config", {}).get("access_key_id")
        cls.aws_secret_access_key = config.get("aws_config", {}).get("secret_access_key")
        cls.aws_region = config.get("aws_config", {}).get("region")
        cls.aws_bucket = config.get("aws_config", {}).get("bucket")
        cls.aws_ingestion_path = config.get("aws_config", {}).get("ingestion")

        cls.spark_master = config.get("spark", {}).get("master")
        cls.spark_app_name = config.get("spark", {}).get("app_name")

        cls.elasticsearch_host = config.get("elasticsearch", {}).get("host")


Settings.load_config()
