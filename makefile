# Docker Compose file
DOCKER_COMPOSE_FILE := compose.yml

# Spark application path
APP_PATH := /main.py

# Spark package dependencies
SPARK_PACKAGES := org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.1000,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2

# Docker Compose project name
COMPOSE_PROJECT_NAME := iot-realtime-analytics

# Spark master URL
SPARK_MASTER_URL := spark://spark-master:7077

# Targets
.PHONY: up down run

up:
    docker-compose -p $(COMPOSE_PROJECT_NAME) up -d

down:
    docker-compose -p $(COMPOSE_PROJECT_NAME) down

run:
    docker run --rm \
        --network=$(COMPOSE_PROJECT_NAME)_spark-cluster \
        apache/spark \
        bin/spark-submit \
        --master=$(SPARK_MASTER_URL) \
        --packages=$(SPARK_PACKAGES) \
        $(APP_PATH)

# Default target (run the application)
.DEFAULT_GOAL := run