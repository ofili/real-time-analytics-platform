#!/bin/bash

. "/opt/spark/bin/load-spark-env.sh"

if [ "$SPARK_WORKLOAD" == "master" ];
then

export SPARK_MASTER_HOST=$(hostname)

cd /opt/spark/bin && ./spark-class org.apache.spark.deploy.master.Master --ip "$SPARK_MASTER_HOST" --port $SPARK_MASTER_PORT --webui-port "$SPARK_MASTER_WEBUI_PORT" >> "$SPARK_MASTER_LOG"

elif [ "$SPARK_WORKLOAD" == "worker" ];
then

# shellcheck disable=SC2086
cd /opt/spark/bin && ./spark-class org.apache.spark.deploy.worker.Worker --webui-port "$SPARK_WORKER_WEBUI_PORT" "$SPARK_MASTER" >> $SPARK_WORKER_LOG

elif [ "$SPARK_WORKLOAD" == "submit" ];
then
    echo "SPARK SUBMIT"

    # Spark submit command
    spark-submit \
      --class com.example.MySparkApp \
      --master spark://spark-master:7077 \
      --deploy-mode cluster \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
      --conf "spark.driver.bindAddress=spark-master" \
      --conf "spark.executor.instances=2" \
      --conf "spark.executor.memory=5G" \
      --conf "spark.driver.memory=5G" \
      --conf "spark.kafka.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
      --conf "spark.kafka.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer" \
      /opt/spark-app/target/MySparkApp-1.0-SNAPSHOT.jar

else
    echo "Undefined Workload Type $SPARK_WORKLOAD, must specify: master, worker, submit"
fi