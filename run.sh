#!/bin/bash

DIR=$(pwd)

# # uncomment this line to run in remote debugging mode
#export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005

config_file="/tmp/config.json"

spark-submit \
    --verbose \
    --master local[3] \
    --class "rocks.sblack.sparkstarter.main" \
    --packages org.apache.bahir:spark-streaming-twitter_2.11:2.0.1 \
    --jars file://${DIR}/lib/ojdbc7.jar \
    --jars file://${DIR}/lib/twitter4j-stream-4.0.4.jar \
    --jars file://${DIR}/lib/ojdbc7.jar \
    file://${DIR}/target/scala-2.11/spark-starter_2.11-0.01.jar ${config_file}

#    --jars file://${DIR}/lib/spark-sql-kafka-0-10_2.11-2.1.0.jar \
#    --jars file://${DIR}/lib/kafka-clients-0.10.1.0.jar \
#    --jars file://${DIR}/lib/spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar \
#    --jars file://${DIR}/lib/twitter4j-core-4.0.4.jar \
#    --jars file://${DIR}/lib/spark-streaming-twitter_2.11-2.0.1.jar \
