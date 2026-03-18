#!/bin/bash
set -e

echo "Starting Spark master..."
start-master.sh

echo "Waiting for Spark master to be ready..."
sleep 5

echo "Starting Spark Thrift Server..."
start-thriftserver.sh \
  --master local[*] \
  --driver-memory 2g \
  --conf spark.executor.memory=2g \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0

echo "Spark Thrift Server started on port 10000"
tail -f ${SPARK_HOME}/logs/*
