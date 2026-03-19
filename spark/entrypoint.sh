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
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
  --hiveconf hive.server2.authentication=NOSASL

echo "Waiting for Thrift Server to accept connections..."
for i in $(seq 1 30); do
  if echo > /dev/tcp/localhost/10000 2>/dev/null; then
    echo "Thrift Server ready."
    break
  fi
  echo "  attempt $i/30..."
  sleep 2
done

echo "Creating Nessie namespaces..."
spark-sql --master local[*] \
  -e "CREATE NAMESPACE IF NOT EXISTS nessie.\`default\`; CREATE NAMESPACE IF NOT EXISTS nessie.db;" 2>/dev/null \
  && echo "Namespaces nessie.default and nessie.db created." \
  || echo "WARNING: Failed to create namespaces (may already exist)."

echo "Spark Thrift Server started on port 10000"
tail -f ${SPARK_HOME}/logs/*
