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

echo "Creating Nessie namespaces via REST API..."
# Wait for Nessie to be reachable
for i in $(seq 1 15); do
  if curl -sf http://nessie:19120/api/v2/config > /dev/null 2>&1; then
    echo "Nessie API ready."
    break
  fi
  echo "  waiting for Nessie... attempt $i/15"
  sleep 2
done

create_namespace() {
  local ns="$1"
  local hash
  hash=$(curl -sf http://nessie:19120/api/v2/trees/main | python3 -c "import sys,json; print(json.load(sys.stdin)['reference']['hash'])")
  curl -sf -X POST "http://nessie:19120/api/v2/trees/main@${hash}/history/commit" \
    -H "Content-Type: application/json" \
    -d "{
      \"commitMeta\": {\"message\": \"create ${ns} namespace\"},
      \"operations\": [
        {\"type\": \"PUT\", \"key\": {\"elements\": [\"${ns}\"]}, \"content\": {\"type\": \"NAMESPACE\", \"elements\": [\"${ns}\"]}}
      ]
    }" > /dev/null 2>&1 \
    && echo "  ✓ Namespace '${ns}' created." \
    || echo "  - Namespace '${ns}' may already exist."
}

create_namespace "default"
create_namespace "db"

echo "Spark Thrift Server started on port 10000"
tail -f ${SPARK_HOME}/logs/*
