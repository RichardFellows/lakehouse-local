#!/bin/bash
# Verify Spark Thrift Server is running
docker compose exec spark bash -c "echo > /dev/tcp/localhost/10000" 2>/dev/null && exit 0 || exit 1
