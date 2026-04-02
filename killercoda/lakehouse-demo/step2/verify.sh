#!/bin/bash
curl -sf http://localhost:8082/health > /dev/null 2>&1 && exit 0 || exit 1
