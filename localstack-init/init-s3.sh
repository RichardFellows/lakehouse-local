#!/bin/bash
# Runs automatically when LocalStack is ready
echo "Creating S3 buckets..."
awslocal s3 mb s3://warehouse
awslocal s3 mb s3://raw-data
echo "S3 buckets created:"
awslocal s3 ls
