#!/bin/bash
#
./bin/ycsb load s3 -p table=ssd-bench-bucket -p s3.endPoint=localhost:9000 -p s3.accessKeyId=minioadmin -p s3.secretKey=redbull64 -p fieldlength=10 -p fieldcount=20 -P workloads/workloada
