#!/bin/bash

# Cloudera VM Splice Machine starter

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
# sets VM specific env vars
source "${ROOT_DIR}"/setEnv

echo "Checking the state of Splice Machine on $SPLICE_ENV"

curl -u admin:admin   'http://localhost:7180/api/v1/clusters/Cluster%201%20-%20CDH4/services/hbase1'
