#!/bin/bash

# Cloudera VM Splice Machine restarter

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
# sets VM specific env vars
source "${ROOT_DIR}"/setEnv

echo "Restarting Splice Machine on $SPLICE_ENV"

curl -X POST -u admin:admin 'http://localhost:7180/api/v1/clusters/Cluster%201%20-%20CDH4/services/hbase1/commands/restart'

echo "To check the state of the active command:"
echo "> curl -u admin:admin 'http://localhost:7180/api/v1/commands/<cmdId>'"
