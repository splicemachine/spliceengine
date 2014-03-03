#!/bin/bash

ROOT_DIR=$1
PID_DIR=$2

echo "Shutting down Splice..."
# shut down splice/hbase
${ROOT_DIR}/bin/_stop.sh "${PID_DIR}"/splice_pid 45

echo "Shutting down Zookeeper..."
# shut down zookeeper
${ROOT_DIR}/bin/_stop.sh "${PID_DIR}"/zoo_pid 15
