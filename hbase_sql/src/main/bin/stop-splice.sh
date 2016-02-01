#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${ROOT_DIR}/bin/functions.sh

# shut down both zookeeper and splice/hbase
_stopServer "${ROOT_DIR}" "${ROOT_DIR}"

# Check for stragglers
SIG=15
S=$(ps -ef | awk '/SpliceTestPlatform/ && !/awk/ {print $2}')
if [[ -n ${S} ]]; then
    echo "Found SpliceTestPlatform straggler. Killing."
    for pid in ${S}; do
        kill -${SIG} ${pid}
    done
fi

Z=$(ps -ef | awk '/ZooKeeperServerMain/ && !/awk/ {print $2}')
if [[ -n ${Z} ]]; then
    echo "Found ZooKeeperServerMain straggler. Killing."
    for pid in ${Z}; do
        kill -${SIG} ${pid}
    done
fi