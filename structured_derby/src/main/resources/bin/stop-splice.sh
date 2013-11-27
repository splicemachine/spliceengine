#!/bin/bash

echo
echo "Shutting down Splice Machine..."
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# shut down splice/hbase
${ROOT_DIR}/bin/_stop.sh "${ROOT_DIR}"/splice_pid 45

# shut down zookeeper
${ROOT_DIR}/bin/_stop.sh "${ROOT_DIR}"/zoo_pid 15
