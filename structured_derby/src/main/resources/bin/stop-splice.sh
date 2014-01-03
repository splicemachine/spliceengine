#!/bin/bash

echo "Shutting down Splice Machine..."
ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# shut down both zookeeper and splice/hbase
${ROOT_DIR}/bin/_stopServer.sh "${ROOT_DIR}" "${ROOT_DIR}"
