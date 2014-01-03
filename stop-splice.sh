#!/bin/bash

##################################################################################
# Stop Zookeeper and the Splice HBase servers
# See usage() below.
##################################################################################

usage() {
    # $1 is an error, if any
    if [[ -n "${1}" ]]; then
        echo "Error: ${1}"
    fi
    echo "Usage: $0 [-h[elp]]"
    echo "Where: "
    echo "  -h => print this message"
    echo "Stop Zookeeper and Splice. Log files get timestamped and copied to the"
    echo "structured_derby/logs directory."
}

if [[ ${1} == -h* ]]; then
    usage
    exit 0 # This is not an error, User asked help. Don't do "exit 1"
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
pushd "${SCRIPT_DIR}/structured_derby" &>/dev/null
ROOT_DIR="$( pwd )"

SPLICELOG="${ROOT_DIR}"/splice.log
ZOOLOG="${ROOT_DIR}"/zoo.log
DERBYLOG="${ROOT_DIR}"/derby.log

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')

echo "Shutting down splice at $currentDateTime" >> ${SPLICELOG}

"${ROOT_DIR}"/target/classes/bin/_stopServer.sh "${ROOT_DIR}/target/classes" "${ROOT_DIR}/target/classes"

if [ ! -d "logs" ]; then
  mkdir logs
fi
currentDateTime=$(date +'%m-%d-%Y-%H_%M_%S')
cp ${SPLICELOG} logs/${currentDateTime}.$( basename "${SPLICELOG}")
cp ${ZOOLOG} logs/${currentDateTime}.$( basename "${ZOOLOG}")
cp ${DERBYLOG} logs/${currentDateTime}.$( basename "${DERBYLOG}")

popd &>/dev/null
