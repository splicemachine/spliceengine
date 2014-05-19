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
    echo "  -n Given by Jenkins when stoping server; env var BUILD_NUMVER, eg \"225\"."
    echo "      Used to stamp log files."
    echo "Stop Zookeeper and Splice. Log files get timestamped and copied to the"
    echo "structured_derby/logs directory."
}

BUILD_NUMBER=""

while getopts ":hn:" flag ; do
    case $flag in
        h* | \?)
            usage
            exit 0 # This is not an error, User asked help. Don't do "exit 1"
        ;;
        n)
        # Jenkins build number to stamp logs
            BUILD_NUMBER=$(echo "${OPTARG}-" | tr -d [[:space:]])
        ;;
        ?)
            usage "Unknown option (ignored): ${OPTARG}"
            exit 1
        ;;
    esac
done

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
pushd "${SCRIPT_DIR}/structured_derby" &>/dev/null
ROOT_DIR="$( pwd )"
source ${ROOT_DIR}/target/classes/bin/functions.sh
SPLICELOG="${ROOT_DIR}"/splice.log
ZOOLOG="${ROOT_DIR}"/zoo.log
DERBYLOG="${ROOT_DIR}"/derby.log

# Check if server running. If not, no need to proceed.
S=$(ps -ef | awk '/SpliceTestPlatform/ && !/awk/ {print $2}')
Z=$(ps -ef | awk '/ZooKeeperServerMain/ && !/awk/ {print $2}')
if [[ -z ${S} && -z ${Z} ]]; then
    echo "Splice server is not running."
    exit 0
fi

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')

echo "Shutting down splice at $currentDateTime" >> ${SPLICELOG}

# Check to see if we've built the package scripts. If not, we'll catch stragglers below.
if [[ -e "${ROOT_DIR}"/target/splicemachine/bin/functions.sh ]]; then
    _stopServer "${ROOT_DIR}/target/splicemachine" "${ROOT_DIR}/target/splicemachine"
fi

# Check for stragglers
SIG=15
S=$(ps -ef | awk '/SpliceTestPlatform/ && !/awk/ {print $2}')
[[ -n ${S} ]] && echo "Found SpliceTestPlatform straggler. Killing." && for pid in ${S}; do kill -${SIG} `echo ${pid}`; done
Z=$(ps -ef | awk '/ZooKeeperServerMain/ && !/awk/ {print $2}')
[[ -n ${Z} ]] && echo "Found ZooKeeperServerMain straggler. Killing." && for pid in ${Z}; do kill -${SIG} `echo ${pid}`; done

if [ ! -d "logs" ]; then
  mkdir logs
fi

if [[ -z ${BUILD_NUMBER} ]]; then
    BUILD_NUMBER="${currentDateTime}-"
fi
cp ${SPLICELOG} logs/${BUILD_NUMBER}$( basename "${SPLICELOG}")
cp ${ZOOLOG} logs/${BUILD_NUMBER}$( basename "${ZOOLOG}")
cp ${DERBYLOG} logs/${BUILD_NUMBER}$( basename "${DERBYLOG}")

popd &>/dev/null
