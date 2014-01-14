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

# Check if server running. If not, no need to proceed.
S=`jps | grep SpliceTestPlatform | grep -v grep  | awk '{print $1}'`
Z=`jps | grep ZooKeeperServerMain | grep -v grep  | awk '{print $1}'`
if [[ -z ${S} && -z ${Z} ]]; then
    echo "Splice server is not running."
    exit 0
fi

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')

echo "Shutting down splice at $currentDateTime" >> ${SPLICELOG}

# Check to see if we've built the package scripts. If not, we'll catch stragglers below.
if [[ -e "${ROOT_DIR}"/target/splicemachine/bin/_stopServer.sh ]]; then
    "${ROOT_DIR}"/target/splicemachine/bin/_stopServer.sh "${ROOT_DIR}/target/splicemachine" "${ROOT_DIR}/target/splicemachine"
fi

# Check for stragglers
SIG=15
S=`jps | grep SpliceTestPlatform | grep -v grep  | awk '{print $1}'`
[[ -n ${S} ]] && echo "Found SpliceTestPlatform straggler. Killing." && for pid in ${S}; do kill -${SIG} `echo ${pid}`; done
Z=`jps | grep ZooKeeperServerMain | grep -v grep  | awk '{print $1}'`
[[ -n ${Z} ]] && echo "Found ZooKeeperServerMain straggler. Killing." && for pid in ${Z}; do kill -${SIG} `echo ${pid}`; done

if [ ! -d "logs" ]; then
  mkdir logs
fi
currentDateTime=$(date +'%m-%d-%Y-%H_%M_%S')
cp ${SPLICELOG} logs/${currentDateTime}.$( basename "${SPLICELOG}")
cp ${ZOOLOG} logs/${currentDateTime}.$( basename "${ZOOLOG}")
cp ${DERBYLOG} logs/${currentDateTime}.$( basename "${DERBYLOG}")

popd &>/dev/null
