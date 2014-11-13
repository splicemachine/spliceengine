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
    echo "splice_machine/logs directory."
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

pushd "${SCRIPT_DIR}/splice_machine" &>/dev/null

ROOT_DIR="$( pwd )"

# Config
SPLICELOG="${ROOT_DIR}"/splice_it.log
ZOOLOG="${ROOT_DIR}"/zoo_it.log

# Check if server running. If not, no need to proceed.
S=$(ps -ef | awk '/SpliceTestPlatform|SpliceSinglePlatform/ && !/awk/ {print $2}')
Z=$(ps -ef | awk '/ZooKeeperServerMain/ && !/awk/ {print $2}')
if [[ -z ${S} && -z ${Z} ]]; then
    echo "Splice server is not running."
    exit 0
else
# otherwise kill it nicely
	SIG=15
	[[ -n ${S} ]] && echo "Found SpliceTestPlatform straggler. Killing." && for pid in ${S}; do kill -${SIG} `echo ${pid}`; done
	[[ -n ${Z} ]] && echo "Found ZooKeeperServerMain straggler. Killing." && for pid in ${Z}; do kill -${SIG} `echo ${pid}`; done
fi

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')
echo "Shutting down splice at $currentDateTime" >> ${SPLICELOG}

# Check for stragglers and kill 'em dead
SIG=9
S=$(ps -ef | awk '/SpliceTestPlatform/ && !/awk/ {print $2}')
Z=$(ps -ef | awk '/ZooKeeperServerMain/ && !/awk/ {print $2}')
[[ -n ${S} ]] && echo "Found SpliceTestPlatform straggler. Killing." && for pid in ${S}; do kill -${SIG} `echo ${pid}`; done
[[ -n ${Z} ]] && echo "Found ZooKeeperServerMain straggler. Killing." && for pid in ${Z}; do kill -${SIG} `echo ${pid}`; done

popd &>/dev/null
