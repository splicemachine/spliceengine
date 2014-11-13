#!/bin/bash

##################################################################################
# Start Zookeeper and the Splice HBase servers
# See usage() below.
##################################################################################

usage() {
    # $1 is an error, if any
    if [[ -n "${1}" ]]; then
        echo "Error: ${1}"
    fi
    echo "Usage: $0 -c -p [<hbase_profile>] -h[elp]"
    echo "Where: "
    echo "  -c is an optional flag determining random task failures. Default is that the chaos"
    echo "    monkey NOT run. To see if you have the chaos monkey running, execute: "
    echo "        grep 'task fail' splice_machine/splice.log"
    echo "  -p <hbase_profile> is the optional splice hbase platform to run.  One of:"
    echo "  cloudera-cdh4.5.0, cloudera-cdh4.3.0, hdp1.3, apache-hbase-0.94.9, mapr-0.94.9.  Default is cloudera-cdh4.5.0."
    echo "  -b Used by Jenkins when starting server; env var BUILD_TAG, eg \"jenkins-Splice-Continuous-Build-325\""
    echo "  -h => print this message"
}

CHAOS="false"
PROFILE="cloudera-cdh4.5.0"  # default hbase platform profile
BUILD_TAG=""

while getopts ":chp:b:" flag ; do
    case $flag in
        h* | \?)
            usage
            exit 0 # This is not an error, User asked help. Don't do "exit 1"
        ;;
        c)
        # start server with the chaos monkey (random task failures)
            CHAOS="true"
        ;;
        p)
        # the hbase profile
            PROFILE=$(echo "$OPTARG" | tr -d [[:space:]])
        ;;
        b)
        # Jenkins build tag
            BUILD_TAG=$(echo "$OPTARG" | tr -d [[:space:]])
        ;;
        ?)
            usage "Unknown option (ignored): ${OPTARG}"
            exit 1
        ;;
    esac
done

echo "Running with hbase profile \"${PROFILE}\" and chaos monkey = ${CHAOS} ${BUILD_TAG}"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"

pushd "${SCRIPT_DIR}/${PROFILE}/splice_machine_test" &>/dev/null

ROOT_DIR="$( pwd )"

# Config
SPLICELOG="${ROOT_DIR}"/splice_it.log
ZOOLOG="${ROOT_DIR}"/zoo_it.log

# Check if server running. Shut down if so.
# Doing this automatically so that running in batch mode, like ITs, works without problems.
S=$(ps -ef | awk '/SpliceTestPlatform}SpliceSinglePlatform/ && !/awk/ {print $2}')
Z=$(ps -ef | awk '/ZooKeeperServerMain/ && !/awk/ {print $2}')
if [[ -n ${S} || -n ${Z} ]]; then
    echo "Splice server is running. Shutting down."
    "${SCRIPT_DIR}"/stop-splice-dev.sh
fi

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')
echo "=== Running with hbase profile ${PROFILE} at ${currentDateTime} ${BUILD_TAG} === " > ${SPLICELOG}

# Start zookeeper in background.
mvn -B exec:exec -PspliceZoo > zoo_it.log 2>&1 &

# Start SpliceTestPlaform in background.
mvn -B exec:exec -PspliceFast -DfailTasksRandomly=${CHAOS} > splice_it.log 2>&1 &

# wait for splice to start
mvn -B exec:java -PspliceWait

popd &>/dev/null

