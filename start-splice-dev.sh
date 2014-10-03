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
    echo "        grep 'task fail' structured_derby/splice.log"
    echo "  -p <hbase_profile> is the optional splice hbase platform to run.  One of:"
    echo "  cloudera-cdh4.5.0, cloudera-cdh4.3.0, hdp1.3, apache-hbase-0.94.9, mapr-0.94.9.  Default is cloudera-cdh4.5.0."
    echo "  -b Used by Jenkins when starting server; env var BUILD_TAG, eg \"jenkins-Splice-Continuous-Build-325\""
    echo "  -h => print this message"
}

CHAOS="FALSE"
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
            CHAOS="TRUE"
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
pushd "${SCRIPT_DIR}/structured_derby" &>/dev/null
ROOT_DIR="$( pwd )"

source ${ROOT_DIR}/src/main/bin/functions.sh

SPLICE_SINGLE_PATTERN="${ROOT_DIR}/target/splice_machine-*-${PROFILE}_simple.tar.gz"
TARBALL=`ls ${SPLICE_SINGLE_PATTERN}`
if [[ ! -f "${TARBALL}" ]]; then
    # Maven simple.tar.gz assembly is required to reference server dependencies.
    # If it's not present, quit.
    echo "Required assembly, ${TARBALL}, not found. An unexpected hbase profile was provided \\"${PROFILE}\\" or the project needs to be built."
    exit 1
fi

# Extract package libs for sever dependencies in classpath
tar xvf ${TARBALL} -C "${ROOT_DIR}"/target splicemachine/lib &>/dev/null

# Config
SPLICELOG="${ROOT_DIR}"/splice.log
ZOOLOG="${ROOT_DIR}"/zoo.log
CLASSPATH="${ROOT_DIR}"/target/splicemachine/lib/*
ZOO_DIR="${ROOT_DIR}"/target/zookeeper
HBASE_ROOT_DIR_URI="file://${ROOT_DIR}/target/hbase"
LOG4J_PATH="file:${ROOT_DIR}/target/classes/hbase-log4j.properties"

# Check if server running. Shut down if so.
# Doing this automatically so that running in batch mode, like ITs, works without problems.
S=$(ps -ef | awk '/SpliceTestPlatform/ && !/awk/ {print $2}')
Z=$(ps -ef | awk '/ZooKeeperServerMain/ && !/awk/ {print $2}')
if [[ -n ${S} || -n ${Z} ]]; then
    echo "Splice server is running. Shutting down."
    "${SCRIPT_DIR}"/stop-splice-dev.sh
fi

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')
echo "=== Running with hbase profile ${PROFILE} at $currentDateTime ${BUILD_TAG} === " > ${SPLICELOG}
spliceJar=`ls ${ROOT_DIR}/target/splicemachine/lib/splice_machine-*-${PROFILE}.jar`
cp "${ROOT_DIR}/conf/splice-site.xml" ./
jar -uf $spliceJar splice-site.xml
rm splice-site.xml
export SPLICE_SYS_ARGS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4000"
ZOO_WAIT_TIME=45
# This is the class we start in dev env
SPLICE_MAIN_CLASS="com.splicemachine.test.SpliceTestPlatform"
# Start server with retry logic
#echo "${ROOT_DIR}/target/classes" "${SPLICELOG}" "${ZOOLOG}" "${LOG4J_PATH}" "${ZOO_DIR}" "${ZOO_WAIT_TIME}" "${HBASE_ROOT_DIR_URI}" "${CLASSPATH}" "${SPLICE_MAIN_CLASS}" "${CHAOS}"
_retrySplice "${ROOT_DIR}/target/splicemachine" "${SPLICELOG}" "${ZOOLOG}" "${LOG4J_PATH}" "${ZOO_DIR}" "${ZOO_WAIT_TIME}" "${HBASE_ROOT_DIR_URI}" "${CLASSPATH}" "${SPLICE_MAIN_CLASS}" "${CHAOS}"

popd &>/dev/null

