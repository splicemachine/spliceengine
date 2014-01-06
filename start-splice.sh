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
    echo "Usage: $0 [true|false] [<hbase_profile>] -h[elp]"
    echo "Where: "
    echo "  true|false is an optional flag determining random task failures. Default is false."
    echo "  <hbase_profile> is the optional splice hbase platform to run.  One of:"
    echo "    cloudera-cdh4.3.0, hdp1.3, apache-hbase-0.94.5, mapr-0.94.5. Default is cloudera-cdh4.3.0."
    echo "  -h => print this message"
}

if [[ ${1} == -h* ]]; then
    usage
    exit 0 # This is not an error, User asked help. Don't do "exit 1"
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
pushd "${SCRIPT_DIR}/structured_derby" &>/dev/null
ROOT_DIR="$( pwd )"

CHAOS=${1}
shopt -s nocasematch
if [[ -z "${CHAOS}" || "${CHAOS}" != "true" ]]; then
    # default is NOT to run chaos monkey
    CHAOS="FALSE"
else
    CHAOS="TRUE"
fi
shopt -u nocasematch

PROFILE=${2}
if [[ -z "${PROFILE}" ]]; then
    # default profile
    PROFILE="cloudera-cdh4.3.0"
fi

TARBALL="${ROOT_DIR}"/target/splice_machine-0.5rc6-SNAPSHOT-${PROFILE}_simple.tar.gz
# fail if wrong profile was provided
if [[ ! -e ${TARBALL} ]]; then
    usage "Cannot find ${TARBALL}. An unexpected profile was provided \\"${PROFILE}\\" or the project needs to be built."
    exit 1
fi

# Extract package libs for classpath and bin scripts to call
tar xvf ${TARBALL} -C "${ROOT_DIR}"/target splicemachine/lib &>/dev/null
tar xvf ${TARBALL} -C "${ROOT_DIR}"/target splicemachine/bin &>/dev/null

# Config
SPLICELOG="${ROOT_DIR}"/splice.log
ZOOLOG="${ROOT_DIR}"/zoo.log
CLASSPATH="${ROOT_DIR}"/target/splicemachine/lib/*
ZOO_DIR="${ROOT_DIR}"/target/zookeeper
HBASE_ROOT_DIR_URI="file://${ROOT_DIR}/target/hbase"
LOG4J_PATH="file:${ROOT_DIR}/target/classes/hbase-log4j.properties"

# Check if server running. Shut down if so.
# Doing this automatically so that running in batch mode, like ITs, works without problems.
S=`jps | grep SpliceTestPlatform | grep -v grep  | awk '{print $1}'`
Z=`jps | grep ZooKeeperServerMain | grep -v grep  | awk '{print $1}'`
if [[ -n ${S} || -n ${Z} ]]; then
    echo "Splice server is running. Shutting down."
    "${SCRIPT_DIR}"/stop-splice.sh
fi

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')
echo "=== Running profile ${PROFILE} at $currentDateTime === " > ${SPLICELOG}

export SPLICE_SYS_ARGS="-Xdebug \
   -Dcom.sun.management.jmxremote.ssl=false \
   -Dcom.sun.management.jmxremote.authenticate=false \
   -Dcom.sun.management.jmxremote.port=10102 \
   -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4000"
ZOO_WAIT_TIME=45
SPLICE_MAIN_CLASS="com.splicemachine.test.SpliceTestPlatform"
# Start server with retry logic
#echo "${ROOT_DIR}/target/classes" "${SPLICELOG}" "${ZOOLOG}" "${LOG4J_PATH}" "${ZOO_DIR}" "${ZOO_WAIT_TIME}" "${HBASE_ROOT_DIR_URI}" "${CLASSPATH}" "${SPLICE_MAIN_CLASS}" "${CHAOS}"
"${ROOT_DIR}"/target/splicemachine/bin/_retrySplice.sh "${ROOT_DIR}/target/splicemachine" "${SPLICELOG}" "${ZOOLOG}" "${LOG4J_PATH}" "${ZOO_DIR}" "${ZOO_WAIT_TIME}" "${HBASE_ROOT_DIR_URI}" "${CLASSPATH}" "${SPLICE_MAIN_CLASS}" "${CHAOS}"

popd &>/dev/null

