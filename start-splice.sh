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
    echo "    cloudera-cdh4.3.0, hdp1.3, apache-hbase-0.94.5, mapr-0.94.5, mapr-0.94.9. Default is cloudera-cdh4.3.0."
    echo "  -b Used by Jenkins when starting server; env var BUILD_TAG, eg \"jenkins-Splice-Continuous-Build-325\""
    echo "  -h => print this message"
}

CHAOS="FALSE"
PROFILE="cloudera-cdh4.3.0"  # default hbase platform profile
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

echo "Runing with hbase profile \"${PROFILE}\" and chaos monkey = ${CHAOS} ${BUILD_TAG}"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
pushd "${SCRIPT_DIR}/structured_derby" &>/dev/null
ROOT_DIR="$( pwd )"

SPLICE_VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -Ev '(^\[|INFO|Download)' | tr -d [[:space:]])
TARBALL="${ROOT_DIR}"/target/splice_machine-${SPLICE_VERSION}-${PROFILE}_simple.tar.gz
if [[ ! -e ${TARBALL} ]]; then
    # Maven assembly is required for server dependencies and executable start/stop scripts.
    # It's not present. Attempt to build assembly (only if it's not already built).
    echo "Required assembly, ${TARBALL}, not found. Running maven assembly."
    mvn package -Dhbase.profile=${PROFILE} -Passemble -DskipTests  &>/dev/null
fi
# fail if wrong profile was provided
if [[ ! -e ${TARBALL} ]]; then
    usage "Cannot find ${TARBALL}. An unexpected hbase profile was provided \\"${PROFILE}\\" or the project needs to be built."
    exit 1
fi

# Extract package libs for sever classpath and bin scripts to call for start/stop
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
echo "=== Running with hbase profile ${PROFILE} at $currentDateTime ${BUILD_TAG} === " > ${SPLICELOG}

export SPLICE_SYS_ARGS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4000"
ZOO_WAIT_TIME=45
SPLICE_MAIN_CLASS="com.splicemachine.test.SpliceTestPlatform"
# Start server with retry logic
#echo "${ROOT_DIR}/target/classes" "${SPLICELOG}" "${ZOOLOG}" "${LOG4J_PATH}" "${ZOO_DIR}" "${ZOO_WAIT_TIME}" "${HBASE_ROOT_DIR_URI}" "${CLASSPATH}" "${SPLICE_MAIN_CLASS}" "${CHAOS}"
"${ROOT_DIR}"/target/splicemachine/bin/_retrySplice.sh "${ROOT_DIR}/target/splicemachine" "${SPLICELOG}" "${ZOOLOG}" "${LOG4J_PATH}" "${ZOO_DIR}" "${ZOO_WAIT_TIME}" "${HBASE_ROOT_DIR_URI}" "${CLASSPATH}" "${SPLICE_MAIN_CLASS}" "${CHAOS}"

popd &>/dev/null

