#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
pushd "${SCRIPT_DIR}/structured_derby" &>/dev/null
ROOT_DIR="$( pwd )"

CHAOS=$1
shopt -s nocasematch
if [[ -z "${CHAOS}" || "${CHAOS}" != "true" ]]; then
    # default is NOT to run chaos monkey
    CHAOS="FALSE"
else
    CHAOS="TRUE"
fi
shopt -u nocasematch

PROFILE=$2
if [[ -z "${PROFILE}" ]]; then
    # default profile
    PROFILE="cloudera-cdh4.3.0"
fi

TARBALL="${ROOT_DIR}"/target/splice_machine-0.5rc6-SNAPSHOT-${PROFILE}_simple.tar.gz
# fail if wrong profile was provided
if [[ ! -e ${TARBALL} ]]; then
    echo "Cannot find ${TARBALL}"
    echo "An unexpected profile was provided \\"${PROFILE}\\""
    echo "Cannot continue"
    exit 1
fi

# Make package scrips executable
chmod 0755 "${ROOT_DIR}"/target/classes/bin/*sh
# Extract libs for classpath
tar xvf ${TARBALL} -C "${ROOT_DIR}"/target splicemachine/lib &>/dev/null
# Config
SPLICELOG="${ROOT_DIR}"/splice.log
ZOOLOG="${ROOT_DIR}"/zoo.log
CLASSPATH="${ROOT_DIR}"/target/splicemachine/lib/*
ZOO_DIR="${ROOT_DIR}"/target/zookeeper
HBASE_ROOT_DIR_URI="file://${ROOT_DIR}/target/hbase"
LOG4J_PATH="file:${ROOT_DIR}/target/classes/hbase-log4j.properties"

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')
echo "=== Running profile ${PROFILE} at $currentDateTime === " > ${SPLICELOG}

# Start server with retry logic
ZOO_WAIT_TIME=45
SPLICE_MAIN_CLASS="com.splicemachine.test.SpliceTestPlatform"
#echo "${ROOT_DIR}/target/classes" "${SPLICELOG}" "${ZOOLOG}" "${LOG4J_PATH}" "${ZOO_DIR}" "${ZOO_WAIT_TIME}" "${HBASE_ROOT_DIR_URI}" "${CLASSPATH}" "${SPLICE_MAIN_CLASS}" "${CHAOS}"
"${ROOT_DIR}"/target/classes/bin/_retrySplice.sh "${ROOT_DIR}/target/classes" "${SPLICELOG}" "${ZOOLOG}" "${LOG4J_PATH}" "${ZOO_DIR}" "${ZOO_WAIT_TIME}" "${HBASE_ROOT_DIR_URI}" "${CLASSPATH}" "${SPLICE_MAIN_CLASS}" "${CHAOS}"

popd &>/dev/null

