#!/bin/bash

ROOT_DIR="${1}"
SPLICELOGFILE="${2}"
ZOOLOGFILE="${3}"
LOG4J_PATH="${4}"
ZOO_DIR="${5}"
ZOO_WAIT_TIME="${6}"
HBASE_ROOT_DIR_URI="${7}"
CP="${8}"
SPLICE_MAIN_CLASS="${9}"
CHAOS="${10}"

echo "Starting Splice Machine..."
echo "Log file is ${SPLICELOGFILE}"
echo "Waiting for Splice..."

# number of seconds we should allow for isReady to return 0
TIMEOUT=100
CYGWIN=`uname -s`
if [[ ${CYGWIN} == CYGWIN* ]]; then
    # be a more lenient with Cygwin
    TIMEOUT=200
fi

# start zookeeper once
"${ROOT_DIR}"/bin/_startZoo.sh "${ROOT_DIR}" "${ZOOLOGFILE}" "${LOG4J_PATH}" "${ZOO_DIR}" "${CP}"
# Give zoo some time
sleep ${ZOO_WAIT_TIME}
maxRetries=3
rCode=0
DONE_CODE=0
for i in $(eval echo "{1..$maxRetries}"); do
    # splice/hbase will be retried several times to accommodate timeouts
    "${ROOT_DIR}"/bin/_startSplice.sh "${ROOT_DIR}" "${SPLICELOGFILE}" "${LOG4J_PATH}" "${ZOO_DIR}" "${HBASE_ROOT_DIR_URI}" "${CP}" "${SPLICE_MAIN_CLASS}" "${CHAOS}"
    "${ROOT_DIR}"/bin/_waitfor.sh "${ROOT_DIR}" "${SPLICELOGFILE}" ${TIMEOUT}
    rCode=$?
    if [[ ${rCode} -eq 0 ]]; then
        DONE_CODE=0
        break
    fi
    if [[ ${rCode} -eq 1 && ${i} -ne ${maxRetries} ]]; then

        if [[ -e "${ROOT_DIR}"/splice_pid ]]; then
            # kill splice, if running (usually not), but let zoo have time to config itself
            ${ROOT_DIR}/bin/_stop.sh "${ROOT_DIR}"/splice_pid 45
            DONE_CODE=0
        fi
    fi
done

if [[ ${DONE_CODE} -ne 0 ]]; then
    if [[ -e "${ROOT_DIR}"/splice_pid || -e "${ROOT_DIR}"/zoo_pid ]]; then
        "${ROOT_DIR}"/bin/stop-splice.sh
    fi
    echo "Splice didn't start as expected. Please restart with the \"-d\" option and check ${SPLICELOGFILE}." >&2
    exit 1;
else
    echo "Splice Server is ready"
    echo "The HBase URI is http://localhost:60010"
    echo "The JDBC URI is jdbc:splice://localhost:1527/splicedb"
    exit 0;
fi
