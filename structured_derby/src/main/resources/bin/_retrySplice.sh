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

# number of seconds we should allow for isReady to return 0
HBASE_TIMEOUT=100
# Zookeeper takes very little time
ZOO_TIMEOUT=15
CYGWIN=`uname -s`
if [[ ${CYGWIN} == CYGWIN* ]]; then
    # be a more lenient with Cygwin
    HBASE_TIMEOUT=200
    ZOO_TIMEOUT=30
fi

echo "Starting Splice Machine..."
echo "Log file is ${SPLICELOGFILE}"
echo "Waiting for Splice..."

maxRetries=3

# Start Zookeeper
RETURN_CODE=0
ERROR_CODE=0
for i in $(eval echo "{1..$maxRetries}"); do
    "${ROOT_DIR}"/bin/_startZoo.sh "${ROOT_DIR}" "${ZOOLOGFILE}" "${LOG4J_PATH}" "${ZOO_DIR}" "${CP}"
    "${ROOT_DIR}"/bin/_waitfor.sh "${ZOOLOGFILE}" "${ZOO_TIMEOUT}" 'Snapshotting'
    RETURN_CODE=$?
    if [[ ${RETURN_CODE} -eq 0 ]]; then
        ERROR_CODE=0
        break
    else
        if [[ ${i} -lt ${maxRetries} ]]; then
            if [[ -e "${ROOT_DIR}"/zoo_pid ]]; then
                ${ROOT_DIR}/bin/_stop.sh "${ROOT_DIR}"/zoo_pid 45
            fi
        else
            ERROR_CODE=1
        fi
    fi
done

# Start HBase
if [[ ${ERROR_CODE} -eq 0 ]]; then
    for i in $(eval echo "{1..$maxRetries}"); do
        # splice/hbase will be retried several times to accommodate timeouts
        "${ROOT_DIR}"/bin/_startSplice.sh "${ROOT_DIR}" "${SPLICELOGFILE}" "${LOG4J_PATH}" "${ZOO_DIR}" "${HBASE_ROOT_DIR_URI}" "${CP}" "${SPLICE_MAIN_CLASS}" "${CHAOS}"
        if [[ ${i} -eq 1 ]]; then
            # We can only check for error msg the first time, else we'll see the same ones again
            "${ROOT_DIR}"/bin/_waitfor.sh "${SPLICELOGFILE}" "${HBASE_TIMEOUT}" 'Ready to accept connections' 'Master not active after'
        else
            "${ROOT_DIR}"/bin/_waitfor.sh "${SPLICELOGFILE}" "${HBASE_TIMEOUT}" 'Ready to accept connections'
        fi
        RETURN_CODE=$?
        if [[ ${RETURN_CODE} -eq 0 ]]; then
            ERROR_CODE=0
            break
        else
            if [[ ${i} -lt ${maxRetries} ]]; then
                if [[ -e "${ROOT_DIR}"/splice_pid ]]; then
                    ${ROOT_DIR}/bin/_stop.sh "${ROOT_DIR}"/splice_pid 45
                fi
            else
                ERROR_CODE=1
            fi
        fi
    done
fi

if [[ ${ERROR_CODE} -ne 0 ]]; then
    if [[ -e "${ROOT_DIR}"/splice_pid || -e "${ROOT_DIR}"/zoo_pid ]]; then
        "${ROOT_DIR}"/bin/stop-splice.sh
    fi
    echo
    echo "Splice didn't start as expected. Please restart with the \"-d\" option and check ${SPLICELOGFILE}." >&2
    exit 1;
else
    echo
    echo "Splice Server is ready"
    echo "The HBase URI is http://localhost:60010"
    echo "The JDBC URI is jdbc:splice://localhost:1527/splicedb"
    exit 0;
fi
