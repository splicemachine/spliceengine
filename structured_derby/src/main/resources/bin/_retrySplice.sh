#!/bin/sh

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
# start zookeeper once
"${ROOT_DIR}"/bin/_startZoo.sh "${ROOT_DIR}" "${ZOOLOGFILE}" "${LOG4J_PATH}" "${ZOO_DIR}" "${CP}"
# Give zoo some time
sleep ${ZOO_WAIT_TIME}
maxRetries=3
rCode=0
for i in $(eval echo "{1..$maxRetries}"); do
    # splice/hbase will be retried several times to accommodate timeouts
    "${ROOT_DIR}"/bin/_startSplice.sh "${ROOT_DIR}" "${SPLICELOGFILE}" "${LOG4J_PATH}" "${ZOO_DIR}" "${HBASE_ROOT_DIR_URI}" "${CP}" "${SPLICE_MAIN_CLASS}" "${CHAOS}"
    "${ROOT_DIR}"/bin/waitfor.sh "${ROOT_DIR}" "${SPLICELOGFILE}"
    rCode=$?
    if [[ ${rCode} -eq 0 ]]; then
        echo "Splice Server is ready"
        exit 0;
    fi
    if [[ ${rCode} -eq 1 && ${i} -ne ${maxRetries} ]]; then

        if [[ -e "${ROOT_DIR}"/splice_pid ]]; then
            # kill splice, if running (usually not), but let zoo have time to config itself
            ${ROOT_DIR}/bin/_stop.sh "${ROOT_DIR}"/splice_pid 45
        fi
    fi
done

if [[ ${rCode} -ne 0 ]]; then
    if [[ -e "${ROOT_DIR}"/splice_pid || -e "${ROOT_DIR}"/zoo_pid ]]; then
        "${ROOT_DIR}"/bin/stop-splice.sh
    fi
    echo
    echo "Server didn't start in expected amount of time. Please restart with the \"-debug\" option and check ${SPLICELOGFILE}." >&2
    exit 1;
fi
