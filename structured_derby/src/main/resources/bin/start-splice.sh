#!/bin/bash

# Start with debug logging by passing this script the "-debug" argument

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
LOGFILE="${ROOT_DIR}"/splice.log
DEBUG=$1

# server still running? - must stop first
if [[ -e "${ROOT_DIR}"/splice_pid || -e "${ROOT_DIR}"/zoo_pid ]]; then
    echo "Splice still running and must be shut down. Run stop-splice.sh"
    exit 1;
fi

echo "Starting Splice Machine..."
echo "Log file is ${LOGFILE}"
echo "Waiting for Splice..."
maxRetries=3
CYGWIN=`uname -s`
if [[ ${CYGWIN} == CYGWIN* ]]; then
    # cygwin likes to write in 2 places for /tmp
    # we'll symlink them
    if [[ -e "/tmp" && ! -L "/tmp" ]]; then
        mv "/tmp" "/tmp_bak"
        ln -s "/cygdrive/c/tmp" "/tmp"
    fi
fi
# start zookeeper once
"${ROOT_DIR}"/bin/_startZoo.sh "${ROOT_DIR}" "${LOGFILE}" "${DEBUG}"
rCode=0
for i in $(eval echo "{1..$maxRetries}"); do
    # splice/hbase will be retried several times to accommodate timeouts
    "${ROOT_DIR}"/bin/_startSplice.sh "${ROOT_DIR}" "${LOGFILE}" "${DEBUG}"
    "${ROOT_DIR}"/bin/waitfor.sh "${ROOT_DIR}" "${LOGFILE}"
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
    echo "Server didn't start in expected amount of time. Please restart with the \"-debug\" option and check ${LOGFILE}." >&2
    exit 1;
fi
