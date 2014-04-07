#!/bin/bash

PID_FILE="$1"
TIMEOUT="$2"

if [[ ! -e "${PID_FILE}" ]]; then
    echo "${PID_FILE} is not running."
    exit 0;
fi

KILL_PID=`cat "${PID_FILE}"`
KILL_PID=$(grep -oE "[0-9]+" <<< "$KILL_PID") # Make sure we get only digits

if [[ -n "${KILL_PID}" ]]; then

    ALIVE_PID=$(ps ax | grep -v grep | grep ${KILL_PID} | awk '{print $1}')
    if [[ -z "${ALIVE_PID}" || "${ALIVE_PID}" -ne "${KILL_PID}" ]]; then
        #echo "No process PID [${KILL_PID}] dead?"
        # clean up old pid file
        CYGWIN=`uname -s`
        if [[ ${CYGWIN} == CYGWIN* ]]; then
            /bin/rm -f "`cygpath --path --unix ${PID_FILE}`"
        else
            /bin/rm -f "${PID_FILE}"
        fi
        exit 1;
    fi

    if [[ -n "$KILL_PID" ]]; then
        kill -15 ${KILL_PID}
        sleep ${TIMEOUT}
    fi

    ALIVE_PID=$(ps ax | grep -v grep | grep ${KILL_PID} | awk '{print $1}')
    if [[ -n "$ALIVE_PID" ]]; then
        echo "Process didn't shut down. Trying again..."
        kill -9 ${KILL_PID}
    fi
else
    echo "Bad PID: [${KILL_PID}] in ${PID_FILE}"
    # clean up old pid file
    CYGWIN=`uname -s`
    if [[ ${CYGWIN} == CYGWIN* ]]; then
        /bin/rm -f "`cygpath --path --unix ${PID_FILE}`"
    else
        /bin/rm -f "${PID_FILE}"
    fi
    exit 1;
fi

# clean up old pid file
CYGWIN=`uname -s`
if [[ ${CYGWIN} == CYGWIN* ]]; then
    /bin/rm -f "`cygpath --path --unix ${PID_FILE}`"
else
    /bin/rm -f "${PID_FILE}"
fi
