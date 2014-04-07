#!/bin/bash

LOGFILE="$1"
TIMEOUT="$2"
LOG_SUCCESS="$3"
LOG_FAILED="$4"
if [[ ${TIMEOUT} -le 0 ]]; then
    echo "TIMEOUT value must be positive"
    exit 1;
fi

# number of seconds we should wait between checks on clean status
INTERVAL=2
# total number of seconds we should wait for clean status
((t = TIMEOUT))
while ((t > 0)); do
    # Poll the log to check for success msg
    OLDIFS=$IFS
    IFS=$'\n'
    if [[ -n ${LOG_FAILED} ]]; then
        # Failed
        FAILED=`grep "${LOG_FAILED}" "${LOGFILE}"`
        if [ -n "${FAILED}" ]; then
            # Error in log - fail
            IFS=${OLDIFS}
            exit 1;
        fi
    fi

    # Ready
    SUCCESS=`grep "${LOG_SUCCESS}" "${LOGFILE}"`
    IFS=${OLDIFS}
    if [ -n "${SUCCESS}" ]; then
        # Started, ready - success
        exit 0;
    fi

    # Show something while we wait
    echo -ne "."
    sleep ${INTERVAL}

    # Still coming up... continue;
    ((t -= INTERVAL))
done
# did not start in allotted timeout - error
exit 1;
