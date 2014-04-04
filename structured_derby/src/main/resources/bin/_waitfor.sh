#!/bin/bash

_isready() {
    logLocationPat=$1
    logFailedLine=$2
    logReadyLine=$3

    OLDIFS=$IFS
    IFS=$'\n'
    # Failed
    failed=`grep "${logFailedLine}" "$logLocationPat"`
    # Ready
    line=`grep "${logReadyLine}" "$logLocationPat"`
    IFS=${OLDIFS}

    if [ -n "${failed}" ]; then
        # Error in log - fail
        #echo "Failed: ${failed}"
        return 2;
    fi

    if [ -z "${line}" ]; then
        # Still trying to start - unknown
        return 1;
    else
        # Started, ready - success
        return 0;
    fi
}

ROOT_DIR="$1"
LOGFILE="$2"
TIMEOUT=$3

# number of seconds we should wait between check on ready status
interval=2
((t = TIMEOUT))
while ((t > 0)); do
    # Show something while we wait
    echo -ne "."
    sleep ${interval}

    _isready "${LOGFILE}" 'Master not active after' 'Ready to accept connections'
    # save return value
    rCode=$?

    if [[ ${rCode} -eq 0 ]]; then
        # started
        echo
        exit 0;
    fi
    if [[ ${rCode} -eq 2 ]]; then
        # error starting but processes running - return to retry
        exit 1;
    fi
    # Still coming up... continue;

    ((t -= interval))
done
# did not start in allotted timeout - error
exit 1;
