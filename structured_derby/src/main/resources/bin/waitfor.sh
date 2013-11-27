#!/bin/bash

LOGFILE="$1"

# number of seconds we should allow for isReady to return 0
timeout=100
CYGWIN=`uname -s`
if [[ ${CYGWIN} == CYGWIN* ]]; then
    # be a more lenient with Cygwin
    timeout=200
fi
# number of seconds we should wait between check on ready status
interval=2
((t = timeout))
while ((t > 0)); do
    # Show something while we wait
    echo -ne "."
    sleep ${interval}

    ./bin/is-ready.sh "${LOGFILE}"
    # save exit value
    rCode=$?

    # debug
    #echo "RCode: ${rCode}"

    if [[ ${rCode} -eq 0 ]]; then
        # started
        echo
        exit 0;
    fi
    if [[ ${rCode} -eq 2 ]]; then
        # error starting but processes running - return to retry
        exit 1;
    fi
    #if [[ ${rCode} -eq 1 ]]; then
    # Still coming up... continue;

    ((t -= interval))
done
# did not start in allotted timeout - error
exit 1;
