#!/bin/bash

ROOT_DIR="$1"
LOGFILE="$2"

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

    "${ROOT_DIR}"/bin/is-ready.sh "${LOGFILE}"
    # save exit value
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
