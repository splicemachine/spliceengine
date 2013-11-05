#!/bin/bash

LOGFILE="$1"

timeout=100
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
