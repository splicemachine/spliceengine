#!/bin/bash

logLocationPat=$1
logFailedLine='Master not active after'
logReadyLine='Ready to accept connections'

echo "Waiting for Splice..."
# Keep checking until success or killed
while [ true ]; do
    # Show something while we wait
    echo -ne "."
    sleep 2

    OLDIFS=$IFS
    IFS=$'\n'
    # Failed
    failed=(`grep logFailedLine "$logLocationPat"`)
    # Ready
    line=(`grep $logReadyLine "$logLocationPat"`)
    IFS=$OLDIFS

    # not be started
    if [ ! -z "$failed" ]; then
        echo ""
        echo "Splice failed to start: $failed" >&2
        exit -1;
    fi

    # not be started
    if [ -z "$line" ]; then
        continue;
    else
        echo ""
        echo "Ready"
        exit 0;
    fi
done
