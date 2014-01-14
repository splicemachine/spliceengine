#!/bin/bash

logLocationPat=$1
logFailedLine='Master not active after'
logReadyLine='Ready to accept connections'

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
    exit 2;
fi

if [ -z "${line}" ]; then
    # Still trying to start - unknown
    exit 1;
else
    # Started, ready - success
    exit 0;
fi
