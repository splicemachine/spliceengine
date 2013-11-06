#!/bin/bash

# Clean the Splice Machine database

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

CYGWIN=`uname -s`
if [[ ${CYGWIN} == CYGWIN* ]]; then
    PID=$(ps ax | grep -v grep | grep 'java' | awk '{print $1}')
    if [[ -n ${PID} ]]; then
        echo "Splice still running and must be shut down. Run stop-splice.sh"
        exit 1;
    fi
else
    # server still running - must stop first
    SPID=$(ps ax | grep -v grep | grep 'SpliceSinglePlatform' | awk '{print $1}')
    ZPID=$(ps ax | grep -v grep | grep 'ZooKeeperServerMain' | awk '{print $1}')
    if [[ -n ${SPID} || -n ${ZPID} ]]; then
        echo "Splice still running and must be shut down. Run stop-splice.sh"
        exit 1;
    fi
fi

/bin/rm -rf "${ROOT_DIR}"/db

if [[ ${CYGWIN} == CYGWIN* ]]; then
    /bin/rm -rf /tmp/hbase-*
fi
