#!/bin/bash

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
source ${ROOT_DIR}/bin/functions.sh

# Clean the Splice Machine database

if [[ ${UNAME} == CYGWIN* ]]; then
	PID=$(ps -ef | awk '/java/ && !/awk/ {print $2}')
    if [[ -n ${PID} ]]; then
        echo "Splice still running and must be shut down. Run stop-splice.sh"
        exit 1;
    fi
else
    # server still running - must stop first
	SPID=$(ps -ef | awk '/SpliceSinglePlatform/ && !/awk/ {print $2}')
	ZPID=$(ps -ef | awk '/ZooKeeperServerMain/ && !/awk/ {print $2}')
    if [[ -n ${SPID} || -n ${ZPID} ]]; then
        echo "Splice still running and must be shut down. Run stop-splice.sh"
        exit 1;
    fi
fi

/bin/rm -rf "${ROOT_DIR}"/db

if [[ ${UNAME} == CYGWIN* ]]; then
    # Clean up for Cygwin
	filelist=( hbase-"${USER}" hsperfdata_\* \*_master_\* \*_regionserver_\* )
	for file in ${filelist[@]}; do
		/bin/rm -rf /cygdrive/c/tmp/${file}
	done
fi
