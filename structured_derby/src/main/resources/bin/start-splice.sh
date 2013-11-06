#!/bin/bash

# Start with debug logging by passing this script the "-debug" argument

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
LOGFILE="${ROOT_DIR}"/splice.log
DEBUG=$1
CYGWIN=`uname -s`

# server still running - must stop first
SPID=$(ps ax | grep -v grep | grep 'SpliceSinglePlatform' | awk '{print $1}')
if [[ ${CYGWIN} == CYGWIN* ]]; then
    SPID=$(ps ax | grep -v grep | grep 'java' | awk '{print $1}')
else
    ZPID=$(ps ax | grep -v grep | grep 'ZooKeeperServerMain' | awk '{print $1}')
fi
if [[ -n ${SPID} || -n ${ZPID} ]]; then
    echo "Splice still running and must be shut down. Run stop-splice.sh"
    exit 1;
fi

echo "Starting Splice Machine..."
echo "Log file is ${LOGFILE}"
echo "Waiting for Splice..."
maxRetries=3
# save exit value
rCode=0
for i in $(eval echo "{1..$maxRetries}"); do
    # debug
    #echo
    #echo "Try $i"
    ./bin/_start.sh "${ROOT_DIR}" "${LOGFILE}" "${DEBUG}"
    ./bin/waitfor.sh "${LOGFILE}"
    rCode=$?
    if [[ ${rCode} -eq 0 ]]; then
        echo "Splice Server is ready"
        exit 0;
    fi
    if [[ ${rCode} -eq 1 && ${i} -ne ${maxRetries} ]]; then
        # debug
        #echo
        #echo "Splice Server didn't start properly. Retrying..."
        #cp "$LOGFILE" "${LOGFILE}_$i"

        if [[ ${CYGWIN} == CYGWIN* ]]; then
            # We can only see if java is running on Cygwin - have to kill everything
            SPID=$(ps ax | grep -v grep | grep 'java' | awk '{print $1}')
            [[ -n ${SPID} ]] && for p in ${SPID}; do kill -15 `echo ${p}`; done
        else
            SPID=$(ps ax | grep -v grep | grep 'SpliceSinglePlatform' | awk '{print $1}')
            if [ -n "${SPID}" ]; then
                # kill splice, if running (usually not), but let zoo have time to config itself
                kill -15 ${SPID}
            fi
        fi
    fi
done

if [[ ${rCode} -ne 0 ]]; then
    SPID=$(ps ax | grep -v grep | grep 'SpliceSinglePlatform' | awk '{print $1}')
    if [[ ${CYGWIN} == CYGWIN* ]]; then
        SPID=$(ps ax | grep -v grep | grep 'java' | awk '{print $1}')
    else
        ZPID=$(ps ax | grep -v grep | grep 'ZooKeeperServerMain' | awk '{print $1}')
    fi
    if [[ -n ${SPID} || -n ${ZPID} ]]; then
        ./bin/_stop.sh
    fi
    echo
    echo "Server didn't start in expected amount of time. Please restart with the \"-debug\" option and check ${LOGFILE}." >&2
    exit 1;
fi
