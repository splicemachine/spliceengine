#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"

if [[ -n "$1" ]]; then
    # The name of a Nist script, like "dml079" or "dml079.sql"
    SCRIPT="-Dscript=$1"
else
    SCRIPT=
fi

#
# Start splice server first
#
SERVER_WAS_RUNNING=false
PID=$(ps ax | grep -v grep | grep 'SpliceTestPlatform' | awk '{print $1}')
if [ -z "$PID" ]; then
    echo "Splice server must be running. Starting server..."
    SERVER_WAS_RUNNING=true
    ${SCRIPT_DIR}/../start-splice.sh
fi

echo " *** Test output goes to target/nist/<sqlFile>.{splice|derby} ***"
echo " *** Check NistTest.log for test comparison details when test finishes ***"
mvn clean test -Dtest=TestNist ${SCRIPT}

if [[ "${SERVER_WAS_RUNNING}" = true ]]; then
    # only perform shutdown on a serve we started
    echo "Shutting down server..."
    ${SCRIPT_DIR}/../stop-splice.sh
fi

