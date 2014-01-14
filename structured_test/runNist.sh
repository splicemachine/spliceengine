#!/bin/sh

if [[ -n "$1" ]]; then
    # The name of a Nist script, like "dml079" or "dml079.sql"
    SCRIPT="-Dscript=$1"
else
    SCRIPT=
fi

#
# Start splice server first
#
PID=$(ps ax | grep -v grep | grep 'SpliceTestPlatform' | awk '{print $1}')
if [ -z "$PID" ]; then
    echo "Splice server must be running."
else
    echo " *** Test output goes to target/nist/<sqlFile>.{splice|derby} ***"
    echo " *** Check NistTest.log for test comparison details when test finishes ***"
    mvn clean test -Dtest=TestNist $SCRIPT
fi
