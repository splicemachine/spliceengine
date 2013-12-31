#!/bin/bash

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')
echo "=== Running profile $1 at $currentDateTime === " > server.log

mvn -o -X exec:exec -e -DspliceCI -P $1 -DfailTasksRandomly=$2>> server.log &

if ps ax | grep -v grep | grep 'exec:exec' > /dev/null
then
 sleep 120
else
 echo "splice is not running!!"
fi
