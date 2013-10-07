#!/bin/bash

echo "=== Running profile $1 === " > server.log

mvn -X exec:exec -e -DspliceCI -P $1 >> server.log &

if ps ax | grep -v grep | grep 'exec:exec' > /dev/null 
then
 sleep 120
else
 echo "splice is not running!!"
fi
