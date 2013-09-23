#!/bin/bash

mvn exec:exec -DspliceCI > server.log &

if ps ax | grep -v grep | grep 'exec:exec' > /dev/null 
then
 sleep 120
else
 echo "splice is not running!!"
fi
