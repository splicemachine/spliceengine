#!/bin/sh

HBASE_HOME=/Users/scottfines/apps/hbase

CLASSPATH="$HBASE_HOME/lib/*"
CLASSPATH="$CLASSPATH:$HBASE_HOME/*"

echo $CLASSPATH

TABLE=$1
DESTINATION_DIR=$2

java -cp $CLASSPATH com.splicemachine.hbase.debug.SpliceTableDebugger count $1 $2 

#Note: This only works if you are running it on a machine with hadoop installed and running
#Otherwise, it'll break with a "command not found"

#Make sure local file is cleaned up
LOCAL_FILE=./.temp_count
if [ -e $LOCAL_FILE ]; then
	rm $LOCAL_FILE
fi 

hdfs dfs -getmerge $DESTINATION_DIR $LOCAL_FILE
cat $LOCAL_FILE | grep -v FINISHED | tr '\n' '+' | sed -e 's/+$/\n/' | bc
