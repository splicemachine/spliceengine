#!/bin/sh

HBASE_HOME=/Users/scottfines/apps/hbase

CLASSPATH="$HBASE_HOME/lib/*"
CLASSPATH="$CLASSPATH:$HBASE_HOME/*"

echo $CLASSPATH

TABLE=$1
DESTINATION_DIR=$2

java -cp $CLASSPATH com.splicemachine.hbase.debug.SpliceTableDebugger tcount $1 $2 

#Copy to local file, and begin doing local analysis
LOCAL_FILE=./.temp_tcount
LOCAL_FILE_2=./.temp_tcount_2

if [ -e $LOCAL_FILE ]; then
	rm $LOCAL_FILE
fi

if [ -e $LOCAL_FILE_2 ]; then
	rm $LOCAL_FILE_2
fi

hdfs dfs -getmerge $DESTINATION_DIR $LOCAL_FILE
cat $LOCAL_FILE | grep -P "\t" | sort -n > $LOCAL_FILE_2

#remove the intermediate messy data
mv $LOCAL_FILE_2 $LOCAL_FILE

#generate a rollup count
cat $LOCAL_FILE | cut -f 2 | tr '\n' '+' | sed -e 's/+$/\n/' | bc 

