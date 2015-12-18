#!/bin/sh
############################################################################
# Quickie script to determine if a given HFile is corrupted (based on how
# the java file HFileDump determines corruption).
#
# First argument: the name of the file that is being checked.
#
#
# Make sure and modify HADOOP_HOME and HBASE_HOME variables to ensure
# that they are pointing to the correct locations for your given environment,
# and that HFileDump.class was properly compiled for your distribution.
# 
############################################################################
FILE=$1

#point to the home directories for HADOOP and HBASE to ensure that the proper 
# classpath is set
HADOOP_HOME=/opt/mapr/hadoop/hadoop-0.20.2
HBASE_HOME=/opt/mapr/hbase/hbase-0.98.4

#export the native libs so that Snappy-compressed files can be read
export JAVA_LIBRARY_PATH=$JAVA_LIBRARY_PATH:$HADOOP_HOME/../hadoop-2.5.1/lib/native
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/../hadoop-2.5.1/lib/native

#Run the java script
java -cp "$HADOOP_HOME/lib/*:$HBASE_HOME/lib/*:$HADOOP_HOME/../hadoop-2.5.1/lib/*:." com/splicemachine/si/impl/compaction/HFileDump $FILE
