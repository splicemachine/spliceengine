#!/bin/sh
# MapR specific environment properties

export SPLICE_ENV=mapr

# TODO - this should be mapr spcific
export ZOOKEEPER_HOME=/usr/lib/zookeeper

export HBASE_HOME="/usr/lib/hbase"
export HBASE_LIB="$HBASE_HOME/lib"
