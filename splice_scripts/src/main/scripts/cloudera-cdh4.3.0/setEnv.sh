#!/bin/sh
# Cloudera specific environment properties

export SPLICE_ENV=cloudera

export ZOOKEEPER_HOME=/usr/lib/zookeeper

export HBASE_HOME="/usr/lib/hbase"
export HBASE_CONF_DIR="/usr/lib/hbase/conf"
export HBASE_LIB="$HBASE_HOME/lib"
export HBASE_BIN="/usr/bin/hbase"
