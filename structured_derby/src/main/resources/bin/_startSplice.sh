#!/bin/bash

ROOT_DIR="$1"
LOGFILE="$2"
DEBUG="$3"

SPLICE_PID_FILE="${ROOT_DIR}"/splice_pid

LOG4J_PATH="file:${ROOT_DIR}/lib/info-log4j.properties"
if [[ -n "$DEBUG" && "$DEBUG" -eq "-debug" ]]; then
    LOG4J_PATH="file:${ROOT_DIR}/lib/hbase-log4j.properties"
fi

CLASSPATH="${ROOT_DIR}/lib/*"
ZOO_DIR="${ROOT_DIR}"/db/zookeeper
HBASE_ROOT_DIR_URI="file://${ROOT_DIR}/db/hbase"

CYGWIN=`uname -s`
if [[ ${CYGWIN} == CYGWIN* ]]; then
    CLASSPATH=`cygpath --path --windows "${ROOT_DIR}/lib/*"`
    ZOO_DIR=`cygpath --path --windows "${ROOT_DIR}/db/zookeeper"`
    HBASE_ROOT_DIR_URI="CYGWIN"
    LOG4J_PATH="file:///`cygpath --path --windows ${ROOT_DIR}/lib/info-log4j.properties`"
    if [[ -n "$DEBUG" && "$DEBUG" -eq "-debug" ]]; then
        LOG4J_PATH="file:///`cygpath --path --windows ${ROOT_DIR}/lib/hbase-log4j.properties`"
    fi
fi
export CLASSPATH
LOG4J_CONFIG="-Dlog4j.configuration=$LOG4J_PATH"

SYS_ARGS="-Xmx3g -Xms1g -Djava.awt.headless=true ${LOG4J_CONFIG} -Djava.net.preferIPv4Stack=true"

#HBase port properties
HBASE_MASTER_PORT=60000
HBASE_MASTER_INFO_PORT=60010
HBASE_REGIONSERVER_PORT=60020
HBASE_REGIONSERVER_INFO_PORT=60030
SPLICE_PORT=1527

(java ${SYS_ARGS} -enableassertions com.splicemachine.single.SpliceSinglePlatform "${ZOO_DIR}" "${HBASE_ROOT_DIR_URI}" ${HBASE_MASTER_PORT} ${HBASE_MASTER_INFO_PORT} ${HBASE_REGIONSERVER_PORT} ${HBASE_REGIONSERVER_INFO_PORT} ${SPLICE_PORT} >> "${LOGFILE}" 2>&1 ) &
echo "$!" >> ${SPLICE_PID_FILE}
