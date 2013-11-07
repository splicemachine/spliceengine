#!/bin/bash

ROOT_DIR="$1"
LOGFILE="$2"
DEBUG="$3"

ZOO_PID_FILE="${ROOT_DIR}"/zoo_pid

LOG4J_PATH="file:${ROOT_DIR}/lib/info-log4j.properties"
if [[ -n "$DEBUG" && "$DEBUG" -eq "-debug" ]]; then
    LOG4J_PATH="file:${ROOT_DIR}/lib/hbase-log4j.properties"
fi

CLASSPATH="${ROOT_DIR}/lib/*"
ZOO_DIR="${ROOT_DIR}"/db/zookeeper

CYGWIN=`uname -s`
if [[ ${CYGWIN} == CYGWIN* ]]; then
    CLASSPATH=`cygpath --path --windows "${ROOT_DIR}/lib/*"`
    ZOO_DIR=`cygpath --path --windows "${ROOT_DIR}/db/zookeeper"`
    LOG4J_PATH="file:///`cygpath --path --windows ${ROOT_DIR}/lib/info-log4j.properties`"
    if [[ -n "$DEBUG" && "$DEBUG" -eq "-debug" ]]; then
        LOG4J_PATH="file:///`cygpath --path --windows ${ROOT_DIR}/lib/hbase-log4j.properties`"
    fi
fi
export CLASSPATH
LOG4J_CONFIG="-Dlog4j.configuration=$LOG4J_PATH"

SYS_ARGS=" -Xmx2g -Xms1g -Dzookeeper.sasl.client=false -Djava.awt.headless=true ${LOG4J_CONFIG} -Djava.net.preferIPv4Stack=true"

(java ${SYS_ARGS} org.apache.zookeeper.server.ZooKeeperServerMain 2181 "${ZOO_DIR}" 10 0  > "${LOGFILE}" 2>&1 ) &
echo "$!" > ${ZOO_PID_FILE}
# Give zoo some time
sleep 60
