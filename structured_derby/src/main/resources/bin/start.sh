#!/bin/bash

ROOT_DIR="$1"
LOGFILE="$2"
DEBUG="$3"

CLASSPATH=""

export MYCLASSPATH="${ROOT_DIR}"/lib/*

LOG4J_CONFIG="-Dlog4j.configuration=file:${ROOT_DIR}/lib/info-log4j.properties"

if [[ ! -z "$DEBUG" && "$DEBUG" -eq "-debug" ]]; then
    LOG4J_CONFIG="-Dlog4j.configuration=file:${ROOT_DIR}/lib/hbase-log4j.properties"
fi

GEN_SYS_ARGS="-Djava.awt.headless=true ${LOG4J_CONFIG} -Djava.net.preferIPv4Stack=true"

ZOO_SYS_ARGS="-Dzookeeper.sasl.client=false -Xmx2g -Xms1g"

ZOO_DIR="${ROOT_DIR}"/db/zookeeper
HBASE_DIR="${ROOT_DIR}"/db/hbase

(java ${GEN_SYS_ARGS} ${ZOO_SYS_ARGS} -classpath "$MYCLASSPATH" org.apache.zookeeper.server.ZooKeeperServerMain 2181 "${ZOO_DIR}" 10 0  > "${LOGFILE}" 2>&1 ) &

sleep 45

SPLICE_SYS_ARGS="-Xmx3g -Xms1g"

(java ${GEN_SYS_ARGS} ${SPLICE_SYS_ARGS} -enableassertions -classpath "$MYCLASSPATH" com.splicemachine.single.SpliceSinglePlatform "${ZOO_DIR}" "${HBASE_DIR}" 60021 60022 60023 60024 >> "${LOGFILE}" 2>&1 ) &
