#!/bin/bash

ROOT_DIR="$1"
LOGFILE="$2"
DEBUG="$3"

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

GEN_SYS_ARGS="-Djava.awt.headless=true ${LOG4J_CONFIG} -Djava.net.preferIPv4Stack=true"

ZOO_SYS_ARGS="-Dzookeeper.sasl.client=false -Xmx2g -Xms1g"

(java ${GEN_SYS_ARGS} ${ZOO_SYS_ARGS} org.apache.zookeeper.server.ZooKeeperServerMain 2181 "${ZOO_DIR}" 10 0  > "${LOGFILE}" 2>&1 ) &
# Give zoo some time
sleep 45

SPLICE_SYS_ARGS="-Xmx3g -Xms1g"

#HBase port properties
HBASE_MASTER_PORT=60000
HBASE_MASTER_INFO_PORT=60010
HBASE_REGIONSERVER_PORT=60020
HBASE_REGIONSERVER_INFO_PORT=60030
SPLICE_PORT=1527

(java ${GEN_SYS_ARGS} ${SPLICE_SYS_ARGS} -enableassertions com.splicemachine.single.SpliceSinglePlatform "${ZOO_DIR}" "${HBASE_ROOT_DIR_URI}" ${HBASE_MASTER_PORT} ${HBASE_MASTER_INFO_PORT} ${HBASE_REGIONSERVER_PORT} ${HBASE_REGIONSERVER_INFO_PORT} ${SPLICE_PORT} >> "${LOGFILE}" 2>&1 ) &
