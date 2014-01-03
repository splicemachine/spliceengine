#!/bin/bash

ROOT_DIR="$1"
LOGFILE="$2"
LOG4J_PATH="$3"
ZOO_DIR="$4"
CLASSPATH="$5"

ZOO_PID_FILE="${ROOT_DIR}"/zoo_pid
export CLASSPATH
LOG4J_CONFIG="-Dlog4j.configuration=$LOG4J_PATH"

SYS_ARGS=" -Xmx2g -Xms1g -Dzookeeper.sasl.client=false -Djava.awt.headless=true ${LOG4J_CONFIG} -Djava.net.preferIPv4Stack=true"

(java ${SYS_ARGS} org.apache.zookeeper.server.ZooKeeperServerMain 2181 "${ZOO_DIR}" 10 0  > "${LOGFILE}" 2>&1 ) &
echo "$!" > ${ZOO_PID_FILE}
