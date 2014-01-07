#!/bin/bash

ROOT_DIR="${1}"
LOGFILE="${2}"
LOG4J_PATH="${3}"
ZOO_DIR="${4}"
HBASE_ROOT_DIR_URI="${5}"
CLASSPATH="${6}"
SPLICE_MAIN_CLASS="${7}"
CHAOS="${8}"

SPLICE_PID_FILE="${ROOT_DIR}"/splice_pid
export CLASSPATH
LOG4J_CONFIG="-Dlog4j.configuration=$LOG4J_PATH"

SYS_ARGS="-Xmx3g -Xms1g \
 -Djava.awt.headless=true \
 ${LOG4J_CONFIG} \
 -Djava.net.preferIPv4Stack=true \
 -Dcom.sun.management.jmxremote.ssl=false \
 -Dcom.sun.management.jmxremote.authenticate=false \
 -Dcom.sun.management.jmxremote.port=10102"
if [[ -n ${SPLICE_SYS_ARGS} ]]; then
    SYS_ARGS="${SYS_ARGS} ${SPLICE_SYS_ARGS}"
fi
    #HBase port properties
HBASE_MASTER_PORT=60000
HBASE_MASTER_INFO_PORT=60010
HBASE_REGIONSERVER_PORT=60020
HBASE_REGIONSERVER_INFO_PORT=60030
SPLICE_PORT=1527

(java ${SYS_ARGS} -enableassertions "${SPLICE_MAIN_CLASS}" "${ZOO_DIR}" "${HBASE_ROOT_DIR_URI}" ${HBASE_MASTER_PORT} ${HBASE_MASTER_INFO_PORT} ${HBASE_REGIONSERVER_PORT} ${HBASE_REGIONSERVER_INFO_PORT} ${SPLICE_PORT} ${CHAOS} >> "${LOGFILE}" 2>&1 ) &
echo "$!" > ${SPLICE_PID_FILE}
