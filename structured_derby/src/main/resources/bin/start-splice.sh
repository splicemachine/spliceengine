#!/bin/bash

# Start with debug logging by passing this script the "-debug" argument

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

CLASSPATH=""

export MYCLASSPATH="${DIR}"/lib/*

LOG4J_CONFIG="-Dlog4j.configuration=file:${DIR}/lib/info-log4j.properties"

if [[ ! -z "$1" && "$1" -eq "-debug" ]]; then
    LOG4J_CONFIG="-Dlog4j.configuration=file:${DIR}/lib/hbase-log4j.properties"
fi

GEN_SYS_ARGS="-Djava.awt.headless=true ${LOG4J_CONFIG} \
-Djava.net.preferIPv4Stack=true"

ZOO_SYS_ARGS="-Dzookeeper.sasl.client=false -Xmx2g -Xms1g"

ZOO_DIR="${DIR}"/db/zookeeper
HBASE_DIR="${DIR}"/db/hbase

echo "Starting Splice Machine..."
echo "Log file is ${DIR}/splice.log"
(java ${GEN_SYS_ARGS} ${ZOO_SYS_ARGS} -classpath "${MYCLASSPATH}" org.apache.zookeeper.server.ZooKeeperServerMain 2181 "${ZOO_DIR}" 10 0  > "${DIR}"/splice.log 2>&1 &)

sleep 15

SPLICE_SYS_ARGS="-Xmx3g -Xms1g"

(java ${GEN_SYS_ARGS} ${SPLICE_SYS_ARGS} -enableassertions -classpath "${MYCLASSPATH}" com.splicemachine.single.SpliceSinglePlatform "${ZOO_DIR}" "${HBASE_DIR}" 60021 60022 60023 60024 >> "${DIR}"/splice.log 2>&1 &)

###
timeout=100
interval=5
# kill -0 pid   Exit code indicates if a signal may be sent to $pid process.
(
  ((t = timeout))

  while ((t > 0)); do
    sleep $interval
    kill -0 $$ || exit 0
    ((t -= interval))
  done

  # Be nice, post SIGTERM first.
  # The 'exit 0' below will be executed if any preceeding command fails.
  kill -s SIGTERM $$ && kill -0 $$ || exit 0
  sleep $delay
  kill -s SIGKILL $$
) 2> /dev/null &

exec ./bin/isReady.sh "${DIR}"/splice.log

if [[ $? -eq -1 ]]; then
    echo "Try again"
fi

