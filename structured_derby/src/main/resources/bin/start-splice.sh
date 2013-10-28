#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

CLASSPATH=""

export MYCLASSPATH="${DIR}"/lib/*

GEN_SYS_ARGS="-Djava.awt.headless=true -Dlog4j.configuration=file:${DIR}/lib/info-log4j.properties \
-Djava.net.preferIPv4Stack=true"

ZOO_SYS_ARGS="-Dzookeeper.sasl.client=false -Xmx2g -Xms1g"

ZOO_DIR="${DIR}"/db/zookeeper
HBASE_DIR="${DIR}"/db/hbase

echo "Starting Splice Machine..."
(java ${GEN_SYS_ARGS} ${ZOO_SYS_ARGS} -classpath "${MYCLASSPATH}" org.apache.zookeeper.server.ZooKeeperServerMain 2181 "${ZOO_DIR}" 10 0  > "${DIR}"/splice.log 2>&1 &)

sleep 15

SPLICE_SYS_ARGS="-Xmx3g -Xms1g"

(java ${GEN_SYS_ARGS} ${SPLICE_SYS_ARGS} -enableassertions -classpath "${MYCLASSPATH}" com.splicemachine.single.SpliceSinglePlatform "${ZOO_DIR}" "${HBASE_DIR}" 60021 60022 60023 60024 >> "${DIR}"/splice.log 2>&1 &)

# TODO: splice is not yet ready for connection