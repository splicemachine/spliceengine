#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "$JAVA_HOME" ]; then
    echo "Please set JAVA_HOME to a valid JDK"
    exit 1;
fi

GEN_SYS_ARGS="-Djava.awt.headless=true -Dlog4j.configuration=file:${DIR}/../lib/info-log4j.properties \
-Djava.net.preferIPv4Stack=true"

ZOO_SYS_ARGS="-Dzookeeper.sasl.client=false -Xmx2g -Xms1g"

ZOO_DIR="${DIR}"/zookeeper
HBASE_DIR="${DIR}"/hbase

MYCLASSPATH="${DIR}/../splice_machine-0.5rc2-SNAPSHOT-cloudera-cdh4.3.0.jar:${DIR}/../lib/*"

("${JAVA_HOME}"/bin/java ${GEN_SYS_ARGS} ${ZOO_SYS_ARGS} -classpath ${MYCLASSPATH} org.apache.zookeeper.server.ZooKeeperServerMain 2181 "${ZOO_DIR}" 10 0  > splice.log &)

sleep 15

SPLICE_SYS_ARGS="-Xmx3g -Xms1g"

("${JAVA_HOME}"/bin/java ${GEN_SYS_ARGS} ${ZOO_SYS_ARGS} -enableassertions -classpath ${MYCLASSPATH} com.splicemachine.test.SpliceSinglePlatform "${ZOO_DIR}" "${HBASE_DIR}" 60021 60022 60023 60024 >> splice.log &)

