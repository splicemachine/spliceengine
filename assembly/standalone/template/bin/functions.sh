#!/bin/bash

UNAME=$(uname -s)


_kill_em_all () {
  SIG=$1
   local P=$(ps -ef | awk '/SpliceTestPlatform|SpliceSinglePlatform|SpliceTestClusterParticipant|OlapServerMaster/ && !/awk/ {print $2}')
   [[ -n $P ]] && echo "Found Splice. Stopping it." && for pid in $P; do kill -$SIG `echo $pid`; done

   P=$(ps -ef | awk '/spliceYarn|SpliceTestYarnPlatform|CoarseGrainedScheduler|ExecutorLauncher/ && !/awk/ {print $2}')
   [[ -n $P ]] && echo "Found YARN. Stopping it." && for pid in $P; do kill -$SIG `echo $pid`; done

   P=$(ps -ef | awk '/ZooKeeper/ && !/awk/ {print $2}')
   [[ -n $P ]] && echo "Found ZooKeeper. Stopping it." && for pid in $P; do kill -$SIG `echo $pid`; done

   P=$(ps -ef | awk '/TestKafkaCluster/ && !/awk/ {print $2}')
   [[ -n $P ]] && echo "Found Kafka. Stopping it." && for pid in $P; do kill -$SIG `echo $pid`; done

   P=$(ps -ef | awk '/exec:java/ && !/awk/ {print $2}')
   [[ -n $P ]] && echo "Found stray maven exec:java. Stopping it." && for pid in $P; do kill -$SIG `echo $pid`; done
}


_startSplice() {

    ROOT_DIR="${1}"
    LOGFILE="${2}"
    LOG4J_PATH="${3}"
    HBASE_ROOT_DIR_URI="${4}"
    CLASSPATH="${5}"

  export CLASSPATH

    SYS_ARGS="-Xdebug \
-Dlog4j.configuration=$LOG4J_PATH \
-Dspark.yarn.jars=$ROOT_DIR/lib,$ROOT_DIR/lib/* \
-Djava.net.preferIPv4Stack=true \
-Djava.awt.headless=true \
-Dzookeeper.sasl.client=false \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.port=10102 \
-Dsplice.authentication=NATIVE \
-Dcom.splicemachine.enableLegacyAsserts=true \
-Xmx6g \
-Xms1g \
-Dsplice.spark.app.name=SpliceMachine \
-Dsplice.spark.driver.host=localhost \
-Dsplice.spark.driver.cores=1 \
-Dsplice.spark.driver.extraClassPath=$ROOT_DIR/lib:$ROOT_DIR/lib/* \
-Dsplice.spark.driver.maxResultSize=1g \
-Dsplice.spark.driver.memory=1g \
-Dsplice.spark.enabled=true \
-Dsplice.spark.executor.cores=3 \
-Dsplice.spark.executor.extraClassPath=$ROOT_DIR/lib:$ROOT_DIR/lib/* \
-Dsplice.spark.executor.instances=1 \
-Dsplice.spark.executor.memory=2g \
-Dsplice.spark.io.compression.lz4.blockSize=32k \
-Dsplice.spark.kryo.referenceTracking=false \
-Dsplice.spark.kryo.registrator=com.splicemachine.derby.impl.SpliceSparkKryoRegistrator \
-Dsplice.spark.kryoserializer.buffer.max=512m \
-Dsplice.spark.kryoserializer.buffer=4m \
-Dsplice.spark.locality.wait=60s \
-Dsplice.spark.logConf=true \
-Dsplice.spark.master=yarn-client \
-Dsplice.spark.scheduler.allocation.file=$ROOT_DIR/conf/fairscheduler.xml \
-Dsplice.spark.scheduler.mode=FAIR \
-Dsplice.spark.serializer=org.apache.spark.serializer.KryoSerializer \
-Dsplice.spark.shuffle.compress=false \
-Dsplice.spark.shuffle.file.buffer=128k \
-Dsplice.spark.shuffle.memoryFraction=0.7 \
-Dsplice.spark.shuffle.service.enabled=true \
-Dsplice.spark.storage.memoryFraction=0.1 \
-Dsplice.spark.yarn.am.waitTime=10s \
-Xmn128m \
-XX:+UseConcMarkSweepGC \
-XX:+UseParNewGC \
-XX:NewSize=128m \
-XX:MaxNewSize=128m \
-XX:CMSInitiatingOccupancyFraction=70 \
-XX:+UseCMSInitiatingOccupancyOnly \
-XX:MaxGCPauseMillis=100 \
-XX:MaxPermSize=512M \
-XX:+CMSClassUnloadingEnabled \
-XX:MaxDirectMemorySize=1g \
-enableassertions \
-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4000 \
-Dderby.language.updateSystemProcs=false \
-Dderby.language.logStatementText=false \
-Dderby.infolog.append=true"

SYS_ARGS2="-Dsplice.spark.executor.extraJavaOptions=-Dhbase.rootdir=$HBASE_ROOT_DIR_URI/hbase
-XX:MaxPermSize=128m
-Dlog4j.configuration=$LOG4J_PATH
-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=4020
-enableassertions"

    if [[ -n ${SPLICE_SYS_ARGS} ]]; then
        SYS_ARGS="${SYS_ARGS} ${SPLICE_SYS_ARGS}"
    fi
        #HBase port properties
    HBASE_MASTER_PORT=60000
    HBASE_MASTER_INFO_PORT=60010
    HBASE_REGIONSERVER_PORT=60020
    HBASE_REGIONSERVER_INFO_PORT=60030
    SPLICE_PORT=1527

    (java ${SYS_ARGS} "${SYS_ARGS2}" com.splicemachine.test.SpliceTestPlatform file://${HBASE_ROOT_DIR_URI} ${HBASE_MASTER_PORT} ${HBASE_MASTER_INFO_PORT} ${HBASE_REGIONSERVER_PORT} ${HBASE_REGIONSERVER_INFO_PORT} ${SPLICE_PORT} false ${LOG4J_PATH} false >> "${LOGFILE}" 2>&1 ) &

}



_startZoo() {
    ROOT_DIR="${1}"
    LOGFILE="${2}"
    LOG4J_FILE="${3}"
    ZOO_DIR="${4}"
    CLASSPATH="${5}"

    export CLASSPATH
    LOG4J_CONFIG="-Dlog4j.configuration=$LOG4J_FILE"

    SYS_ARGS=" -Xmx1g -Xms1g -Dzookeeper.sasl.client=false -Djava.awt.headless=true ${LOG4J_CONFIG} -Djava.net.preferIPv4Stack=true"

    (java ${SYS_ARGS} org.apache.zookeeper.server.ZooKeeperServerMain 2181 "${ZOO_DIR}" 10 0  > "${LOGFILE}" 2>&1 ) &

}

_startYarn() {
    ROOT_DIR="${1}"
    CLASSPATH="${2}"
    LOGFILE="${3}"

    export CLASSPATH

    (java com.splicemachine.test.SpliceTestYarnPlatform "${ROOT_DIR}"/lib 1 >> "${LOGFILE}" 2>&1 ) &

}

_startKafka() {
    ROOT_DIR="${1}"
    CLASSPATH="${2}"
    LOGFILE="${3}"
    LOG4J_FILE="${4}"

    SYS_ARGS1=" -Djava.awt.headless=true -Dlog4j.configuration=${LOG4J_FILE} -Djava.net.preferIPv4Stack=true -Dzookeeper.sasl.client=false"
    SYS_ARGS2=" -Xmx1g -Xms1g -XX:MaxPermSize=256M -XX:+CMSClassUnloadingEnabled "

    export CLASSPATH

    (java ${SYS_ARGS1} ${SYS_ARGS2} com.splicemachine.kafka.TestKafkaCluster localhost:2181 >> "${LOGFILE}" 2>&1 ) &

}

function is_port_open
{
  host1=$1
  port2=$2
  # note there's two versions of nc: GNU and OpenBSD
  # OpenBSD supports -z (only check port), but GNU not.
  # GNU would work with 'exit', OpenBSD needs 'exit\n'
  # this one works in both and on mac
  echo 'exit\n' | nc ${host1} ${port2} > /dev/null 2> /dev/null
}
