#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Set environment variables here.

# This script sets variables multiple times over the course of starting an hbase process,
# so try to keep things idempotent unless you want to take an even deeper look
# into the startup scripts (bin/hbase, etc.)

# The java implementation to use.  Java 1.7+ required.
# export JAVA_HOME=/usr/java/jdk1.6.0/

# Extra Java CLASSPATH elements.  Optional.
# export HBASE_CLASSPATH=

SPLICELIBDIR="/opt/splice/default/lib"
HADOOPTOOLSDIR="/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/tools/lib"
PREPENDSTRING="$SPLICELIBDIR/*:$HADOOPTOOLSDIR"
export HBASE_CLASSPATH="${PREPENDSTRING}:${HBASE_CLASSPATH}"

# explicitly use our hbase conf dir
export HBASE_CONF_DIR="/opt/mapr/hbase/hbase1.1.8-splice/conf"

# The maximum amount of heap to use. Default is left to JVM default.
# export HBASE_HEAPSIZE=1G

# Uncomment below if you intend to use off heap cache. For example, to allocate 8G of 
# offheap, set the value to "8G".
# export HBASE_OFFHEAPSIZE=1G

# FOR Splice Machine
# build these out in a clear manner

### Java Configuration Options for HBase Master
SPLICE_HBASE_MASTER_OPTS=""
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -XX:+HeapDumpOnOutOfMemoryError"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -XX:MaxDirectMemorySize=2g"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -XX:+AlwaysPreTouch"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -XX:+UseParNewGC"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -XX:+UseConcMarkSweepGC"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -XX:CMSInitiatingOccupancyFraction=70"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -XX:+CMSParallelRemarkEnabled"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dcom.sun.management.jmxremote.ssl=false"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dcom.sun.management.jmxremote.port=10101"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.enabled=true"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.app.name=SpliceMachine"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.yarn.appMasterEnv.HADOOP_USER=mapr"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.yarn.appMasterEnv.HBASE_USER=mapr"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.yarn.user=mapr"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.master=yarn-client"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.logConf=true"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.driver.maxResultSize=1g"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.driver.memory=1g"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.dynamicAllocation.enabled=true"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.dynamicAllocation.executorIdleTimeout=600"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.dynamicAllocation.minExecutors=0"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.io.compression.lz4.blockSize=32k"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.kryo.referenceTracking=false"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.kryo.registrator=com.splicemachine.derby.impl.SpliceSparkKryoRegistrator"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.kryoserializer.buffer.max=512m"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.kryoserializer.buffer=4m"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.locality.wait=0"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.scheduler.mode=FAIR"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.serializer=org.apache.spark.serializer.KryoSerializer"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.shuffle.compress=false"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.shuffle.file.buffer=128k"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.shuffle.memoryFraction=0.7"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.shuffle.service.enabled=true"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.storage.memoryFraction=0.1"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.yarn.am.extraLibraryPath=/opt/mapr/hadoop/hadoop-2.7.0/lib/native"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.yarn.am.waitTime=10s"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.yarn.executor.memoryOverhead=2048"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/etc/spark/conf/log4j.properties"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.driver.extraLibraryPath=/opt/mapr/hadoop/hadoop-2.7.0/lib/native"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.driver.extraClassPath=/opt/mapr/hbase/hbase1.1.8-splice/conf:/opt/mapr/hbase/hbase1.1.8-splice/lib/*:/opt/splice/default/lib/*"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/etc/spark/conf/log4j.properties"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.executor.extraLibraryPath=/opt/mapr/hadoop/hadoop-2.7.0/lib/native"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.executor.extraClassPath=/opt/mapr/hbase/hbase1.1.8-splice/conf:/opt/mapr/hbase/hbase1.1.8-splice/lib/*:/opt/splice/default/lib/*"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ui.retainedJobs=100"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ui.retainedStages=100"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.worker.ui.retainedExecutors=100"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.worker.ui.retainedDrivers=100"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.streaming.ui.retainedBatches=100"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.executor.cores=4"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.executor.memory=8g"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dspark.compaction.reserved.slots=4"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.eventLog.enabled=true"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.eventLog.dir=maprfs:///user/splice/history" # this needs to be created before startup
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.dynamicAllocation.maxExecutors=11" # this is installation specific
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.local.dir=/tmp"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.authenticate=true"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.authenticate.enableSaslEncryption=true"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ssl.fs.enabled=true"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ssl.keyPassword=mapr123"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ssl.keyStore=/opt/mapr/conf/ssl_keystore"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ssl.keyStorePassword=mapr123"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ssl.trustStore=/opt/mapr/conf/ssl_truststore"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ssl.trustStorePassword=mapr123"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ssl.protocol=TLSv1.2"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ssl.enabledAlgorithms=TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA"
SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.yarn.jars=${SPLICELIBDIR}/*"
# SPLICE_HBASE_MASTER_OPTS="$SPLICE_HBASE_MASTER_OPTS -enableassertions"

### Java Configuration Options for HBase RegionServer
SPLICE_HBASE_REGIONSERVER_OPTS=""
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -XX:+HeapDumpOnOutOfMemoryError"
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -XX:MaxDirectMemorySize=2g"
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -XX:+AlwaysPreTouch"
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -XX:+UseG1GC"
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -XX:MaxNewSize=4g"
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -XX:InitiatingHeapOccupancyPercent=60"
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -XX:ParallelGCThreads=24"
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -XX:+ParallelRefProcEnabled"
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -XX:MaxGCPauseMillis=5000"
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -Dcom.sun.management.jmxremote.ssl=false"
SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -Dcom.sun.management.jmxremote.port=10102"
# SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -enableassertions"
# SPLICE_HBASE_REGIONSERVER_OPTS="$SPLICE_HBASE_REGIONSERVER_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4000"

# the $HBASE_OPTS are commented out in favor of $SPLICE_HBASE_MASTER_OPTS to avoid
# conflict between G1GC for the RegionServer and CMS + ParNew for the Master

# Extra Java runtime options.
# Below are what we set by default.  May only work with SUN JVM.
# For more on why as well as other possible settings,
# see http://wiki.apache.org/hadoop/PerformanceTuning
# export HBASE_OPTS="-XX:+UseConcMarkSweepGC"
# and http://www.scribd.com/doc/37127094/GCTuningPresentationFISL10
# export HBASE_OPTS="$HBASE_OPTS -XX:+UseParNewGC -XX:NewRatio=16 -XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSInitiatingOccupancyOnly -XX:MaxGCPauseMillis=100"

# the $HBASE_{MASTER,REGIONSERVER}_OPTS are commented out in favor of $SPLICE_HBASE_{MASTER,REGIONSERVER}_OPTS
# Configure PermSize. Only needed in JDK7. You can safely remove it for JDK8+
# export HBASE_MASTER_OPTS="$HBASE_MASTER_OPTS -XX:PermSize=128m -XX:MaxPermSize=128m"
# export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS -XX:PermSize=128m -XX:MaxPermSize=128m"

# Uncomment one of the below three options to enable java garbage collection logging for the server-side processes.

# This enables basic gc logging to the .out file.
# export SERVER_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps"

# This enables basic gc logging to its own file.
# If FILE-PATH is not replaced, the log file(.gc) would still be generated in the HBASE_LOG_DIR .
# export SERVER_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<FILE-PATH>"

# This enables basic GC logging to its own file with automatic log rolling. Only applies to jdk 1.6.0_34+ and 1.7.0_2+.
# If FILE-PATH is not replaced, the log file(.gc) would still be generated in the HBASE_LOG_DIR .
# export SERVER_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<FILE-PATH> -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=1 -XX:GCLogFileSize=512M"

# Uncomment one of the below three options to enable java garbage collection logging for the client processes.

# This enables basic gc logging to the .out file.
# export CLIENT_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps"

# This enables basic gc logging to its own file.
# If FILE-PATH is not replaced, the log file(.gc) would still be generated in the HBASE_LOG_DIR .
# export CLIENT_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<FILE-PATH>"

# This enables basic GC logging to its own file with automatic log rolling. Only applies to jdk 1.6.0_34+ and 1.7.0_2+.
# If FILE-PATH is not replaced, the log file(.gc) would still be generated in the HBASE_LOG_DIR .
# export CLIENT_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<FILE-PATH> -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=1 -XX:GCLogFileSize=512M"

# See the package documentation for org.apache.hadoop.hbase.io.hfile for other configurations
# needed setting up off-heap block caching. 

# Uncomment and adjust to enable JMX exporting
# See jmxremote.password and jmxremote.access in $JRE_HOME/lib/management to configure remote password access.
# More details at: http://java.sun.com/javase/6/docs/technotes/guides/management/agent.html
# NOTE: HBase provides an alternative JMX implementation to fix the random ports issue, please see JMX
# section in HBase Reference Guide for instructions.

export HBASE_JMX_BASE="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
export HBASE_MASTER_OPTS="$HBASE_MASTER_OPTS $HBASE_JMX_BASE $SPLICE_HBASE_MASTER_OPTS -Dsun.security.krb5.debug=true -Dcom.sun.management.jmxremote.port=10101"
export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS $HBASE_JMX_BASE $SPLICE_HBASE_REGIONSERVER_OPTS -Dcom.sun.management.jmxremote.port=10102"
# export HBASE_THRIFT_OPTS="$HBASE_THRIFT_OPTS $HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10103"
export HBASE_ZOOKEEPER_OPTS="$HBASE_ZOOKEEPER_OPTS $HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10104"
# export HBASE_REST_OPTS="$HBASE_REST_OPTS $HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10105"

# File naming hosts on which HRegionServers will run.  $HBASE_HOME/conf/regionservers by default.
# export HBASE_REGIONSERVERS=${HBASE_HOME}/conf/regionservers

# Uncomment and adjust to keep all the Region Server pages mapped to be memory resident
#HBASE_REGIONSERVER_MLOCK=true
#HBASE_REGIONSERVER_UID="hbase"

# File naming hosts on which backup HMaster will run.  $HBASE_HOME/conf/backup-masters by default.
# export HBASE_BACKUP_MASTERS=${HBASE_HOME}/conf/backup-masters

# Extra ssh options.  Empty by default.
# export HBASE_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=HBASE_CONF_DIR"

# Where log files are stored.  $HBASE_HOME/logs by default.
# export HBASE_LOG_DIR=${HBASE_HOME}/logs

# Enable remote JDWP debugging of major HBase processes. Meant for Core Developers 
# export HBASE_MASTER_OPTS="$HBASE_MASTER_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8070"
# export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8071"
# export HBASE_THRIFT_OPTS="$HBASE_THRIFT_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8072"
# export HBASE_ZOOKEEPER_OPTS="$HBASE_ZOOKEEPER_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8073"

# A string representing this instance of hbase. $USER by default.
# export HBASE_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export HBASE_NICENESS=10

# The directory where pid files are stored. /tmp by default.
export HBASE_PID_DIR=/opt/mapr/pid

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export HBASE_SLAVE_SLEEP=0.1

# Tell HBase whether it should manage it's own instance of Zookeeper or not.
# export HBASE_MANAGES_ZK=true

# The default log rolling policy is RFA, where the log file is rolled as per the size defined for the 
# RFA appender.
