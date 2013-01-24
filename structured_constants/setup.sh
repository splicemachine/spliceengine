#!/bin/bash

#######################################################
#   SET UP SCRIPT VARS
#######################################################

UserPath=/Users/SeeSaw
WorkingPath=$UserPath/dev/splicemachine

StructuredHBasePath=$WorkingPath/structured_hbase
DatanucleusHBasePath=$WorkingPath/datanucleus-hbase

HadoopRootPath=$UserPath/hadoop
HadoopPath=$HadoopRootPath/hadoop-2.0.0-cdh4.0.1
HBasePath=$HadoopRootPath/hbase-0.92.1-cdh4.0.1
ZookeeperPath=$HadoopRootPath/zookeeper-3.4.3-cdh4.0.1
HivePath=$HadoopRootPath/hive-0.8.1-cdh4.0.1

#######################################################
#   STOP PROCESSES
#######################################################

$HBasePath/bin/stop-hbase.sh
$ZookeeperPath/bin/zkServer.sh stop
$HadoopPath/sbin/stop-yarn.sh

# what is this for?
rm -rf /var/zookeeper/version-2

#######################################################
#   BUILD CODE
#######################################################

cd $StructuredHBasePath
mvn install -DskipTests
cd -
# why aren't we building datanucleus?


#######################################################
#   COPY JAR FILES
#######################################################

scp $StructuredHBasePath/lib/gson-2.2.1.jar $HBasePath/lib/
scp $StructuredHBasePath/lib/guava-11.0.2.jar $HBasePath/lib/
# not sure we need this?
scp $StructuredHBasePath/lib/datanucleus-core-2.0.3.jar $HBasePath/lib/
scp $StructuredHBasePath/structured_hive/target/structured_hive-1.0.0-SNAPSHOT.jar $HBasePath/lib/
scp $StructuredHBasePath/structured_constants/target/structured_constants-1.0.0-SNAPSHOT.jar $HBasePath/lib/
scp $StructuredHBasePath/structured_trx/target/structured_trx-1.0.0-SNAPSHOT.jar $HBasePath/lib/
scp $StructuredHBasePath/structured_idx/target/structured_idx-1.0.0-SNAPSHOT.jar $HBasePath/lib/
scp $DatanucleusHBasePath/datanucleus-hbase-store/target/datanucleus-hbase-store-1.0.0-SNAPSHOT.jar $HBasePath/lib/
scp $DatanucleusHBasePath/datanucleus-hbase-constants/target/datanucleus-hbase-constants-1.0.0-SNAPSHOT.jar $HBasePath/lib/

scp $StructuredHBasePath/structured_hive/target/structured_hive-1.0.0-SNAPSHOT.jar $ZookeeperPath/lib/
scp $StructuredHBasePath/structured_constants/target/structured_constants-1.0.0-SNAPSHOT.jar $ZookeeperPath/lib/
scp $StructuredHBasePath/structured_trx/target/structured_trx-1.0.0-SNAPSHOT.jar $ZookeeperPath/lib/
scp $StructuredHBasePath/structured_idx/target/structured_idx-1.0.0-SNAPSHOT.jar $ZookeeperPath/lib/
scp $DatanucleusHBasePath/datanucleus-hbase-store/target/datanucleus-hbase-store-1.0.0-SNAPSHOT.jar $ZookeeperPath/lib/
scp $DatanucleusHBasePath/datanucleus-hbase-constants/target/datanucleus-hbase-constants-1.0.0-SNAPSHOT.jar $ZookeeperPath/lib/

scp $StructuredHBasePath/lib/gson-2.2.1.jar $HadoopPath/share/hadoop/mapreduce/
scp $StructuredHBasePath/lib/guava-11.0.2.jar $HadoopPath/share/hadoop/mapreduce/
# not sure we need this?
scp $StructuredHBasePath/lib/datanucleus-core-2.0.3.jar $HadoopPath/share/hadoop/mapreduce/
scp $StructuredHBasePath/structured_hive/target/structured_hive-1.0.0-SNAPSHOT.jar $HadoopPath/share/hadoop/mapreduce/
scp $StructuredHBasePath/structured_constants/target/structured_constants-1.0.0-SNAPSHOT.jar $HadoopPath/share/hadoop/mapreduce/
scp $StructuredHBasePath/structured_trx/target/structured_trx-1.0.0-SNAPSHOT.jar $HadoopPath/share/hadoop/mapreduce/
scp $StructuredHBasePath/structured_idx/target/structured_idx-1.0.0-SNAPSHOT.jar $HadoopPath/share/hadoop/mapreduce/
scp $DatanucleusHBasePath/datanucleus-hbase-store/target/datanucleus-hbase-store-1.0.0-SNAPSHOT.jar $HadoopPath/share/hadoop/mapreduce/
scp $DatanucleusHBasePath/datanucleus-hbase-constants/target/datanucleus-hbase-constants-1.0.0-SNAPSHOT.jar $HadoopPath/share/hadoop/mapreduce/

scp $StructuredHBasePath/lib/gson-2.2.1.jar $HivePath/lib/
scp $StructuredHBasePath/lib/guava-11.0.2.jar $HivePath/lib/
# not sure we need this?
scp $StructuredHBasePath/lib/datanucleus-core-2.0.3.jar $HivePath/lib/
scp $StructuredHBasePath/structured_hive/target/structured_hive-1.0.0-SNAPSHOT.jar $HivePath/lib/
scp $StructuredHBasePath/structured_constants/target/structured_constants-1.0.0-SNAPSHOT.jar $HivePath/lib/
scp $StructuredHBasePath/structured_trx/target/structured_trx-1.0.0-SNAPSHOT.jar $HivePath/lib/
scp $StructuredHBasePath/structured_idx/target/structured_idx-1.0.0-SNAPSHOT.jar $HivePath/lib/
scp $DatanucleusHBasePath/datanucleus-hbase-store/target/datanucleus-hbase-store-1.0.0-SNAPSHOT.jar $HivePath/lib/
scp $DatanucleusHBasePath/datanucleus-hbase-constants/target/datanucleus-hbase-constants-1.0.0-SNAPSHOT.jar $HivePath/lib/

#######################################################
#   START PROCESSES
#######################################################

$HadoopPath/bin/hadoop fs -rm -r /hbase
$HadoopPath/bin/hadoop fs -mkdir /hbase

rm $HBasePath/logs/*

$HadoopPath/sbin/start-yarn.sh
$ZookeeperPath/bin/zkServer.sh start
$HBasePath/bin/start-hbase.sh
echo "create '__TXN_LOG', 'attributes'" | $HBasePath/bin/hbase shell

