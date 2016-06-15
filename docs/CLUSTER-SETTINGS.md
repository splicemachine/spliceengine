# Settings for Running Splice Machine on a Cluster 
## Zookeeper
SETTING | VALUE | DEFAULT (CDH 5.6.0 + CM 5.7)
------- | ----- | ----------------------------
maxClientCnxns | 0 | 60
maxSessionTimeout | 120000 | 60000

## HDFS
SETTING | VALUE | DEFAULT (CDH 5.6.0 + CM 5.7)
------- | ----- | ----------------------------
dfs.data.dir, dfs.datanode.data.dir | CHECK THIS | automatically determined
dfs.name.dir, dfs.namenode.name.dir | CHECK THIS | automatically determined
fs.checkpoint.dir, dfs.namenode.checkpoint.dir | CHECK THIS | automatically determined
Java Configuration Options for NameNode | -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:NewSize=256m -XX:MaxNewSize=256m | -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled
dfs.datanode.handler.count | 20 | 3
dfs.datanode.max.xcievers, dfs.datanode.max.transfer.threads | 8192 | 4096
Maximum Process File Descriptors | 32768 | unset in UI
dfs.namenode.handler.count | 64 | 30
dfs.namenode.service.handler.count | 60 | 30
dfs.replication | 2 or 3 (based on preference, splice recommends 2 for cluster less than 8 DN) | 3
Java Heap Size of NameNode in Bytes | 2GB | 4GB
Java Heap Size of Secondary NameNode in Bytes | 2GB | 4GB
Java Heap Size of DataNode in Bytes | 2GB | 1GB

## YARN
SETTING | VALUE | DEFAULT (CDH 5.6.0 + CM 5.7)
------- | ----- | ----------------------------
yarn.nodemanager.local-dirs | CHECK THIS | automatically determined
yarn.application.classpath | ADD $HADOOP_MAPRED_HOME/*, $HADOOP_MAPRED_HOME/lib/*, $MR2_CLASSPATH, /opt/cloudera/parcels/CDH/lib/hbase/*, /opt/cloudera/parcels/CDH/lib/hbase/lib/*, /opt/cloudera/parcels/SPLICEMACHINE/lib/* |
yarn.nodemanager.delete.debug-delay-sec | 86400 | 0
JobHistory Server Max Log Size | 1GB | 200MB
NodeManager Max Log Size | 1GB | 200MB
ResourceManager Max Log Size | 1GB | 200MB
Maximum Process File Descriptors | 32768 | unset in UI
yarn.nodemanager.resource.memory-mb | 24GB | defaults to 8GB, CM auto sets to 55134MB (on nodes with 128GB)
yarn.nodemanager.resource.cpu-vcores | 16 | defaults to 8, CM auto sets to 24 (on nodes with 24 vCPU, (Intel Xeon E5-2620 v3))
yarn.scheduler.maximum-allocation-mb | 24GB | defaults to 64GB, CM auto sets to 55134MB (on nodes with 128GB) 
yarn.scheduler.maximum-allocation-vcores | 16 | defaults to 32, CM auto sets to 24 (on nodes with 24 vCPU, (Intel Xeon E5-2620 v3))

## HBase
SETTING | VALUE | DEFAULT (CDH 5.6.0 + CM 5.7)
------- | ----- | ----------------------------
zookeeper.session.timeout | 120000 | 60000
hbase.client.pause | 90ms | 100ms
hbase.client.retries.number | 40 | 35
hbase.client.scanner.caching | 1000 | 100
Graceful Shutdown Timeout | 30 sec | 3 min
hbase.rpc.timeout | 20 min | 1 min
hbase.regionserver.lease.period, hbase.client.scanner.timeout.period | 20 min | 1 min
hbase.regionserver.handler.count | 400 | 30 
hbase.regionserver.maxlogs | 48 | 32 
hbase.wal.regiongrouping.numgroups | 16 | 1
hbase.hregion.memstore.block.multiplier | 4 | 2
hbase.hstore.compactionThreshold | 5 | 3 
hbase.hstore.blockingStoreFiles | 20 | 10
hfile.block.cache.size | 0.25 | 0.4
hbase.splitlog.manager.timeout | 5 min | 2 min
hbase.regionserver.thread.compaction.small | 4 | 1
hbase.master.port | 16000 | 60000
hbase.master.info.port | 16010 | 60010
hbase.regionserver.port | 16020 | 60020
hbase.regionserver.info.port | 16030 | 60030
hbase.coprocessor.master.classes | com.splicemachine.hbase.SpliceMasterObserver | 
hbase.coprocessor.region.classes | com.splicemachine.hbase.MemstoreAwareObserver com.splicemachine.derby.hbase.SpliceIndexObserver com.splicemachine.derby.hbase.SpliceIndexEndpoint com.splicemachine.hbase.RegionSizeEndpoint com.splicemachine.si.data.hbase.coprocessor.TxnLifecycleEndpoint com.splicemachine.si.data.hbase.coprocessor.SIObserver com.splicemachine.hbase.BackupEndpointObserver | 

### Other hbase-site.xml settings (HBase Service Advanced Configuration Snippet (Safety Valve) for hbase-site.xml)
SETTING | VALUE 
------- | ----- 
dfs.client.read.shortcircuit.buffer.size | 131072
hbase.balancer.period | 60000
hbase.client.ipc.pool.size | 10
hbase.client.max.perregion.tasks | 100
hbase.coprocessor.regionserver.classes | com.splicemachine.hbase.RegionServerLifecycleObserver
hbase.htable.threads.max | 96
hbase.ipc.warn.response.size | -1
hbase.ipc.warn.response.time | -1
hbase.master.loadbalance.bytable | true
hbase.regions.slop | 0.01
hbase.regionserver.global.memstore.size.lower.limit | 0.9
hbase.regionserver.global.memstore.size | 0.25
hbase.regionserver.maxlogs | 48
hbase.regionserver.wal.enablecompression | true
hbase.status.multicast.port | 16100
hbase.wal.disruptor.batch | true
hbase.wal.provider | multiwal
hbase.wal.regiongrouping.numgroups | 16
hbase.zookeeper.property.tickTime | 6000
hfile.block.bloom.cacheonwrite | true
io.storefile.bloom.error.rate | 0.005
splice.authentication.native.algorithm | SHA-512
splice.authentication | NATIVE
splice.client.numConnections | 1
splice.client.write.maxDependentWrites | 60000
splice.client.write.maxIndependentWrites | 60000
splice.compression | snappy
splice.marshal.kryoPoolSize | 1100
splice.ring.bufferSize | 131072
splice.splitBlockSize | 67108864
splice.task.priority.dmlRead.default | 0
splice.task.priority.dmlWrite.default | 0
splice.timestamp_server.clientWaitTime | 120000
splice.txn.activeTxns.cacheSize | 10240
splice.txn.completedTxns.concurrency | 128
splice.txn.concurrencyLevel | 4096
splice.txn.concurrencyLevel | 4096
hbase.hstore.defaultengine.compactor.class | com.splicemachine.compactions.SpliceDefaultCompactor
hbase.hstore.defaultengine.compactionpolicy.class | com.splicemachine.compactions.SpliceDefaultCompactionPolicy
hbase.mvcc.impl | org.apache.hadoop.hbase.regionserver.SIMultiVersionConsistencyControl
splice.olap_server.clientWaitTime | 900000
hbase.hstore.compaction.min | 5
hbase.hstore.compaction.min.size | 16777216
hbase.hstore.compaction.max.size | 260046848
hbase.regionserver.thread.compaction.large | 1

#### As xml blob:
```
<property><name>dfs.client.read.shortcircuit.buffer.size</name><value>131072</value></property>
<property><name>hbase.balancer.period</name><value>60000</value></property>
<property><name>hbase.client.ipc.pool.size</name><value>10</value></property>
<property><name>hbase.client.max.perregion.tasks</name><value>100</value></property>
<property><name>hbase.coprocessor.regionserver.classes</name><value>com.splicemachine.hbase.RegionServerLifecycleObserver</value></property>
<property><name>hbase.htable.threads.max</name><value>96</value></property>
<property><name>hbase.ipc.warn.response.size</name><value>-1</value></property>
<property><name>hbase.ipc.warn.response.time</name><value>-1</value></property>
<property><name>hbase.master.loadbalance.bytable</name><value>true</value></property>
<property><name>hbase.regions.slop</name><value>0.01</value></property>
<property><name>hbase.regionserver.global.memstore.size.lower.limit</name><value>0.9</value></property>
<property><name>hbase.regionserver.global.memstore.size</name><value>0.25</value></property>
<property><name>hbase.regionserver.maxlogs</name><value>48</value></property>
<property><name>hbase.regionserver.wal.enablecompression</name><value>true</value></property>
<property><name>hbase.status.multicast.port</name><value>16100</value></property>
<property><name>hbase.wal.disruptor.batch</name><value>true</value></property>
<property><name>hbase.wal.provider</name><value>multiwal</value></property>
<property><name>hbase.wal.regiongrouping.numgroups</name><value>16</value></property>
<property><name>hbase.zookeeper.property.tickTime</name><value>6000</value></property>
<property><name>hfile.block.bloom.cacheonwrite</name><value>true</value></property>
<property><name>io.storefile.bloom.error.rate</name><value>0.005</value></property>
<property><name>splice.authentication.native.algorithm</name><value>SHA-512</value></property>
<property><name>splice.authentication</name><value>NATIVE</value></property>
<property><name>splice.client.numConnections</name><value>1</value></property>
<property><name>splice.client.write.maxDependentWrites</name><value>40000</value></property>
<property><name>splice.client.write.maxIndependentWrites</name><value>40000</value></property>
<property><name>splice.compression</name><value>snappy</value></property>
<property><name>splice.marshal.kryoPoolSize</name><value>1100</value></property>
<property><name>splice.ring.bufferSize</name><value>131072</value></property>
<property><name>splice.splitBlockSize</name><value>67108864</value></property>
<property><name>splice.task.priority.dmlRead.default</name><value>0</value></property>
<property><name>splice.task.priority.dmlWrite.default</name><value>0</value></property>
<property><name>splice.timestamp_server.clientWaitTime</name><value>120000</value></property>
<property><name>splice.txn.activeTxns.cacheSize</name><value>10240</value></property>
<property><name>splice.txn.completedTxns.concurrency</name><value>128</value></property>
<property><name>splice.txn.concurrencyLevel</name><value>4096</value></property>
<property><name>splice.txn.concurrencyLevel</name><value>4096</value></property>
<property><name>hbase.hstore.defaultengine.compactor.class</name><value>com.splicemachine.compactions.SpliceDefaultCompactor</value></property>
<property><name>hbase.hstore.defaultengine.compactionpolicy.class</name><value>com.splicemachine.compactions.SpliceDefaultCompactionPolicy</value></property>
<property><name>hbase.mvcc.impl</name><value>org.apache.hadoop.hbase.regionserver.SIMultiVersionConsistencyControl</value></property>
<property><name>splice.olap_server.clientWaitTime</name><value>90000</value></property>
<property><name>hbase.hstore.compaction.min</name><value>5</value></property>
<property><name>hbase.hstore.compaction.min.size</name><value>16777216</value></property>
<property><name>hbase.hstore.compaction.max.size</name><value>260046848</value></property>
<property><name>hbase.regionserver.thread.compaction.large</name><value>4</value></property>
```


### HBase Environment settings (hbase-env.sh)
#### HMaster
SETTING | VALUE 
------- | ----- 
Java Heap Size of HBase Master in Bytes | 5GB
Java Configuration Options for HBase Master | 
-XX:+HeapDumpOnOutOfMemoryError | 
-XX:MaxDirectMemorySize | 2g
-XX:MaxPermSize | 512M
-XX:+UseParNewGC |
-XX:+UseConcMarkSweepGC |
-XX:CMSInitiatingOccupancyFraction | 70
-XX:+CMSParallelRemarkEnabled |
-XX:+AlwaysPreTouch |
-Dcom.sun.management.jmxremote.authenticate | false
-Dcom.sun.management.jmxremote.ssl | false
-Dcom.sun.management.jmxremote.port | 10101
-enableassertions |
-Dsplice.spark.enabled | true
-Dsplice.spark.app.name | SpliceMachineCompactor
-Dsplice.spark.driver.maxResultSize | 1g
-Dsplice.spark.driver.memory | 1g
-Dsplice.spark.executor.cores | 4
-Dsplice.spark.executor.memory | 3g
-Dsplice.spark.local.dir | /diska/tmp,/diskb/tmp,/diskc/tmp,/diskd/tmp
-Dsplice.spark.logConf | true
-Dsplice.spark.master | yarn-client
-Dsplice.spark.driver.extraClassPath | /opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar
-Dsplice.spark.driver.extraLibraryPath | /opt/cloudera/parcels/CDH/lib/hadoop/lib/native
-Dsplice.spark.executor.extraClassPath | /opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar
-Dsplice.spark.executor.extraLibraryPath | /opt/cloudera/parcels/CDH/lib/hadoop/lib/native
-Dsplice.spark.shuffle.compress | false
-Dsplice.spark.shuffle.file.buffer | 128k
-Dsplice.spark.shuffle.memoryFraction | 0.7
-Dsplice.spark.shuffle.service.enabled | true
-Dsplice.spark.io.compression.lz4.blockSize | 32k
-Dsplice.spark.kryo.referenceTracking | false
-Dsplice.spark.kryo.registrator | com.splicemachine.derby.impl.SpliceSparkKryoRegistrator
-Dsplice.spark.kryoserializer.buffer.max | 512m
-Dsplice.spark.kryoserializer.buffer | 4m
-Dsplice.spark.serializer | org.apache.spark.serializer.KryoSerializer
-Dsplice.spark.broadcast.factory | org.apache.spark.broadcast.HttpBroadcastFactory
-Dsplice.spark.storage.memoryFraction | 0.1
-Dsplice.spark.locality.wait | 100
-Dsplice.spark.scheduler.mode | FAIR
-Dsplice.spark.dynamicAllocation.enabled | true
-Dsplice.spark.dynamicAllocation.minExecutors | 0
-Dsplice.spark.dynamicAllocation.maxExecutors | 15
-Dsplice.spark.dynamicAllocation.executorIdleTimeout | 600
-Dsplice.spark.yarn.am.extraLibraryPath | /opt/cloudera/parcels/CDH/lib/hadoop/lib/native
-Dsplice.spark.yarn.am.waitTime | 10s
-Dsplice.spark.yarn.historyServer.address | SPARK_HISTORY_SERVER_HOSTNAME:18088
-Dsplice.spark.eventLog.dir | hdfs:///user/spark/applicationHistory
-Dsplice.spark.driver.extraJavaOptions | -Dlog4j.configuration=file:/etc/spark/conf/log4j.properties
-Dsplice.spark.executor.extraJavaOptions | -Dlog4j.configuration=file:/etc/spark/conf/log4j.properties
-Dsplice.spark.eventLog.enabled | true
-Dsplice.spark.ui.retainedJobs | 100
-Dsplice.spark.ui.retainedStages | 100
-Dsplice.spark.worker.ui.retainedExecutors | 100
-Dsplice.spark.worker.ui.retainedDrivers | 100
-Dsplice.spark.streaming.ui.retainedBatches | 100

##### as plain text blob 
```
-XX:+HeapDumpOnOutOfMemoryError -XX:MaxDirectMemorySize=2g -XX:MaxPermSize=512M -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:+AlwaysPreTouch -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=10101 -Dsplice.spark.enabled=true -Dsplice.spark.app.name=SpliceMachineCompactor -Dsplice.spark.driver.maxResultSize=1g -Dsplice.spark.driver.memory=1g -Dsplice.spark.executor.cores=4 -Dsplice.spark.executor.memory=6g -Dsplice.spark.local.dir=/diska/tmp,/diskb/tmp,/diskc/tmp,/diskd/tmp -Dsplice.spark.logConf=true -Dsplice.spark.master=yarn-client -Dsplice.spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar -Dsplice.spark.driver.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar -Dsplice.spark.executor.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.shuffle.compress=false -Dsplice.spark.shuffle.file.buffer=128k -Dsplice.spark.shuffle.memoryFraction=0.7 -Dsplice.spark.shuffle.service.enabled=true -Dsplice.spark.io.compression.lz4.blockSize=32k -Dsplice.spark.kryo.referenceTracking=false -Dsplice.spark.kryo.registrator=com.splicemachine.derby.impl.SpliceSparkKryoRegistrator -Dsplice.spark.kryoserializer.buffer.max=512m -Dsplice.spark.kryoserializer.buffer=4m -Dsplice.spark.serializer=org.apache.spark.serializer.KryoSerializer -Dsplice.spark.broadcast.factory=org.apache.spark.broadcast.HttpBroadcastFactory -Dsplice.spark.storage.memoryFraction=0.1 -Dsplice.spark.locality.wait=100 -Dsplice.spark.scheduler.mode=FAIR -Dsplice.spark.dynamicAllocation.enabled=true -Dsplice.spark.dynamicAllocation.minExecutors=0 -Dsplice.spark.dynamicAllocation.maxExecutors=7 -Dsplice.spark.dynamicAllocation.executorIdleTimeout=600 -Dsplice.spark.yarn.am.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.yarn.am.waitTime=10s -Dsplice.spark.yarn.historyServer.address=HISTORY_SERVER_HOSTNAME:18088 -Dsplice.spark.eventLog.dir=hdfs:///user/spark/applicationHistory -Dsplice.spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/etc/spark/conf/log4j.properties -Dsplice.spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/etc/spark/conf/log4j.properties -Dsplice.spark.eventLog.enabled=true -Dsplice.spark.ui.retainedJobs=100 -Dsplice.spark.ui.retainedStages=100 -Dsplice.spark.worker.ui.retainedExecutors=100 -Dsplice.spark.worker.ui.retainedDrivers=100 -Dsplice.spark.streaming.ui.retainedBatches=100 -enableassertions
```

#### HRegionServer
SETTING | VALUE 
------- | ----- 
Java Heap Size of HBase RegionServer in Bytes | 24GB
Java Configuration Options for HBase RegionServer |
-XX:+HeapDumpOnOutOfMemoryError |
-XX:MaxDirectMemorySize | 2g
-XX:+UseG1GC |
-XX:MaxPermSize | 512M
-XX:MaxNewSize | 4g
-XX:InitiatingHeapOccupancyPercent | 60
-XX:ParallelGCThreads | 24
-XX:+ParallelRefProcEnabled |
-XX:MaxGCPauseMillis | 5000
-XX:+AlwaysPreTouch |
-Dcom.sun.management.jmxremote.authenticate | false
-Dcom.sun.management.jmxremote.ssl | false
-Dcom.sun.management.jmxremote.port | 10102
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4000 |
-enableassertions |
-Dsplice.spark.enabled | true
-Dsplice.spark.app.name | SpliceMachine
-Dsplice.spark.driver.maxResultSize | 1g
-Dsplice.spark.driver.memory | 1g
-Dsplice.spark.executor.cores | 4
-Dsplice.spark.executor.memory | 9g
-Dsplice.spark.local.dir | /diska/tmp,/diskb/tmp,/diskc/tmp,/diskd/tmp
-Dsplice.spark.logConf | true
-Dsplice.spark.master | yarn-client
-Dsplice.spark.driver.extraClassPath | /opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar
-Dsplice.spark.driver.extraLibraryPath | /opt/cloudera/parcels/CDH/lib/hadoop/lib/native
-Dsplice.spark.executor.extraClassPath | /opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar
-Dsplice.spark.executor.extraLibraryPath | /opt/cloudera/parcels/CDH/lib/hadoop/lib/native
-Dsplice.spark.shuffle.compress | false
-Dsplice.spark.shuffle.file.buffer | 128k
-Dsplice.spark.shuffle.memoryFraction | 0.7
-Dsplice.spark.shuffle.service.enabled | true
-Dsplice.spark.io.compression.lz4.blockSize | 32k
-Dsplice.spark.kryo.referenceTracking | false
-Dsplice.spark.kryo.registrator | com.splicemachine.derby.impl.SpliceSparkKryoRegistrator
-Dsplice.spark.kryoserializer.buffer.max | 512m
-Dsplice.spark.kryoserializer.buffer | 4m
-Dsplice.spark.serializer | org.apache.spark.serializer.KryoSerializer
-Dsplice.spark.broadcast.factory | org.apache.spark.broadcast.HttpBroadcastFactory
-Dsplice.spark.storage.memoryFraction | 0.1
-Dsplice.spark.locality.wait | 100
-Dsplice.spark.scheduler.mode | FAIR
-Dsplice.spark.dynamicAllocation.enabled | true
-Dsplice.spark.dynamicAllocation.minExecutors | 0
-Dsplice.spark.dynamicAllocation.maxExecutors | 15
-Dsplice.spark.dynamicAllocation.executorIdleTimeout | 600
-Dsplice.spark.yarn.am.extraLibraryPath | /opt/cloudera/parcels/CDH/lib/hadoop/lib/native
-Dsplice.spark.yarn.am.waitTime | 10s
-Dsplice.spark.yarn.executor.memoryOverhead | 2048
-Dsplice.spark.yarn.historyServer.address | SPARK_HISTORY_SERVER_HOSTNAME:18088
-Dsplice.spark.eventLog.dir | hdfs:///user/spark/applicationHistory
-Dsplice.spark.driver.extraJavaOptions | -Dlog4j.configuration=file:/etc/spark/conf/log4j.properties
-Dsplice.spark.executor.extraJavaOptions | -Dlog4j.configuration=file:/etc/spark/conf/log4j.properties
-Dsplice.spark.eventLog.enabled | true
-Dsplice.spark.ui.retainedJobs | 100
-Dsplice.spark.ui.retainedStages | 100
-Dsplice.spark.worker.ui.retainedExecutors | 100
-Dsplice.spark.worker.ui.retainedDrivers | 100
-Dsplice.spark.streaming.ui.retainedBatches | 100

##### as plain text blob
```
-XX:+HeapDumpOnOutOfMemoryError -XX:MaxDirectMemorySize=2g -XX:+UseG1GC -XX:MaxPermSize=512M -XX:MaxNewSize=4g -XX:InitiatingHeapOccupancyPercent=60 -XX:ParallelGCThreads=24 -XX:+ParallelRefProcEnabled -XX:MaxGCPauseMillis=5000 -XX:+AlwaysPreTouch -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=10102 -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4000 -Dsplice.spark.enabled=true -Dsplice.spark.app.name=SpliceMachine -Dsplice.spark.driver.maxResultSize=1g -Dsplice.spark.driver.memory=1g -Dsplice.spark.executor.cores=4 -Dsplice.spark.executor.memory=6g -Dsplice.spark.local.dir=/diska/tmp,/diskb/tmp,/diskc/tmp,/diskd/tmp -Dsplice.spark.logConf=true -Dsplice.spark.master=yarn-client -Dsplice.spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar -Dsplice.spark.driver.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar -Dsplice.spark.executor.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.shuffle.compress=false -Dsplice.spark.shuffle.file.buffer=128k -Dsplice.spark.shuffle.memoryFraction=0.7 -Dsplice.spark.shuffle.service.enabled=true -Dsplice.spark.io.compression.lz4.blockSize=32k -Dsplice.spark.kryo.referenceTracking=false -Dsplice.spark.kryo.registrator=com.splicemachine.derby.impl.SpliceSparkKryoRegistrator -Dsplice.spark.kryoserializer.buffer.max=512m -Dsplice.spark.kryoserializer.buffer=4m -Dsplice.spark.serializer=org.apache.spark.serializer.KryoSerializer -Dsplice.spark.broadcast.factory=org.apache.spark.broadcast.HttpBroadcastFactory -Dsplice.spark.storage.memoryFraction=0.1 -Dsplice.spark.locality.wait=100 -Dsplice.spark.scheduler.mode=FAIR -Dsplice.spark.dynamicAllocation.enabled=true -Dsplice.spark.dynamicAllocation.minExecutors=0 -Dsplice.spark.dynamicAllocation.maxExecutors=7 -Dsplice.spark.dynamicAllocation.executorIdleTimeout=600 -Dsplice.spark.yarn.am.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.yarn.am.waitTime=10s -Dsplice.spark.yarn.executor.memoryOverhead=2048 -Dsplice.spark.yarn.historyServer.address=HISTORY_SERVER_HOSTNAME:18088 -Dsplice.spark.eventLog.dir=hdfs:///user/spark/applicationHistory -Dsplice.spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/etc/spark/conf/log4j.properties -Dsplice.spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/etc/spark/conf/log4j.properties -Dsplice.spark.eventLog.enabled=true -Dsplice.spark.ui.retainedJobs=100 -Dsplice.spark.ui.retainedStages=100 -Dsplice.spark.worker.ui.retainedExecutors=100 -Dsplice.spark.worker.ui.retainedDrivers=100 -Dsplice.spark.streaming.ui.retainedBatches=100 -enableassertions
```
