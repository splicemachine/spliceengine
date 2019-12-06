# Installing and Configuring Splice Machine for Cloudera Manager

This topic describes installing and configuring Splice Machine on a
Cloudera-managed cluster. Follow these steps:

1.  [Verify Prerequisites](#verify-prerequisites)
2.  [Install the Splice Machine Parcel](#install-the-splice-machine-parcel)
3.  [Stop Hadoop Services](#stop-hadoop-services)
4.  [Make Cluster Modifications for Splice Machine](#make-cluster-modifications-for-splice-machine)
5.  [Configure Hadoop Services](#configure-hadoop-services)
6.  Make any needed [Optional Configuration Modifications](#optional-cluster-modifications)
7.  [Deploy the Client Configuration](#deploy-the-client-configuration)
8.  [Restart the Cluster](#restart-the-cluster)
9.  [Verify your Splice Machine Installation](#verify-your-splice-machine-installation)

## Verify Prerequisites

Before starting your Splice Machine installation, please make sure that
your cluster contains the prerequisite software components:

* A cluster running Cloudera Data Hub (CDH) with Cloudera Manager (CM)
* HBase installed
* HDFS installed
* YARN installed
* ZooKeeper installed
* Spark2 installed (2.2 Release 2 recommended)

**NOTE:** The specific versions of these components that you need depend on your
operating environment, and are called out in detail in the
[Requirements](https://doc.splicemachine.com/onprem_info_requirements.html) topic of our *Getting
Started Guide*.

## Install the Splice Machine Parcel

Follow these steps to install CDH, Hadoop, Hadoop services, and Splice
Machine on your cluster:

1. Copy your parcel URL to the clipboard for use in the next step.

   Which Splice Machine parcel URL you need depends upon which Splice
   Machine version you're installing and which version of CDH you are
   using. Here are the URLs for Splice Machine Release 2.7 and
   2.5:

   <table>
   <col />
   <col />
   <col />
   <col />
   <thead>
       <tr>
           <th>CDH Version</th>
           <th>Parcel Type</th>
           <th>Installer Package Link(s)</th>
       </tr>
   </thead>
   <tbody>
       <tr>
           <td rowspan="6" style="vertical-align:top"><bold>5.8.3</bold></td>
           <td>EL6</td>
           <td>[https://s3.amazonaws.com/splice-releases/2.5.0.1802/cluster/parcel/cdh5.8.3/SPLICEMACHINE-2.5.0.1802.cdh5.8.3.p0.540-el6.parcel]</td>
       </tr>
       <tr>
           <td>EL7</td>
           <td>[https://s3.amazonaws.com/splice-releases/2.5.0.1802/cluster/parcel/cdh5.8.3/SPLICEMACHINE-2.5.0.1802.cdh5.8.3.p0.540-el7.parcel]</td>
       </tr>
       <tr>
           <td>Precise</td>
           <td>[https://s3.amazonaws.com/splice-releases/2.5.0.1802/cluster/parcel/cdh5.8.3/SPLICEMACHINE-2.5.0.1802.cdh5.8.3.p0.540-el6.precise]</td>
       </tr>
       <tr>
           <td>SLES11</td>
           <td>[https://s3.amazonaws.com/splice-releases/2.5.0.1802/cluster/parcel/cdh5.8.3/SPLICEMACHINE-2.5.0.1802.cdh5.8.3.p0.540-sles11.parcel]</td>
       </tr>
       <tr>
           <td>Trusty</td>
           <td>[https://s3.amazonaws.com/splice-releases/2.5.0.1802/cluster/parcel/cdh5.8.3/SPLICEMACHINE-2.5.0.1802.cdh5.8.3.p0.540-trusty.parcel]</td>
       </tr>
       <tr>
           <td>Wheezy</td>
           <td>[https://s3.amazonaws.com/splice-releases/2.5.0.1802/cluster/parcel/cdh5.8.3/SPLICEMACHINE-2.5.0.1802.cdh5.8.3.p0.540-wheezy.parcel]</a></td>
        </tr>
    </tbody>
   </table>

   **NOTE:** To be sure that you have the latest URL, please check [the Splice
   Machine Community site](https://community.splicemachine.com/) or contact your Splice
   Machine representative.

2. Add the parcel repository

   a. Make sure the `Use Parcels
   (Recommended)` option and the `Matched release` option are both
   selected.

   b. Click the `Continue` button to
   land on the *More Options* screen.

   c. Cick the `+` button for the `Remote Parcel Repository URLs` field.
   Paste your Splice Machine repository URL into this field.

3. Use Cloudera Manager to install the parcel.

4. Verify that the parcel has been distributed and activated.

   The Splice Machine parcel is identified as `SPLICEMACHINE` in the
   Cloudera Manager user interface. Make sure that this parcel has been
   downloaded, distributed, and activated on your cluster.

5. Restart and redeploy any client changes when Cloudera Manager
prompts you.

## Stop Hadoop Services

As a first step, we stop cluster services to allow our installer to make
changes that require the cluster to be temporarily inactive.

From the Cloudera Manager home screen, click the drop-down arrow next to
the cluster on

1. Select your cluster in Cloudera Manager

   Click the drop-down arrow next to the name of the cluster on which
   you are installing Splice Machine.

2. Stop the cluster

   Click the `Stop` button.

## Configure Hadoop Services

Now it's time to make a few modifications in the Hadoop services
configurations:

* [Configure and Restart the Management Service](#Configur)
* [Configure ZooKeeper](#Configur4)
* [Configure HDFS](#Configur5)
* [Configure YARN](#Configur2)
* [Configure HBASE](#Configur3)

### Configure and Restart the Management Service

1. Select the `Configuration` tab in CM:

   <img src="https://doc.splicemachine.com/images/CM.AlertListenPort.png" />

2. Change the value of the Alerts: Listen Port to `10110`.

3. Save changes and restart the Management Service.

### Configure ZooKeeper

To edit the ZooKeeper configuration, click `ZooKeeper` in the Cloudera Manager (CM) home
screen, then click the `Configuration` tab
and follow these steps:

1. Select the `Service-Wide` category.

   Make the following changes:
   
   ````
   Maximum Client Connections = 0
   Maximum Session Timeout    = 120000
   ````
   
   Click the `Save Changes` button.

### Configure HDFS

To edit the HDFS configuration, click `HDFS` in the Cloudera Manager home screen, then
click the `Configuration` tab and make
these changes:

1. Verify that the HDFS data directories for your cluster are set up to
use your data disks.

2. Change the values of these settings:

   <table>
    <col />
    <col />
    <thead>
        <tr>
            <th>Setting</th>
            <th>New Value</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><code>Handler Count</code></td>
            <td><code>20</code></td>
        </tr>
        <tr>
            <td><code>Maximum Number of Transfer Threads</code></td>
            <td><code>8192</code></td>
        </tr>
        <tr>
            <td><code>NameNode Handler Count</code></td>
            <td><code>64</code></td>
        </tr>
        <tr>
            <td><code>NameNode Service Handler Count</code></td>
            <td><code>60</code></td>
        </tr>
        <tr>
            <td><code>Replication Factor</code></td>
            <td><code>2 or 3 *</code></td>
        </tr>
        <tr>
            <td><code>Java Heap Size of DataNode in Bytes</code></td>
            <td><code>2 GB</code></td>
        </tr>
    </tbody>
   </table>

3. Click the `Save Changes` button.

### Configure YARN

To edit the YARN configuration, click `YARN` in the Cloudera Manager home screen, then
click the `Configuration` tab and make
these changes:

1. Verify that the following directories are set up to use your data disks:

   * `NodeManager Local Directories`
   * `NameNode Data Directories`
   * `HDFS Checkpoint Directories`

2. Change the values of these settings

   <table>
     <col width="50%" />
     <col width="50%" />
     <thead>
         <tr>
             <th style="text-align:left">Setting</th>
             <th style="text-align:left">New Value</th>
         </tr>
     </thead>
     <tbody>
         <tr>
             <td><code>Heartbeat Interval</code></td>
             <td><code>100 ms</code></td>
         </tr>
         <tr>
             <td><code>MR Application Classpath</code></td>
             <td><code>$HADOOP_MAPRED_HOME/*
$HADOOP_MAPRED_HOME/lib/*
$MR2_CLASSPATH</code>
             </td>
         </tr>
         <tr>
             <td><code>YARN Application Classpath</code></td>
             <td><code>$HADOOP_CLIENT_CONF_DIR
$HADOOP_CONF_DIR
$HADOOP_COMMON_HOME/*
$HADOOP_COMMON_HOME/lib/*
$HADOOP_HDFS_HOME/*
$HADOOP_HDFS_HOME/lib/*
$HADOOP_YARN_HOME/*
$HADOOP_YARN_HOME/lib/*
$HADOOP_MAPRED_HOME/*
$HADOOP_MAPRED_HOME/lib/*
$MR2_CLASSPATH</code>
             </td>
         </tr>
         <tr>
             <td><code>Localized Dir Deletion Delay</code></td>
             <td><code>86400</code></td>
         </tr>
         <tr>
             <td><code>JobHistory Server Max Log Size</code></td>
             <td><code>1 GB</code></td>
         </tr>
         <tr>
             <td><code>NodeManager Max Log Size</code></td>
             <td><code>1 GB</code></td>
         </tr>
         <tr>
             <td><code>ResourceManager Max Log Size</code></td>
             <td><code>1 GB</code></td>
         </tr>
         <tr>
             <td><code>Container Memory</code></td>
             <td><code>30 GB (based on node specs)</code></td>
         </tr>
         <tr>
             <td><code>Container Memory Maximum</code></td>
             <td><code>30 GB (based on node specs)</code></td>
         </tr>
         <tr>
             <td><code>Container Virtual CPU Cores</code></td>
             <td><code>19</code> (based on node specs)</td>
         </tr>
         <tr>
             <td><code>Container Virtual CPU Cores Maximum</code></td>
             <td><code>19</code> (Based on node specs)</td>
         </tr>
     </tbody>
   </table>

3. Add property values

   You need to add the same two property values to each of four YARN advanced configuration settings.

   Add these properties:

   <table>
     <col width="50%" />
     <col width="50%" />
     <thead>
         <tr>
             <th>XML Property Name</th>
             <th>XML Property Value</th>
         </tr>
     </thead>
     <tbody>
         <tr>
             <td><code>yarn.nodemanager.aux-services.spark_shuffle.class</code></td>
             <td><code>org.apache.spark.network.yarn.YarnShuffleService</code></td>
         </tr>
         <tr>
             <td><code>yarn.nodemanager.aux-services</code></td>
             <td><code>mapreduce_shuffle,spark_shuffle</code></td>
         </tr>
     </tbody>
   </table>

   To each of these YARN settings:

   * `Yarn Service Advanced Configuration Snippet (Safety Valve) for yarn-site.xml`
   * `Yarn Client Advanced Configuration Snippet (Safety Valve) for yarn-site.xml`
   * `NodeManager Advanced Configuration Snippet (Safety Valve) for yarn-site.xml`
   * `ResourceManager Advanced Configuration Snippet (Safety Valve) for yarn-site.xml`

4. Click the `Save Changes` button.

### Configure HBASE

To edit the HBASE configuration, click `HBASE` in the Cloudera Manager home screen,
then click the `Configuration` tab and
make these changes:

1. Change the values of these settings

   <table>
    <col width="50%" />
    <col width="50%" />
    <thead>
        <tr>
            <th>Setting</th>
            <th>New Value</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><code>HBase Client Scanner Caching</code></td>
            <td><code>100 ms</code></td>
        </tr>
        <tr>
            <td><code>Graceful Shutdown Timeout</code></td>
            <td><code>600 seconds</code></td>
        </tr>
        <tr>
            <td><code>HBase Service Advanced Configuration Snippet (Safety Valve) for hbase-site.xml</code></td>
            <td>The property list for the Safety Valve snippet is shown below, in Step 2</td>
        </tr>
        <tr>
            <td><code>SplitLog Manager Timeout</code></td>
            <td><code>5 minutes</code></td>
        </tr>
        <tr>
            <td><code>Maximum HBase Client Retries</code></td>
            <td><code>40</code></td>
        </tr>
        <tr>
            <td><code>RPC Timeout</code></td>
            <td><code>20 minutes (or 1200000 milliseconds)</code></td>
        </tr>
        <tr>
            <td><code>HBase Client Pause</code></td>
            <td><code>90</code></td>
        </tr>
        <tr>
            <td><code>ZooKeeper Session Timeout</code></td>
            <td><code>120000</code></td>
        </tr>
        <tr>
            <td><code>HBase Master Web UI Port</code></td>
            <td><code>16010</code></td>
        </tr>
        <tr>
            <td><code>HBase Master Port</code></td>
            <td><code>16000</code></td>
        </tr>
        <tr>
            <td><code>Java Configuration Options for HBase Master</code></td>
            <td>The  HBase Master Java configuration options list is shown below, in Step 3
            </td>
        </tr>
        <tr>
            <td><code>HBase Coprocessor Master Classes</code></td>
            <td>
                <p>com.splicemachine.hbase.SpliceMasterObserver</p>
            </td>
        </tr>
        <tr>
            <td><code>Java Heap Size of HBase Master in Bytes</code></td>
            <td><code>5 GB</code></td>
        </tr>
        <tr>
            <td><code>HStore Compaction Threshold</code></td>
            <td><code>5</code></td>
        </tr>
        <tr>
            <td><code>HBase RegionServer Web UI port</code></td>
            <td><code>16030</code></td>
        </tr>
        <tr>
            <td><code>HStore Blocking Store Files</code></td>
            <td><code>20</code></td>
        </tr>
        <tr>
            <td><code>Java Configuration Options for HBase RegionServer</code></td>
            <td>The HBase RegionServerJava configuration options list is shown below, in Step 4
            </td>
        </tr>
        <tr>
            <td><code>HBase Memstore Block Multiplier</code></td>
            <td><code>4</code></td>
        </tr>
        <tr>
            <td><code>Maximum Number of HStoreFiles Compaction</code></td>
            <td><code>7</code></td>
        </tr>
        <tr>
            <td><code>HBase RegionServer Lease Period</code></td>
            <td><code>20 minutes (or 1200000 milliseconds)</code></td>
        </tr>
        <tr>
            <td><code>HFile Block Cache Size</code></td>
            <td><code>0.25</code></td>
        </tr>
        <tr>
            <td><code>Java Heap Size of HBase RegionServer in Bytes</code></td>
            <td><code>24 GB</code></td>
        </tr>
        <tr>
            <td><code>HBase RegionServer Handler Count</code></td>
            <td><code>200</code></td>
        </tr>
        <tr>
            <td><code>HBase RegionServer Meta-Handler Count</code></td>
            <td><code>200</code></td>
        </tr>
        <tr>
            <td><code>HBase Coprocessor Region Classes</code></td>
            <td><code>com.splicemachine.hbase.MemstoreAwareObserver
com.splicemachine.derby.hbase.SpliceIndexObserver
com.splicemachine.derby.hbase.SpliceIndexEndpoint
com.splicemachine.hbase.RegionSizeEndpoint
com.splicemachine.si.data.hbase.coprocessor.TxnLifecycleEndpoint
com.splicemachine.si.data.hbase.coprocessor.SIObserver
com.splicemachine.hbase.BackupEndpointObserver</code>
           </td>
        </tr>
        <tr>
            <td><code>Maximum number of Write-Ahead Log (WAL) files</code></td>
            <td><code>48</code></td>
        </tr>
        <tr>
            <td><code>RegionServer Small Compactions Thread Count</code></td>
            <td><code>4</code></td>
        </tr>
        <tr>
            <td><code>HBase RegionServer Port</code></td>
            <td><code>16020</code></td>
        </tr>
        <tr>
            <td><code>Per-RegionServer Number of WAL Pipelines</code></td>
            <td><code>16</code></td>
        </tr>
    </tbody>
   </table>

2. Set the value of `HBase Service Advanced Configuration Snippet (Safety Valve) for hbase-site.xml`:

   ````
   <property><name>dfs.client.read.shortcircuit.buffer.size</name><value>131072</value></property>
   <property><name>hbase.balancer.period</name><value>60000</value></property>
   <property><name>hbase.client.ipc.pool.size</name><value>10</value></property>
   <property><name>hbase.client.max.perregion.tasks</name><value>100</value></property>
   <property><name>hbase.coprocessor.regionserver.classes</name><value>com.splicemachine.hbase.RegionServerLifecycleObserver</value></property>
   <property><name>hbase.hstore.defaultengine.compactionpolicy.class</name><value>com.splicemachine.compactions.SpliceDefaultCompactionPolicy</value></property>
   <property><name>hbase.hstore.defaultengine.compactor.class</name><value>com.splicemachine.compactions.SpliceDefaultCompactor</value></property>
   <property><name>hbase.htable.threads.max</name><value>96</value></property>
   <property><name>hbase.ipc.warn.response.size</name><value>-1</value></property>
   <property><name>hbase.ipc.warn.response.time</name><value>-1</value></property>
   <property><name>hbase.master.loadbalance.bytable</name><value>true</value></property>
   <property><name>hbase.master.balancer.stochastic.regionCountCost</name><value>1500</value></property>
   <property><name>hbase.mvcc.impl</name><value>org.apache.hadoop.hbase.regionserver.SIMultiVersionConsistencyControl</value></property>
   <property><name>hbase.regions.slop</name><value>0</value></property>
   <property><name>hbase.regionserver.global.memstore.size.lower.limit</name><value>0.9</value></property>
   <property><name>hbase.regionserver.global.memstore.size</name><value>0.25</value></property>
   <property><name>hbase.regionserver.maxlogs</name><value>48</value></property>
   <property><name>hbase.regionserver.wal.enablecompression</name><value>true</value></property>
   <property><name>hbase.rowlock.wait.duration</name><value>0</value></property>
   <property><name>hbase.status.multicast.port</name><value>16100</value></property>
   <property><name>hbase.wal.disruptor.batch</name><value>true</value></property>
   <property><name>hbase.wal.provider</name><value>multiwal</value></property>
   <property><name>hbase.wal.regiongrouping.numgroups</name><value>16</value></property>
   <property><name>hbase.zookeeper.property.tickTime</name><value>6000</value></property>
   <property><name>hfile.block.bloom.cacheonwrite</name><value>true</value></property>
   <property><name>io.storefile.bloom.error.rate</name><value>0.005</value></property>
   <property><name>splice.client.numConnections</name><value>1</value></property>
   <property><name>splice.client.write.maxDependentWrites</name><value>60000</value></property>
   <property><name>splice.client.write.maxIndependentWrites</name><value>60000</value></property>
   <property><name>splice.compression</name><value>snappy</value></property>
   <property><name>splice.marshal.kryoPoolSize</name><value>1100</value></property>
   <property><name>splice.olap_server.clientWaitTime</name><value>900000</value></property>
   <property><name>splice.ring.bufferSize</name><value>131072</value></property>
   <property><name>splice.splitBlockSize</name><value>67108864</value></property>
   <property><name>splice.timestamp_server.clientWaitTime</name><value>120000</value></property>
   <property><name>splice.txn.activeTxns.cacheSize</name><value>10240</value></property>
   <property><name>splice.txn.completedTxns.concurrency</name><value>128</value></property>
   <property><name>splice.txn.concurrencyLevel</name><value>4096</value></property>
   <property><name>hbase.hstore.compaction.min.size</name><value>136314880</value></property>
   <property><name>hbase.hstore.compaction.min</name><value>3</value></property>
   <property><name>hbase.regionserver.thread.compaction.large</name><value>4</value></property>
   <property><name>splice.authentication.native.algorithm</name><value>SHA-512</value></property>
   <property><name>splice.authentication</name><value>NATIVE</value></property>
   <property><name>splice.olap_server.memory</name><value>8196</value></property>   
   <property><name>splice.olap.log4j.configuration</name><value>file:/opt/cloudera/parcels/SPLICEMACHINE/conf/olap-log4j.properties</value></property>   
   ````

3. Set the value of `HBase Client Advanced Configuration Snippet (Safety Valve) for hbase-site.xml`:

   ````
   <property><name>hbase.client.ipc.pool.size</name><value>10</value></property>
   <property><name>hbase.zookeeper.property.tickTime</name><value>6000</value></property>
   <property><name>hfile.block.cache.size</name><value>.1</value></property>
   <property><name>splice.compression</name><value>snappy</value></property>
   <property><name>splice.txn.activeCacheSize</name><value>10240</value></property>
   ````

4. Set the value of Java Configuration Options for HBase Master

   ````
   -XX:MaxPermSize=512M -XX:+HeapDumpOnOutOfMemoryError -XX:MaxDirectMemorySize=2g -XX:+AlwaysPreTouch -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=10101 -Dsplice.spark.enabled=true -Dsplice.spark.app.name=SpliceMachine -Dsplice.spark.master=yarn-client -Dsplice.spark.logConf=true -Dsplice.spark.yarn.maxAppAttempts=1 -Dsplice.spark.driver.maxResultSize=1g -Dsplice.spark.driver.cores=2 -Dsplice.spark.yarn.am.memory=1g -Dsplice.spark.dynamicAllocation.enabled=true -Dsplice.spark.dynamicAllocation.executorIdleTimeout=120 -Dsplice.spark.dynamicAllocation.cachedExecutorIdleTimeout=120 -Dsplice.spark.dynamicAllocation.minExecutors=0 -Dsplice.spark.dynamicAllocation.maxExecutors=12 -Dsplice.spark.io.compression.lz4.blockSize=32k -Dsplice.spark.kryo.referenceTracking=false -Dsplice.spark.kryo.registrator=com.splicemachine.derby.impl.SpliceSparkKryoRegistrator -Dsplice.spark.kryoserializer.buffer.max=512m -Dsplice.spark.kryoserializer.buffer=4m -Dsplice.spark.locality.wait=0 -Dsplice.spark.memory.fraction=0.5 -Dsplice.spark.scheduler.mode=FAIR -Dsplice.spark.serializer=org.apache.spark.serializer.KryoSerializer -Dsplice.spark.shuffle.compress=false -Dsplice.spark.shuffle.file.buffer=128k -Dsplice.spark.shuffle.service.enabled=true  -Dsplice.spark.yarn.am.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.yarn.am.waitTime=10s -Dsplice.spark.yarn.executor.memoryOverhead=2048 -Dsplice.spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/etc/spark/conf/log4j.properties -Dsplice.spark.driver.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar -Dsplice.spark.executor.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/SPLICEMACHINE/lib/*:/opt/cloudera/parcels/SPARK2/lib/spark2/jars/*:/opt/cloudera/parcels/CDH/lib/hbase/lib/* -Dsplice.spark.executor.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.ui.retainedJobs=100 -Dsplice.spark.ui.retainedStages=100 -Dsplice.spark.worker.ui.retainedExecutors=100 -Dsplice.spark.worker.ui.retainedDrivers=100 -Dsplice.spark.streaming.ui.retainedBatches=100 -Dsplice.spark.executor.cores=4 -Dsplice.spark.executor.memory=8g -Dspark.compaction.reserved.slots=4  -Dsplice.spark.local.dir=/tmp -Dsplice.spark.yarn.jars=/opt/cloudera/parcels/SPARK2/lib/spark2/jars/*
   ````

5. Set the value of Java Configuration Options for Region Servers:

   ```
   -XX:+HeapDumpOnOutOfMemoryError -XX:MaxDirectMemorySize=2g -XX:MaxPermSize=512M -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:MaxNewSize=4g -XX:InitiatingHeapOccupancyPercent=60 -XX:ParallelGCThreads=24 -XX:+ParallelRefProcEnabled -XX:MaxGCPauseMillis=5000 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=10102
   ```

6. Click the `Save Changes` button.

## Optional Configuration Modifications

There are a few configuration modifications you might want to make:

* [Modify the Authentication Mechanism](#modify-the-authentication-mechanism) if you want to
  authenticate users with something other than the default *native
  authentication* mechanism.
* [Modify the Log Location](#modify-the-logging-location) if you want your Splice Machine
  log entries stored somewhere other than in the logs for your region
  servers.

### Modify the Authentication Mechanism

Splice Machine installs with Native authentication configured; native
authentication uses the `sys.sysusers` table in the `splice` schema for
configuring user names and passwords.

You can disable authentication or change the authentication mechanism
that Splice Machine uses to LDAP by following the simple instructions in
[Configuring Splice Machine
Authentication](https://doc.splicemachine.com/onprem_install_configureauth.html)

You can use <a href="https://www.cloudera.com/documentation/enterprise/5-8-x/topics/cm_sg_intro_kerb.html" target="_blank">Cloudera's Kerberos Wizard</a> to enable Kerberos mode on a CDH5.8.x cluster. If you're enabling Kerberos, you need to add this option to your HBase Master Java Configuration Options:

````
    -Dsplice.spark.hadoop.fs.hdfs.impl.disable.cache=true
````

### Modify the Log Location

#### Query Statement log

Splice Machine logs all SQL statements by default, storing the log
entries in your region server's logs, as described in our [Using
Logging](developers_tuning_logging) topic. You can modify where Splice
Machine stroes logs by adding the following snippet to your *RegionServer Logging
Advanced Configuration Snippet (Safety Valve)* section of your HBase
Configuration:

   ```
   log4j.appender.spliceDerby=org.apache.log4j.FileAppender
   log4j.appender.spliceDerby.File=${hbase.log.dir}/splice-derby.log
   log4j.appender.spliceDerby.layout=org.apache.log4j.EnhancedPatternLayout
   log4j.appender.spliceDerby.layout.ConversionPattern=%d{EEE MMM d HH:mm:ss,SSS} Thread[%t] %m%n
   log4j.appender.spliceStatement=org.apache.log4j.FileAppender
   log4j.appender.spliceStatement.File=${hbase.log.dir}/splice-statement.log
   log4j.appender.spliceStatement.layout=org.apache.log4j.EnhancedPatternLayout
   log4j.appender.spliceStatement.layout.ConversionPattern=%d{EEE MMM d HH:mm:ss,SSS} Thread[%t] %m%n

   log4j.logger.splice-derby=INFO, spliceDerby
   log4j.additivity.splice-derby=false

   # Uncomment to log statements to a different file:
   #log4j.logger.splice-derby.statement=INFO, spliceStatement
   # Uncomment to not replicate statements to the spliceDerby file:
   #log4j.additivity.splice-derby.statement=false
   ```
   
#### OLAP Server Log

Splice Machine uses log4j to config OLAP server's log. There is a default configuration at `conf/` directory in Splice Machine's 
parcel. It default to write logs to `/var/log/hadoop-yarn`.
If you want to change the log behavior of OLAP server, config `splice.olap.log4j.configuration` in `hbase-site.xml`.
It specifies the log4j.properties file you want to use. This file needs to be available on HBase master server.

## Deploy the Client Configuration

Now that you've updated your configuration information, you need to
deploy it throughout your cluster. You should see a small notification
in the upper right corner of your screen that looks like this:

   ![Clicking the button to tell Cloudera to redeploy the client configuration](https://doc.splicemachine.com/images/CDH.StaleConfig.png)

To deploy your configuration:

1. Click the notification.
2. Click the `Deploy Client Configuration` button.
3. When the deployment completes, click the `Finish` button.

## Restart the Cluster

As a first step, we stop the services that we're about to configure from
the Cloudera Manager home screen:

1. Restart ZooKeeper

   Select `Start` from the `Actions` menu in the upper right corner of
   the ZooKeeper `Configuration` tab to
   restart ZooKeeper.

2. Restart HDFS

   Click the `HDFS Actions` drop-down
   arrow associated with (to the right of) HDFS in the cluster summary
   section of the Cloudera Manager home screen, and then click `Start` to restart HDFS.

   Use your terminal window to create these directories (if they are
   not already available in HDFS):

   ````
   sudo -iu hdfs hadoop fs -mkdir -p hdfs:///user/hbase hdfs:///user/splice/history
   sudo -iu hdfs hadoop fs -chown -R hbase:hbase hdfs:///user/hbase hdfs:///user/splice
   sudo -iu hdfs hadoop fs -chmod 1777 hdfs:///user/splice hdfs:///user/splice/history
   ````

3.  Restart YARN

   Click the `YARN Actions` drop-down
   arrow associated with (to the right of) YARN in the cluster summary
   section of the Cloudera Manager home screen, and then click `Start` to restart YARN.

4.  Restart HBase

   Click the `HBASE Actions` drop-down
   arrow associated with (to the right of) HBASE in the cluster summary
   section of the Cloudera Manager home screen, and then click `Start` to restart HBase.

## Verify your Splice Machine Installation

Now start using the Splice Machine command line interpreter, which is
referred to as *the splice prompt* or simply `splice>` by launching the `sqlshell.sh`
script on any node in your cluster that is running an HBase region
server.

The command line interpreter defaults to connecting on port `1527` on
`localhost`, with username `splice`, and password `admin`. You can
override these defaults when starting the interpreter, as described in
the [Command Line (splice&gt;) Reference](https://doc.splicemachine.com/cmdlineref_intro.html) topic
in our *Developer's Guide*.

Now try entering a few sample commands you can run to verify that
everything is working with your Splice Machine installation.

   <table>
    <col />
    <col />
    <thead>
        <tr>
            <th>Operation</th>
            <th>Command to perform operation</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Display tables</td>
            <td><code>splice&gt; show tables;</code></td>
        </tr>
        <tr>
            <td>Create a table</td>
            <td><code>splice&gt; create table test (i int);</code></td>
        </tr>
        <tr>
            <td>Add data to the table</td>
            <td><code>splice&gt; insert into test values 1,2,3,4,5;</code></td>
        </tr>
        <tr>
            <td>Query data in the table</td>
            <td><code>splice&gt; select * from test;</code></td>
        </tr>
        <tr>
            <td>Drop the table</td>
            <td><code>splice&gt; drop table test;</code></td>
        </tr>
        <tr>
            <td>List available commands</td>
            <td><code>splice&gt; help;</code></td>
        </tr>
        <tr>
            <td>Exit the command line interpreter</td>
            <td><code>splice&gt; exit;</code></td>
        </tr>
        <tr>
            <td colspan="2"><strong>Make sure you end each command with a semicolon</strong> (<code>;</code>), followed by the <em>Enter</em> key or <em>Return</em> key </td>
        </tr>
    </tbody>
   </table>

See the [Command Line (splice&gt;) Reference](https://doc.splicemachine.com/cmdlineref_intro.html)
section of our *Developer's Guide* for information about our commands
and command syntax.
