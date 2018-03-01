---
title: 'Documentation : CDH Install'
---

::: {#page}
::: {#main .aui-page-panel}
::: {#main-header}
::: {#breadcrumb-section}
1.  [Documentation](index.html)
2.  [Documentation](Documentation_197787719.html)
3.  [Installation Documents](Installation-Documents_198475781.html)
:::

[ Documentation : CDH Install ]{#title-text} {#title-heading .pagetitle}
============================================
:::

::: {#content .view}
::: {.page-metadata}
Created by [ Gary Hillerson]{.author}, last modified by [ Murray
Brown]{.editor} on Feb 15, 2018
:::

::: {#main-content .wiki-content .group}
::: {.TopicContent style="text-align: left;"}
Installing and Configuring Splice Machine for Cloudera Manager {#CDHInstall-InstallingandConfiguringSpliceMachineforClouderaManager style="text-align: left;"}
==============================================================

    [Learn about our
products](https://www.splicemachine.com/get-started/){.external-link}

This topic describes installing and configuring Splice Machine on a
Cloudera-managed cluster. Follow these steps:

1.  [Verify
    Prerequisites](http://docstest.splicemachine.com/onprem_install_cloudera.html#Verify){.external-link}
2.  [Install the Splice Machine
    Parcel](http://docstest.splicemachine.com/onprem_install_cloudera.html#Install){.external-link}
3.  [Stop Hadoop
    Services](http://docstest.splicemachine.com/onprem_install_cloudera.html#Stop){.external-link}
4.  [Make Cluster Modifications for Splice
    Machine](http://docstest.splicemachine.com/onprem_install_cloudera.html#ClusterMod){.external-link}
5.  [Configure Hadoop
    Services](http://docstest.splicemachine.com/onprem_install_cloudera.html#Configur8){.external-link}
6.  Make any needed [Optional Configuration
    Modifications](http://docstest.splicemachine.com/onprem_install_cloudera.html#Optional){.external-link}
7.  [Deploy the Client
    Configuration](http://docstest.splicemachine.com/onprem_install_cloudera.html#Deploy){.external-link}
8.  [Restart the
    Cluster](http://docstest.splicemachine.com/onprem_install_cloudera.html#Restart){.external-link}
9.  [Verify your Splice Machine
    Installation](http://docstest.splicemachine.com/onprem_install_cloudera.html#Run){.external-link}

Verify Prerequisites {#CDHInstall-VerifyPrerequisites}
--------------------

Before starting your Splice Machine installation, please make sure that
your cluster contains the prerequisite software components:

-   A cluster running Cloudera Data Hub (CDH) with Cloudera Manager (CM)
-   HBase installed
-   HDFS installed
-   YARN installed
-   ZooKeeper installed

The specific versions of these components that you need depend on your
operating environment, and are called out in detail in
the [Requirements](http://docstest.splicemachine.com/onprem_info_requirements.html){.external-link} topic
of our *Getting Started Guide*.

Install the Splice Machine Parcel {#CDHInstall-InstalltheSpliceMachineParcel}
---------------------------------

Follow these steps to install CDH, Hadoop, Hadoop services, and Splice
Machine on your cluster:

::: {.opsStepsList style="text-align: left;"}
1.  Copy your parcel URL to the clipboard for use in the next step.

    Which Splice Machine parcel URL you need depends upon which Splice
    Machine version you're installing and which version of CDH you are
    using. Here are the URLs for Splice Machine Release 2.7.0 and 2.5.0:

    ::: {.table-wrap}
    Splice Machine Release
    :::

2.  Add the parcel repository

    1.  Make sure the [Use Parcels (Recommended)]{.AppCommand
        style="color: inherit;"} option and the [Matched
        release]{.AppCommand style="color: inherit;"} option are both
        selected.

    2.  Click the [Continue]{.AppCommand style="color: inherit;"} button
        to land on the *More Options* screen.

    3.  Cick the [+]{.AppCommand style="color: inherit;"} button for
        the [Remote Parcel Repository URLs]{.AppCommand
        style="color: inherit;"} field. Paste your Splice Machine
        repository URL into this field.

3.  Use Cloudera Manager to install the parcel.

4.  Verify that the parcel has been distributed and activated.

    The Splice Machine parcel is identified as `SPLICEMACHINE` in the
    Cloudera Manager user interface. Make sure that this parcel has been
    downloaded, distributed, and activated on your cluster.

5.  Restart and redeploy any client changes when Cloudera Manager
    prompts you.
:::

Stop Hadoop Services {#CDHInstall-StopHadoopServices}
--------------------

As a first step, we stop cluster services to allow our installer to make
changes that require the cluster to be temporarily inactive.

From the Cloudera Manager home screen, click the drop-down arrow next to
the cluster on

::: {.opsStepsList style="text-align: left;"}
1.  Select your cluster in Cloudera Manager

    Click the drop-down arrow next to the name of the cluster on which
    you are installing Splice Machine.

2.  Stop the cluster

    Click the [Stop]{.AppCommand style="color: inherit;"} button.
:::

Configure Hadoop Services {#CDHInstall-ConfigureHadoopServices}
-------------------------

Now it's time to make a few modifications in the Hadoop services
configurations:

-   [Configure and Restart the Management
    Service](http://docstest.splicemachine.com/onprem_install_cloudera.html#Configur){.external-link}
-   [Configure
    ZooKeeper](http://docstest.splicemachine.com/onprem_install_cloudera.html#Configur4){.external-link}
-   [Configure
    HDFS](http://docstest.splicemachine.com/onprem_install_cloudera.html#Configur5){.external-link}
-   [Configure
    YARN](http://docstest.splicemachine.com/onprem_install_cloudera.html#Configur2){.external-link}
-   [Configure
    HBASE](http://docstest.splicemachine.com/onprem_install_cloudera.html#Configur3){.external-link}

### Configure and Restart the Management Service {#CDHInstall-ConfigureandRestarttheManagementService}

::: {.opsStepsList style="text-align: left;"}
1.  Select the [Configuration]{.AppCommand style="color: inherit;"} tab
    in CM:

    [![Configuring the Cloudera Manager
    ports](http://docstest.splicemachine.com/images/CM.AlertListenPort.png){.confluence-embedded-image
    .nestedTightSpacing
    .confluence-external-resource}]{.confluence-embedded-file-wrapper}

2.  Change the value of the Alerts: Listen Port
    to [10110]{.AppFontCustCode style="color: inherit;"}.

3.  Save changes and restart the Management Service.
:::

### Configure ZooKeeper {#CDHInstall-ConfigureZooKeeper}

To edit the ZooKeeper configuration, click [ZooKeeper]{.AppCommand
style="color: inherit;"} in the Cloudera Manager (CM) home screen, then
click the [Configuration]{.AppCommand style="color: inherit;"} tab and
follow these steps:

::: {.opsStepsList style="text-align: left;"}
1.  Select the [Service-Wide]{.AppCommand
    style="color: inherit;"} category.

    Make the following changes:

    ::: {.preWrapperWide}
    Maximum Client Connections = 0 Maximum Session Timeout = 120000
    :::

    Click the [Save Changes]{.AppCommand
    style="color: inherit;"} button.
:::

### Configure HDFS {#CDHInstall-ConfigureHDFS}

To edit the HDFS configuration, click [HDFS]{.AppCommand
style="color: inherit;"} in the Cloudera Manager home screen, then click
the [Configuration]{.AppCommand style="color: inherit;"} tab and make
these changes:

::: {.opsStepsList style="text-align: left;"}
1.  Verify that the HDFS data directories for your cluster are set up to
    use your data disks.

2.  Change the values of these settings

    ::: {.table-wrap}
      Setting                               New Value
      ------------------------------------- -----------
      Handler Count                         20
      Maximum Number of Transfer Threads    8192
      NameNodeHandler Count                 64
      NameNode Service Handler Count        60
      Replication Factor                    2 or 3 \*
      Java Heap Size of DataNode in Bytes   2 GB
    :::

3.  Click the [Save Changes]{.AppCommand
    style="color: inherit;"} button.
:::

### Configure YARN {#CDHInstall-ConfigureYARN}

To edit the YARN configuration, click [YARN]{.AppCommand
style="color: inherit;"} in the Cloudera Manager home screen, then click
the [Configuration]{.AppCommand style="color: inherit;"} tab and make
these changes:

::: {.opsStepsList style="text-align: left;"}
1.  Verify that the following directories are set up to use your data
    disks.

    ::: {.table-wrap}
      --------------------------------
      NodeManager Local Directories\
      NameNode Data Directories\
      HDFS Checkpoint Directories
      --------------------------------
    :::

2.  Change the values of these settings

    ::: {.table-wrap}
    +-------------------------------------+-----------------------------+
    | Setting                             | New Value                   |
    +:====================================+:============================+
    | Heartbeat Interval                  | 100 ms                      |
    +-------------------------------------+-----------------------------+
    | MR Application Classpath            | ::: {.preWrapperWide}       |
    |                                     | ``` {.Example}              |
    |                                     | $HADOOP_MAPRED_HOME/*       |
    |                                     | $HADOOP_MAPRED_HOME/lib/*   |
    |                                     | $MR2_CLASSPATH              |
    |                                     | ```                         |
    |                                     | :::                         |
    +-------------------------------------+-----------------------------+
    | YARN Application Classpath          | ::: {.preWrapperWide}       |
    |                                     | ``` {.Example}              |
    |                                     | $HADOOP_CLIENT_CONF_DIR     |
    |                                     | $HADOOP_CONF_DIR            |
    |                                     | $HADOOP_COMMON_HOME/*       |
    |                                     | $HADOOP_COMMON_HOME/lib/*   |
    |                                     | $HADOOP_HDFS_HOME/*         |
    |                                     | $HADOOP_HDFS_HOME/lib/*     |
    |                                     | $HADOOP_YARN_HOME/*         |
    |                                     | $HADOOP_YARN_HOME/lib/*     |
    |                                     | $HADOOP_MAPRED_HOME/*       |
    |                                     | $HADOOP_MAPRED_HOME/lib/*   |
    |                                     | $MR2_CLASSPATH              |
    |                                     | ```                         |
    |                                     | :::                         |
    +-------------------------------------+-----------------------------+
    | Localized Dir Deletion Delay        | 86400                       |
    +-------------------------------------+-----------------------------+
    | JobHistory Server Max Log Size      | 1 GB                        |
    +-------------------------------------+-----------------------------+
    | NodeManager Max Log Size            | 1 GB                        |
    +-------------------------------------+-----------------------------+
    | ResourceManager Max Log Size        | 1 GB                        |
    +-------------------------------------+-----------------------------+
    | Container Memory                    | 30 GB (based on node specs) |
    +-------------------------------------+-----------------------------+
    | Container Memory Maximum            | 30 GB (based on node specs) |
    +-------------------------------------+-----------------------------+
    | Container Virtual CPU Cores         | 19 (based on node specs)    |
    +-------------------------------------+-----------------------------+
    | Container Virtual CPU Cores Maximum | 19 (Based on node specs)    |
    +-------------------------------------+-----------------------------+
    :::

3.  Add property values

    You need to add the same two property values to each of four YARN
    advanced configuration settings.

    Add these properties:

    ::: {.table-wrap}
      XML Property Name                                    XML Property Value
      ---------------------------------------------------- --------------------------------------------------
      yarn.nodemanager.aux-services.spark\_shuffle.class   org.apache.spark.network.yarn.YarnShuffleService
      yarn.nodemanager.aux-services                        mapreduce\_shuffle,spark\_shuffle
    :::

    To each of these YARN settings:

    -   Yarn Service Advanced Configuration Snippet (Safety Valve) for
        yarn-site.xml

    -   Yarn Client Advanced Configuration Snippet (Safety Valve) for
        yarn-site.xml

    -   NodeManager Advanced Configuration Snippet (Safety Valve) for
        yarn-site.xml

    -   ResourceManager Advanced Configuration Snippet (Safety Valve)
        for yarn-site.xml

4.  Click the [Save Changes]{.AppCommand
    style="color: inherit;"} button.
:::

### Configure HBASE {#CDHInstall-ConfigureHBASE}

To edit the HBASE configuration, click [HBASE]{.AppCommand
style="color: inherit;"} in the Cloudera Manager home screen, then click
the [Configuration]{.AppCommand style="color: inherit;"} tab and make
these changes:

::: {.opsStepsList style="text-align: left;"}
1.  Change the values of these settings

    ::: {.table-wrap}
    +-----------------------------------+-----------------------------------+
    | Setting                           | New Value                         |
    +:==================================+:==================================+
    | HBase Client Scanner Caching      | 100 ms                            |
    +-----------------------------------+-----------------------------------+
    | Graceful Shutdown Timeout         | 30 seconds                        |
    +-----------------------------------+-----------------------------------+
    | HBase Service Advanced            | [The property list for the Safety |
    | Configuration Snippet (Safety     | Valve snippet is shown below, in  |
    | Valve) for hbase-site.xml         | Step 2]{.bodyFont                 |
    |                                   | style="color: inherit;"}          |
    +-----------------------------------+-----------------------------------+
    | SplitLog Manager Timeout          | 5 minutes                         |
    +-----------------------------------+-----------------------------------+
    | Maximum HBase Client Retries      | 40                                |
    +-----------------------------------+-----------------------------------+
    | RPC Timeout                       | 20 minutes (or 1200000            |
    |                                   | milliseconds)                     |
    +-----------------------------------+-----------------------------------+
    | HBase Client Pause                | 90                                |
    +-----------------------------------+-----------------------------------+
    | ZooKeeper Session Timeout         | 120000                            |
    +-----------------------------------+-----------------------------------+
    | HBase Master Web UI Port          | 16010                             |
    +-----------------------------------+-----------------------------------+
    | HBase Master Port                 | 16000                             |
    +-----------------------------------+-----------------------------------+
    | Java Configuration Options for    | [The HBase Master Java            |
    | HBase Master                      | configuration options list is     |
    |                                   | shown below, in Step 3]{.bodyFont |
    |                                   | style="color: inherit;"}          |
    +-----------------------------------+-----------------------------------+
    | HBase Coprocessor Master Classes  | com.splicemachine.hbase.SpliceMas |
    |                                   | terObserver                       |
    +-----------------------------------+-----------------------------------+
    | Java Heap Size of HBase Master in | 5 GB                              |
    | Bytes                             |                                   |
    +-----------------------------------+-----------------------------------+
    | HStore Compaction Threshold       | 5                                 |
    +-----------------------------------+-----------------------------------+
    | HBase RegionServer Web UI port    | 16030                             |
    +-----------------------------------+-----------------------------------+
    | HStore Blocking Store Files       | 20                                |
    +-----------------------------------+-----------------------------------+
    | Java Configuration Options for    | [The HBase RegionServerJava       |
    | HBase RegionServer                | configuration options list is     |
    |                                   | shown below, in Step 4]{.bodyFont |
    |                                   | style="color: inherit;"}          |
    +-----------------------------------+-----------------------------------+
    | HBase Memstore Block Multiplier   | 4                                 |
    +-----------------------------------+-----------------------------------+
    | Maximum Number of HStoreFiles     | 7                                 |
    | Compaction                        |                                   |
    +-----------------------------------+-----------------------------------+
    | HBase RegionServer Lease Period   | 20 minutes (or 1200000            |
    |                                   | milliseconds)                     |
    +-----------------------------------+-----------------------------------+
    | HFile Block Cache Size            | 0.25                              |
    +-----------------------------------+-----------------------------------+
    | Java Heap Size of HBase           | 24 GB                             |
    | RegionServer in Bytes             |                                   |
    +-----------------------------------+-----------------------------------+
    | HBase RegionServer Handler Count  | 200                               |
    +-----------------------------------+-----------------------------------+
    | HBase RegionServer Meta-Handler   | 200                               |
    | Count                             |                                   |
    +-----------------------------------+-----------------------------------+
    | HBase Coprocessor Region Classes  | com.splicemachine.hbase.MemstoreA |
    |                                   | wareObserver\                     |
    |                                   | com.splicemachine.derby.hbase.Spl |
    |                                   | iceIndexObserver\                 |
    |                                   | com.splicemachine.derby.hbase.Spl |
    |                                   | iceIndexEndpoint\                 |
    |                                   | com.splicemachine.hbase.RegionSiz |
    |                                   | eEndpoint\                        |
    |                                   | [com.splicemachine.si](http://com |
    |                                   | .splicemachine.si){.external-link |
    |                                   | }.data.hbase.coprocessor.TxnLifec |
    |                                   | ycleEndpoint\                     |
    |                                   | [com.splicemachine.si](http://com |
    |                                   | .splicemachine.si){.external-link |
    |                                   | }.data.hbase.coprocessor.SIObserv |
    |                                   | er\                               |
    |                                   | com.splicemachine.hbase.BackupEnd |
    |                                   | pointObserver                     |
    +-----------------------------------+-----------------------------------+
    | Maximum number of Write-Ahead Log | 48                                |
    | (WAL) files                       |                                   |
    +-----------------------------------+-----------------------------------+
    | RegionServer Small Compactions    | 4                                 |
    | Thread Count                      |                                   |
    +-----------------------------------+-----------------------------------+
    | HBase RegionServer Port           | 16020                             |
    +-----------------------------------+-----------------------------------+
    | Per-RegionServer Number of WAL    | 16                                |
    | Pipelines                         |                                   |
    +-----------------------------------+-----------------------------------+
    :::

2.  Set the value
    of `HBase Service Advanced Configuration Snippet (Safety Valve)` for `hbase-site.xml`:\
    \

    ::: {.preWrapperWide}
    ``` {.Example}
    <property><name>dfs.client.read.shortcircuit.buffer.size</name><value>131072</value></property>
    <property><name>hbase.balancer.period</name><value>60000</value></property>
    <property><name>hbase.client.ipc.pool.size</name><value>10</value></property>
    <property><name>hbase.client.max.perregion.tasks</name><value>100</value></property>
    <property><name>hbase.coprocessor.regionserver.classes</name><value>com.splicemachine.hbase.RegionServerLifecycleObserver</value></property><property><name>hbase.hstore.defaultengine.compactionpolicy.class</name><value>com.splicemachine.compactions.SpliceDefaultCompactionPolicy</value></property>
    <property><name>hbase.hstore.defaultengine.compactor.class</name><value>com.splicemachine.compactions.SpliceDefaultCompactor</value></property>
    <property><name>hbase.htable.threads.max</name><value>96</value></property>
    <property><name>hbase.ipc.warn.response.size</name><value>-1</value></property>
    <property><name>hbase.ipc.warn.response.time</name><value>-1</value></property>
    <property><name>hbase.master.loadbalance.bytable</name><value>true</value></property>
    <property><name>hbase.mvcc.impl</name><value>org.apache.hadoop.hbase.regionserver.SIMultiVersionConsistencyControl</value></property>
    <property><name>hbase.regions.slop</name><value>0.01</value></property>
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
    <property><name>hbase.hstore.compaction.max.size</name><value>260046848</value></property>
    <property><name>hbase.hstore.compaction.min.size</name><value>16777216</value></property>
    <property><name>hbase.hstore.compaction.min</name><value>5</value></property>
    <property><name>hbase.regionserver.thread.compaction.large</name><value>1</value></property>
    <property><name>splice.authentication.native.algorithm</name><value>SHA-512</value></property>
    <property><name>splice.authentication</name><value>NATIVE</value></property>
    ```

    ``` {.Example}
    ```
    :::

3.  Set the value of Java Configuration Options for HBase Master

    ::: {.code .panel .pdl style="border-width: 1px;"}
    ::: {.codeHeader .panelHeader .pdl .hide-border-bottom}
     [[ ]{.expand-control-icon .icon}[Expand
    source]{.expand-control-text}]{.collapse-source .expand-control
    style="display:none;"} []{.collapse-spinner-wrapper}
    :::

    ::: {.codeContent .panelContent .pdl .hide-toolbar}
    ``` {.syntaxhighlighter-pre data-syntaxhighlighter-params="brush: java; gutter: false; theme: Confluence; collapse: true" data-theme="Confluence"}
    -XX:MaxPermSize=512M -XX:+HeapDumpOnOutOfMemoryError -XX:MaxDirectMemorySize=2g -XX:+AlwaysPreTouch -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=10101 -Dsplice.spark.enabled=true -Dsplice.spark.app.name=SpliceMachine -Dsplice.spark.master=yarn-client -Dsplice.spark.logConf=true -Dsplice.spark.yarn.maxAppAttempts=1 -Dsplice.spark.driver.maxResultSize=1g -Dsplice.spark.driver.cores=2 -Dsplice.spark.yarn.am.memory=1g -Dsplice.spark.dynamicAllocation.enabled=true -Dsplice.spark.dynamicAllocation.executorIdleTimeout=120 -Dsplice.spark.dynamicAllocation.cachedExecutorIdleTimeout=120 -Dsplice.spark.dynamicAllocation.minExecutors=0 -Dsplice.spark.dynamicAllocation.maxExecutors=12 -Dsplice.spark.io.compression.lz4.blockSize=32k -Dsplice.spark.kryo.referenceTracking=false -Dsplice.spark.kryo.registrator=com.splicemachine.derby.impl.SpliceSparkKryoRegistrator -Dsplice.spark.kryoserializer.buffer.max=512m -Dsplice.spark.kryoserializer.buffer=4m -Dsplice.spark.locality.wait=100 -Dsplice.spark.memory.fraction=0.5 -Dsplice.spark.scheduler.mode=FAIR -Dsplice.spark.serializer=org.apache.spark.serializer.KryoSerializer -Dsplice.spark.shuffle.compress=false -Dsplice.spark.shuffle.file.buffer=128k -Dsplice.spark.shuffle.service.enabled=true  -Dsplice.spark.yarn.am.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.yarn.am.waitTime=10s -Dsplice.spark.yarn.executor.memoryOverhead=2048 -Dsplice.spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/etc/spark/conf/log4j.properties -Dsplice.spark.driver.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.driver.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/jars/htrace-core-3.1.0-incubating.jar -Dsplice.spark.executor.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/conf:/opt/cloudera/parcels/SPLICEMACHINE/lib/*:/opt/cloudera/parcels/SPARK2/lib/spark2/jars/*:/opt/cloudera/parcels/CDH/lib/hbase/lib/* -Dsplice.spark.executor.extraLibraryPath=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native -Dsplice.spark.ui.retainedJobs=100 -Dsplice.spark.ui.retainedStages=100 -Dsplice.spark.worker.ui.retainedExecutors=100 -Dsplice.spark.worker.ui.retainedDrivers=100 -Dsplice.spark.streaming.ui.retainedBatches=100 -Dsplice.spark.executor.cores=4 -Dsplice.spark.executor.memory=8g -Dspark.compaction.reserved.slots=4 -Dsplice.spark.eventLog.enabled=true -Dsplice.spark.eventLog.dir=hdfs:///user/splice/history -Dsplice.olap_server.memory=12288 -Dsplice.spark.local.dir=/tmp -Dsplice.spark.yarn.jars=/opt/cloudera/parcels/SPARK2/lib/spark2/jars/* 
    ```
    :::
    :::

    \

    ::: {.preWrapperWide}
    ``` {.Example}
    ```
    :::

4.  Set the value of Java Configuration Options for Region Servers:

    ::: {.code .panel .pdl style="border-width: 1px;"}
    ::: {.codeHeader .panelHeader .pdl .hide-border-bottom}
     [[ ]{.expand-control-icon .icon}[Expand
    source]{.expand-control-text}]{.collapse-source .expand-control
    style="display:none;"} []{.collapse-spinner-wrapper}
    :::

    ::: {.codeContent .panelContent .pdl .hide-toolbar}
    ``` {.syntaxhighlighter-pre data-syntaxhighlighter-params="brush: java; gutter: false; theme: Confluence; collapse: true" data-theme="Confluence"}
    -XX:+HeapDumpOnOutOfMemoryError -XX:MaxDirectMemorySize=2g -XX:MaxPermSize=512M -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:MaxNewSize=4g -XX:InitiatingHeapOccupancyPercent=60 -XX:ParallelGCThreads=24 -XX:+ParallelRefProcEnabled -XX:MaxGCPauseMillis=5000 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=10102
    ```
    :::
    :::

5.  Click the [Save Changes]{.AppCommand
    style="color: inherit;"} button.
:::

Optional Configuration Modifications {#CDHInstall-OptionalConfigurationModifications}
------------------------------------

There are a few configuration modifications you might want to make:

-   [Modify the Authentication
    Mechanism](http://docstest.splicemachine.com/onprem_install_cloudera.html#Modify){.external-link} if
    you want to authenticate users with something other than the
    default *native authentication*mechanism.
-   [Modify the Log
    Location](http://docstest.splicemachine.com/onprem_install_cloudera.html#Logging){.external-link} if
    you want your Splice Machine log entries stored somewhere other than
    in the logs for your region servers.

### Modify the Authentication Mechanism {#CDHInstall-ModifytheAuthenticationMechanism}

Splice Machine installs with Native authentication configured; native
authentication uses the `sys.sysusers` table in the `splice` schema for
configuring user names and passwords.

You can disable authentication or change the authentication mechanism
that Splice Machine uses to LDAP by following the simple instructions
in [Configuring Splice Machine
Authentication](http://docstest.splicemachine.com/onprem_install_configureauth.html){.external-link}

You can use [Cloudera's Kerberos
Wizard](https://www.cloudera.com/documentation/enterprise/5-8-x/topics/cm_sg_intro_kerb.html){.external-link} to
enable Kerberos mode on a CDH5.8.x cluster. If you're enabling Kerberos,
you need to add this option to your HBase Master Java Configuration
Options:

::: {.preWrapper}
``` {.Example}
-Dsplice.spark.hadoop.fs.hdfs.impl.disable.cache=true
```
:::

### Modify the Log Location {#CDHInstall-ModifytheLogLocation}

Splice Machine logs all SQL statements by default, storing the log
entries in your region server's logs, as described in our [Using
Logging](http://docstest.splicemachine.com/developers_tuning_logging){.external-link} topic.
You can modify where Splice Machine stroes logs by adding the following
snippet to your *RegionServer Logging Advanced Configuration Snippet
(Safety Valve)* section of your HBase Configuration:

::: {.preWrapper}
``` {.Plain}
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
:::

Deploy the Client Configuration {#CDHInstall-DeploytheClientConfiguration}
-------------------------------

Now that you've updated your configuration information, you need to
deploy it throughout your cluster. You should see a small notification
in the upper right corner of your screen that looks like this:

[![Clicking the button to tell Cloudera to redeploy the client
configuration](http://docstest.splicemachine.com/images/CDH.StaleConfig.png){.confluence-embedded-image
.nestedTightSpacing
.confluence-external-resource}]{.confluence-embedded-file-wrapper}

To deploy your configuration:

::: {.opsStepsList style="text-align: left;"}
1.  Click the notification.
2.  Click the [Deploy Client Configuration]{.AppCommand
    style="color: inherit;"} button.
3.  When the deployment completes, click the [Finish]{.AppCommand
    style="color: inherit;"} button.
:::

Restart the Cluster {#CDHInstall-RestarttheCluster}
-------------------

As a first step, we stop the services that we're about to configure from
the Cloudera Manager home screen:

::: {.opsStepsList style="text-align: left;"}
1.  Restart ZooKeeper

    Select [Start]{.AppCommand style="color: inherit;"} from
    the [Actions]{.AppCommand style="color: inherit;"} menu in the upper
    right corner of the ZooKeeper [Configuration]{.AppCommand
    style="color: inherit;"} tab to restart ZooKeeper.

2.  Restart HDFS

    Click the [HDFS Actions]{.AppCommand
    style="color: inherit;"} drop-down arrow associated with (to the
    right of) HDFS in the cluster summary section of the Cloudera
    Manager home screen, and then click [Start]{.AppCommand
    style="color: inherit;"} to restart HDFS.

    Use your terminal window to create these directories (if they are
    not already available in HDFS):

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    sudo -iu hdfs hadoop fs -mkdir -p hdfs:///user/hbase hdfs:///user/splice/history
    sudo -iu hdfs hadoop fs -chown -R hbase:hbase hdfs:///user/hbase hdfs:///user/splice
    sudo -iu hdfs hadoop fs -chmod 1777 hdfs:///user/splice hdfs:///user/splice/history
    ```
    :::

3.  Restart YARN

    Click the [YARN Actions]{.AppCommand
    style="color: inherit;"} drop-down arrow associated with (to the
    right of) YARN in the cluster summary section of the Cloudera
    Manager home screen, and then click [Start]{.AppCommand
    style="color: inherit;"} to restart YARN.

4.  Restart HBase

    Click the [HBASE Actions]{.AppCommand
    style="color: inherit;"} drop-down arrow associated with (to the
    right of) HBASE in the cluster summary section of the Cloudera
    Manager home screen, and then click [Start]{.AppCommand
    style="color: inherit;"} to restart HBase.
:::

Verify your Splice Machine Installation {#CDHInstall-VerifyyourSpliceMachineInstallation}
---------------------------------------

Now start using the Splice Machine command line interpreter, which is
referred to as *the splice prompt* or simply [splice\>]{.AppCommand
style="color: inherit;"} by launching the `sqlshell.sh` script on any
node in your cluster that is running an HBase region server.

The command line interpreter defaults to connecting on
port `1527` on `localhost`, with username `splice`, and
password `admin`. You can override these defaults when starting the
interpreter, as described in the [Command Line (splice\>)
Reference](http://docstest.splicemachine.com/cmdlineref_intro.html){.external-link} topic
in our *Developer's Guide*.

Now try entering a few sample commands you can run to verify that
everything is working with your Splice Machine installation.

::: {.table-wrap}
Operation
:::
:::
:::
:::
:::
:::

Command to perform operation

Display tables

::: {.preWrapperWide}
``` {.AppCommandCell}
splice> show tables;
```
:::

Create a table

::: {.preWrapperWide}
``` {.AppCommandCell}
splice> create table test (i int);
```
:::

Add data to the table

::: {.preWrapperWide}
``` {.AppCommandCell}
splice> insert into test values 1,2,3,4,5;
```
:::

Query data in the table

::: {.preWrapperWide}
``` {.AppCommandCell}
splice> select * from test;
```
:::

Drop the table

::: {.preWrapperWide}
``` {.AppCommandCell}
splice> drop table test;
```
:::

List available commands

::: {.preWrapperWide}
``` {.AppCommandCell}
splice> help;
```
:::

Exit the command line interpreter

::: {.preWrapperWide}
``` {.AppCommandCell}
splice> exit;
```
:::

**Make sure you end each command with a semicolon** (`;`), followed by
the *Enter* key or *Return* key

See the [Command Line (splice\>)
Reference](http://docstest.splicemachine.com/cmdlineref_intro.html){.external-link} section
of our *Developer's Guide* for information about our commands and
command syntax.

::: {.pageSection .group}
::: {.pageSectionHeader}
Attachments: {#attachments .pageSectionTitle}
------------
:::

::: {.greybox align="left"}
![](images/icons/bullet_blue.gif){width="8" height="8"}
[cmdlineref\_analyze.html](attachments/198180865/198246401.html)
(text/html)\
![](images/icons/bullet_blue.gif){width="8" height="8"}
[gtest1.docx](attachments/198180865/198148103.docx)
(application/vnd.openxmlformats-officedocument.wordprocessingml.document)\
:::
:::

::: {#footer role="contentinfo"}
::: {.section .footer-body}
Document generated by Confluence on Feb 21, 2018 15:36

::: {#footer-logo}
[Atlassian](http://www.atlassian.com/)
:::
:::
:::
