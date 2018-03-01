---
title: 'Documentation : Hortonworks Install'
---

::: {#page}
::: {#main .aui-page-panel}
::: {#main-header}
::: {#breadcrumb-section}
1.  [Documentation](index.html)
2.  [Documentation](Documentation_197787719.html)
3.  [Installation Documents](Installation-Documents_198475781.html)
:::

[ Documentation : Hortonworks Install ]{#title-text} {#title-heading .pagetitle}
====================================================
:::

::: {#content .view}
::: {.page-metadata}
Created by [ Gary Hillerson]{.author}, last modified on Feb 07, 2018
:::

::: {#main-content .wiki-content .group}
Installing and Configuring Splice Machine for Hortonworks HDP {#HortonworksInstall-InstallingandConfiguringSpliceMachineforHortonworksHDP style="text-align: left;"}
=============================================================

    [Learn about our
products](https://www.splicemachine.com/get-started/){.external-link}

This topic describes installing and configuring Splice Machine on a
Hortonworks Ambari-managed cluster. Follow these steps:

1.  [Verify
    Prerequisites](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Verify){.external-link}
2.  [Download and Install Splice
    Machine](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Install){.external-link}
3.  [Stop Hadoop
    Services](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Stop){.external-link}
4.  [Configure Hadoop
    Services](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Configur8){.external-link}
5.  [Start Any Additional
    Services](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Start){.external-link}
6.  Make any needed [Optional Configuration
    Modifications](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Optional){.external-link}
7.  [Verify your Splice Machine
    Installation](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Run){.external-link}

Verify Prerequisites {#HortonworksInstall-VerifyPrerequisites style="text-align: left;"}
--------------------

Before starting your Splice Machine installation, please make sure that
your cluster contains the prerequisite software components:

-   A cluster running HDP
-   Ambari installed and configured for HDP
-   HBase installed
-   HDFS installed
-   YARN installed
-   ZooKeeper installed
-   Ensure that Phoenix services are **NOT** installed on your cluster,
    as they interfere with Splice Machine HBase settings.

The specific versions of these components that you need depend on your
operating environment, and are called out in detail in
the [Requirements](http://docstest.splicemachine.com/onprem_info_requirements.html){.external-link} topic
of our *Getting Started Guide*.

Download and Install Splice Machine {#HortonworksInstall-DownloadandInstallSpliceMachine style="text-align: left;"}
-----------------------------------

Perform the following steps [on each node]{.important
style="color: rgb(255,0,0);"} in your cluster:

::: {.opsStepsList style="text-align: left;"}
1.  Download the installer for your version.

    Which Splice Machine installer (gzip) package you need depends upon
    which Splice Machine version you're installing and which version of
    HDP you are using. Here are the URLs for Splice Machine Release
    2.7.0 and 2.5.0:

    ::: {.table-wrap}
    Splice Machine Release
    :::

2.  Create the `splice` installation directory:

    ::: {.preWrapperWide}
    ``` {.ShellCommandCell}
    sudo mkdir -p /opt/splice
    ```
    :::

3.  Download the Splice Machine package into the `splice` directory on
    the node. For example:

    ::: {.preWrapperWide}
    ``` {.ShellCommandCell}
    sudo curl '////SPLICEMACHINE-..' -o /opt/splice/SPLICEMACHINE-..
    ```
    :::

4.  Extract the Splice Machine package:

    ::: {.preWrapperWide}
    ``` {.ShellCommandCell}
    sudo tar -xf SPLICEMACHINE-.. --directory /opt/splice
    ```
    :::

5.  Run our script as *root* user [on each node]{.important
    style="color: rgb(255,0,0);"} in your cluster to add symbolic links
    to set up Splice Machine jar script symbolic links

    Issue this command [on each node]{.important
    style="color: rgb(255,0,0);"} in your cluster:

    ::: {.preWrapperWide}
    ``` {.AppCommand}
    sudo /opt/splice/default/scripts/install-splice-symlinks.sh
    ```
    :::
:::

Stop Hadoop Services {#HortonworksInstall-StopHadoopServices style="text-align: left;"}
--------------------

As a first step, we stop cluster services to allow our installer to make
changes that require the cluster to be temporarily inactive.

::: {.opsStepsList style="text-align: left;"}
1.  Access the Ambari Home Screen

2.  Click the [Actions]{.AppCommand style="color: inherit;"} drop-down
    in the Ambari *Services* sidebar, and then click the [Stop
    All]{.AppCommand style="color: inherit;"} button.
:::

Configure Hadoop Services {#HortonworksInstall-ConfigureHadoopServices style="text-align: left;"}
-------------------------

Now it's time to make a few modifications in the Hadoop services
configurations:

-   [Configure and Restart
    ZooKeeper](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Configur4){.external-link}
-   [Configure and Restart
    HDFS](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Configur5){.external-link}
-   [Configure and Restart
    YARN](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Configur2){.external-link}
-   [Configure
    MapReduce2](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Configur){.external-link}
-   [Configure and Restart
    HBASE](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Configur3){.external-link}

### Configure and Restart ZooKeeper {#HortonworksInstall-ConfigureandRestartZooKeeper style="text-align: left;"}

To edit the ZooKeeper configuration, click [ZooKeeper]{.AppCommand
style="color: inherit;"} in the Ambari *Services* sidebar. Then click
the [Configs]{.AppCommand style="color: inherit;"} tab and follow these
steps:

::: {.opsStepsList style="text-align: left;"}
1.  Click the *Custom zoo.cfg* drop-down arrow, then click [Add
    Property]{.AppCommand style="color: inherit;"} to add
    the `maxClientCnxns` property and then again to add
    the `maxSessionTimeout` property, with these values:

    ::: {.preWrapperWide}
    ``` {.AppCommand}
    maxClientCnxns=0
    maxSessionTimeout=120000
    ```
    :::

2.  Click the [Save]{.AppCommand style="color: inherit;"} button to save
    your changes. You'll be prompted to optionally add a note such
    as `Updated ZooKeeper configuration for Splice Machine`.
    Click [Save]{.AppCommand style="color: inherit;"} again.

3.  Select the [Actions]{.AppCommand style="color: inherit;"} drop-down
    in the Ambari *Services* sidebar, then click the [Start]{.AppCommand
    style="color: inherit;"} action to start ZooKeeper. Wait for the
    restart to complete.
:::

### Configure and Restart HDFS {#HortonworksInstall-ConfigureandRestartHDFS style="text-align: left;"}

To edit the HDFS configuration, click [HDFS]{.AppCommand
style="color: inherit;"} in the Ambari *Services* sidebar. Then click
the [Configs]{.AppCommand style="color: inherit;"} tab and follow these
steps:

::: {.opsStepsList style="text-align: left;"}
1.  Edit the HDFS configuration as follows:

    ::: {.table-wrap}
      --------------------------------- -------------------------------------------------------------------------------
      NameNode Java heap size           4 GB

      DataNode maximum Java heap size   2 GB

      Block replication                 2 [(for clusters with less than 8 nodes)]{.bodyFont style="color: inherit;"}\
                                        3 [(for clusters with 8 or more nodes)]{.bodyFont style="color: inherit;"}
      --------------------------------- -------------------------------------------------------------------------------
    :::

2.  Add a new property:

    Click [Add Property]{.AppCommand style="color: inherit;"}...
    under [Custom hdfs-site]{.AppCommand style="color: inherit;"}, and
    add the following property:

    ::: {.preWrapperWide}
    ``` {.AppCommand}
    dfs.datanode.handler.count=20
    ```
    :::

3.  Save Changes

    Click the [Save]{.AppCommand style="color: inherit;"} button to save
    your changes. You'll be prompted to optionally add a note such
    as `Updated HDFS configuration for Splice Machine`.
    Click [Save]{.AppCommand style="color: inherit;"} again.

4.  Start HDFS

    After you save your changes, you'll land back on the [HDFS Service
    Configs]{.AppCommand style="color: inherit;"} tab in Ambari.

    Click the [Actions]{.AppCommand style="color: inherit;"} drop-down
    in the Ambari *Services* sidebar, then click the [Start]{.AppCommand
    style="color: inherit;"} action to start HDFS. Wait for the restart
    to complete.

5.  Create directories for hbase user and the Splice Machine YARN
    application:

    Use your terminal window to create these directories:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    sudo -iu hdfs hadoop fs -mkdir -p hdfs:///user/hbase hdfs:///user/splice/history
    sudo -iu hdfs hadoop fs -chown -R hbase:hbase hdfs:///user/hbase hdfs:///user/splice
    sudo -iu hdfs hadoop fs -chmod 1777 hdfs:///user/splice hdfs:///user/splice/history
    ```
    :::
:::

### Configure and Restart YARN {#HortonworksInstall-ConfigureandRestartYARN style="text-align: left;"}

To edit the YARN configuration, click [YARN]{.AppCommand
style="color: inherit;"} in the Ambari *Services* sidebar. Then click
the [Configs]{.AppCommand style="color: inherit;"} tab and follow these
steps:

::: {.opsStepsList style="text-align: left;"}
1.  Update these other configuration values:

    ::: {.table-wrap}
    +-----------------------------------+-----------------------------------+
    | Setting                           | New Value                         |
    +:==================================+:==================================+
    | yarn.application.classpath        | ::: {.preWrapperWide}             |
    |                                   | ``` {.Example}                    |
    |                                   | $HADOOP_CONF_DIR,/usr/hdp/current |
    |                                   | /hadoop-client/*,/usr/hdp/current |
    |                                   | /hadoop-client/lib/*,/usr/hdp/cur |
    |                                   | rent/hadoop-hdfs-client/*,/usr/hd |
    |                                   | p/current/hadoop-hdfs-client/lib/ |
    |                                   | *,/usr/hdp/current/hadoop-yarn-cl |
    |                                   | ient/*,/usr/hdp/current/hadoop-ya |
    |                                   | rn-client/lib/*,/usr/hdp/current/ |
    |                                   | hadoop-mapreduce-client/*,/usr/hd |
    |                                   | p/current/hadoop-mapreduce-client |
    |                                   | /lib/*,/usr/hdp/current/hbase-reg |
    |                                   | ionserver/*,/usr/hdp/current/hbas |
    |                                   | e-regionserver/lib/*,/opt/splice/ |
    |                                   | default/lib/*                     |
    |                                   | ```                               |
    |                                   | :::                               |
    +-----------------------------------+-----------------------------------+
    | yarn.nodemanager.aux-services.spa | /opt/splice/default/lib/\*        |
    | rk2\_shuffle.classpath            |                                   |
    +-----------------------------------+-----------------------------------+
    | yarn.nodemanager.aux-services.spa | /opt/splice/default/lib/\*        |
    | rk\_shuffle.classpath             |                                   |
    +-----------------------------------+-----------------------------------+
    | yarn.nodemanager.aux-services.spa | org.apache.spark.network.yarn.Yar |
    | rk2\_shuffle.class                | nShuffleService                   |
    +-----------------------------------+-----------------------------------+
    | yarn.nodemanager.delete.debug-del | 86400                             |
    | ay-sec                            |                                   |
    +-----------------------------------+-----------------------------------+
    | Memory allocated for all YARN     | 30 GB (based on node specs)       |
    | containers on a node              |                                   |
    +-----------------------------------+-----------------------------------+
    | Minimum Container Size (Memory)   | 1 GB (based on node specs)        |
    +-----------------------------------+-----------------------------------+
    | Minimum Container Size (Memory)   | 30 GB (based on node specs)       |
    +-----------------------------------+-----------------------------------+
    :::

2.  Save Changes

    Click the [Save]{.AppCommand style="color: inherit;"} button to save
    your changes. You'll be prompted to optionally add a note such
    as `Updated YARN configuration for Splice Machine`.
    Click [Save]{.AppCommand style="color: inherit;"} again.

3.  Start YARN

    After you save your changes, you'll land back on the [YARN Service
    Configs]{.AppCommand style="color: inherit;"} tab in Ambari.

    Open the [Service Actions]{.AppCommand
    style="color: inherit;"} drop-down in the upper-right corner and
    select the [Start]{.AppCommand style="color: inherit;"} action to
    start YARN. Wait for the restart to complete.
:::

### Configure MapReduce2 {#HortonworksInstall-ConfigureMapReduce2 style="text-align: left;"}

Ambari automatically sets these values for you:

-   Map Memory
-   Reduce Memory
-   Sort Allocation Memory
-   AppMaster Memory
-   MR Map Java Heap Size
-   MR Reduce Java Heap Size

You do, however, need to make a few property changes for this service.

To edit the MapReduce2 configuration, click [MapReduce2]{.AppCommand
style="color: inherit;"} in the Ambari *Services* sidebar. Then click
the [Configs]{.AppCommand style="color: inherit;"} tab and follow these
steps:

::: {.opsStepsList style="text-align: left;"}
1.  You need to replace [\${hdp.version}]{.HighlightedCode
    style="color: inherit;"} with the actual HDP version number you are
    using in these property values:

    ::: {.preWrapperWide}
    ``` {.Example}
    mapreduce.admin.map.child.java.opts
    mapreduce.admin.reduce.child.java.opts
    mapreduce.admin.user.env
    mapreduce.application.classpath
    mapreduce.application.framework.path
    yarn.app.mapreduce.am.admin-command-opts
    MR AppMaster Java Heap Size
    ```
    :::

    An example of an HDP version number that you would substitute
    for[\${hdp.version}]{.AppFontCustCode
    style="color: inherit;"} is `2.5.0.0-1245`.

2.  Click the [Save]{.AppCommand style="color: inherit;"} button to save
    your changes. You'll be prompted to optionally add a note such
    as `Updated MapReduce2 configuration for Splice Machine`.
    Click [Save]{.AppCommand style="color: inherit;"} again.

3.  Select the [Actions]{.AppCommand style="color: inherit;"} drop-down
    in the Ambari *Services* sidebar, then click the [Start]{.AppCommand
    style="color: inherit;"} action to start MapReduce2. Wait for the
    restart to complete.
:::

### Configure and Restart HBASE {#HortonworksInstall-ConfigureandRestartHBASE style="text-align: left;"}

To edit the HBase configuration, click [HBase]{.AppCommand
style="color: inherit;"} in the Ambari *Services* sidebar. Then click
the [Configs]{.AppCommand style="color: inherit;"} tab and follow these
steps:

::: {.opsStepsList style="text-align: left;"}
1.  Change the values of these settings

    ::: {.table-wrap}
    +-----------------------------------+-----------------------------------+
    | Setting                           | New Value                         |
    +:==================================+:==================================+
    | \% of RegionServer Allocated to   | 0.25                              |
    | Write Buffer\                     |                                   |
    | (hbase.regionserver.global.memsto |                                   |
    | re.size)                          |                                   |
    +-----------------------------------+-----------------------------------+
    | HBase RegionServer Maximum        | 24 GB                             |
    | Memory\                           |                                   |
    | (hbase\_regionserver\_heapsize)   |                                   |
    +-----------------------------------+-----------------------------------+
    | \% of RegionServer Allocated to   | 0.25                              |
    | Read Buffers\                     |                                   |
    | (hfile.block.cache.size)          |                                   |
    +-----------------------------------+-----------------------------------+
    | HBase Master Maximum Memory\      | 5 GB                              |
    | (hbase\_master\_heapsize)         |                                   |
    +-----------------------------------+-----------------------------------+
    | Number of Handlers per            | 200                               |
    | RegionServer\                     |                                   |
    | (hbase.regionserver.handler.count |                                   |
    | )                                 |                                   |
    +-----------------------------------+-----------------------------------+
    | HBase RegionServer Meta-Handler   | 200                               |
    | Count                             |                                   |
    +-----------------------------------+-----------------------------------+
    | HBase RPC Timeout                 | 1200000 (20 minutes)              |
    +-----------------------------------+-----------------------------------+
    | Zookeeper Session Timeout         | 120000 (2 minutes)                |
    +-----------------------------------+-----------------------------------+
    | hbase.coprocessor.master.classes  | com.splicemachine.hbase.SpliceMas |
    |                                   | terObserver                       |
    +-----------------------------------+-----------------------------------+
    | hbase.coprocessor.region.classes  | [The value of this property is    |
    |                                   | shown below, in Step 2]{.bodyFont |
    |                                   | style="color: inherit;"}          |
    +-----------------------------------+-----------------------------------+
    | Maximum Store Files before Minor  | 5                                 |
    | Compaction\                       |                                   |
    | (hbase.hstore.compactionThreshold |                                   |
    | )                                 |                                   |
    +-----------------------------------+-----------------------------------+
    | Number of Fetched Rows when       | 1000                              |
    | Scanning from Disk\               |                                   |
    | (hbase.client.scanner.caching)    |                                   |
    +-----------------------------------+-----------------------------------+
    | hstore blocking storefiles\       | 20                                |
    | (hbase.hstore.blockingStoreFiles) |                                   |
    +-----------------------------------+-----------------------------------+
    | Advanced hbase-env                | [The value of this property is    |
    |                                   | shown below, in Step 3]{.bodyFont |
    |                                   | style="color: inherit;"}          |
    +-----------------------------------+-----------------------------------+
    | Custom hbase-site                 | [The value of this is shown       |
    |                                   | below, in Step 4]{.bodyFont       |
    |                                   | style="color: inherit;"}          |
    +-----------------------------------+-----------------------------------+
    :::

2.  Set the value of the `hbase.coprocessor.region.classes` property to
    the following:

    ::: {.preWrapperWide}
    ``` {.Example}
    com.splicemachine.hbase.MemstoreAwareObserver,com.splicemachine.derby.hbase.SpliceIndexObserver,com.splicemachine.derby.hbase.SpliceIndexEndpoint,com.splicemachine.hbase.RegionSizeEndpoint,com.splicemachine.si.data.hbase.coprocessor.TxnLifecycleEndpoint,com.splicemachine.si.data.hbase.coprocessor.SIObserver,com.splicemachine.hbase.BackupEndpointObserver
    ```
    :::

3.  Under `Advanced hbase-env`, set the value of `hbase-env template` to
    the following:

    ::: {.preWrapperWide}
    ``` {.Example}
    # Set environment variables here.

    # The java implementation to use. Java 1.6 required.
    export JAVA_HOME={{java64_home}}

    # HBase Configuration directory
    export HBASE_CONF_DIR=${HBASE_CONF_DIR:-{{hbase_conf_dir}}}

    # Extra Java CLASSPATH elements. Optional.
    export HBASE_CLASSPATH=${HBASE_CLASSPATH}
    # add Splice Machine to the HBase classpath
    SPLICELIBDIR="/opt/splice/default/lib"
    APPENDSTRING=$(echo $(find ${SPLICELIBDIR} -maxdepth 1 -name \*.jar | sort) | sed 's/ /:/g')
    export HBASE_CLASSPATH="${HBASE_CLASSPATH}:${APPENDSTRING}"

    # The maximum amount of heap to use, in MB. Default is 1000.
    # export HBASE_HEAPSIZE=1000

    # Extra Java runtime options.
    # Below are what we set by default. May only work with SUN JVM.
    # For more on why as well as other possible settings,
    # see http://wiki.apache.org/hadoop/PerformanceTuning
    export SERVER_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:{{log_dir}}/gc.log-`date +'%Y%m%d%H%M'`"
    # Uncomment below to enable java garbage collection logging.
    # export HBASE_OPTS="$HBASE_OPTS -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:$HBASE_HOME/logs/gc-hbase.log"

    # Uncomment and adjust to enable JMX exporting
    # See jmxremote.password and jmxremote.access in $JRE_HOME/lib/management to configure remote password access.
    # More details at: http://java.sun.com/javase/6/docs/technotes/guides/management/agent.html
    #
    # export HBASE_JMX_BASE="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
    # If you want to configure BucketCache, specify '-XX: MaxDirectMemorySize=' with proper direct memory size
    # export HBASE_THRIFT_OPTS="$HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10103"
    # export HBASE_ZOOKEEPER_OPTS="$HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10104"

    # File naming hosts on which HRegionServers will run. $HBASE_HOME/conf/regionservers by default.
    export HBASE_REGIONSERVERS=${HBASE_CONF_DIR}/regionservers

    # Extra ssh options. Empty by default.
    # export HBASE_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=HBASE_CONF_DIR"

    # Where log files are stored. $HBASE_HOME/logs by default.
    export HBASE_LOG_DIR={{log_dir}}

    # A string representing this instance of hbase. $USER by default.
    # export HBASE_IDENT_STRING=$USER

    # The scheduling priority for daemon processes. See 'man nice'.
    # export HBASE_NICENESS=10

    # The directory where pid files are stored. /tmp by default.
    export HBASE_PID_DIR={{pid_dir}}

    # Seconds to sleep between slave commands. Unset by default. This
    # can be useful in large clusters, where, e.g., slave rsyncs can
    # otherwise arrive faster than the master can service them.
    # export HBASE_SLAVE_SLEEP=0.1

    # Tell HBase whether it should manage it's own instance of Zookeeper or not.
    export HBASE_MANAGES_ZK=false

    export HBASE_OPTS="${HBASE_OPTS} -XX:ErrorFile={{log_dir}}/hs_err_pid%p.log -Djava.io.tmpdir={{java_io_tmpdir}}"
    ```
    :::

    -   If you're using version 2.2 or later of the Spark Shuffle
        service, set these HBase Master option values:

        ::: {.preWrapperWide}
        export HBASE\_MASTER\_OPTS="\${HBASE\_MASTER\_OPTS}
        -Xms{{master\_heapsize}} -Xmx{{master\_heapsize}}
        \${JDK\_DEPENDED\_OPTS} -XX:+HeapDumpOnOutOfMemoryError
        -XX:MaxDirectMemorySize=2g -XX:+AlwaysPreTouch -XX:+UseParNewGC
        -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70
        -XX:+CMSParallelRemarkEnabled
        -Dcom.sun.management.jmxremote.authenticate=false
        -Dcom.sun.management.jmxremote.ssl=false
        -Dcom.sun.management.jmxremote.port=10101
        -Dsplice.spark.enabled=true
        -[Dsplice.spark.app.name](http://Dsplice.spark.app.name){.external-link}=SpliceMachine
        -Dsplice.spark.master=yarn-client -Dsplice.spark.logConf=true
        -Dsplice.spark.yarn.maxAppAttempts=1
        -Dsplice.spark.driver.maxResultSize=1g
        -Dsplice.spark.driver.cores=2
        -[Dsplice.spark.yarn.am](http://Dsplice.spark.yarn.am){.external-link}.memory=1g
        -Dsplice.spark.dynamicAllocation.enabled=true
        -Dsplice.spark.dynamicAllocation.executorIdleTimeout=120
        -Dsplice.spark.dynamicAllocation.cachedExecutorIdleTimeout=120
        -Dsplice.spark.dynamicAllocation.minExecutors=0
        -Dsplice.spark.dynamicAllocation.maxExecutors=12
        -[Dsplice.spark.io](http://Dsplice.spark.io){.external-link}.compression.lz4.blockSize=32k
        -Dsplice.spark.kryo.referenceTracking=false
        -Dsplice.spark.kryo.registrator=com.splicemachine.derby.impl.SpliceSparkKryoRegistrator
        -Dsplice.spark.kryoserializer.buffer.max=512m
        -Dsplice.spark.kryoserializer.buffer=4m
        -Dsplice.spark.locality.wait=100
        -Dsplice.spark.memory.fraction=0.5
        -Dsplice.spark.scheduler.mode=FAIR
        -Dsplice.spark.serializer=org.apache.spark.serializer.KryoSerializer
        -Dsplice.spark.shuffle.compress=false
        -Dsplice.spark.shuffle.file.buffer=128k
        -Dsplice.spark.shuffle.service.enabled=true
        -Dsplice.spark.reducer.maxReqSizeShuffleToMem=134217728
        -[Dsplice.spark.yarn.am](http://Dsplice.spark.yarn.am){.external-link}.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native
        -[Dsplice.spark.yarn.am](http://Dsplice.spark.yarn.am){.external-link}.waitTime=10s
        -Dsplice.spark.yarn.executor.memoryOverhead=2048
        -Dsplice.spark.driver.extraJavaOptions=-Dlog4j.configuration=[file:/etc/spark/conf/log4j.properties](http://file/etc/spark/conf/log4j.properties){.external-link}
        -Dsplice.spark.driver.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native
        -Dsplice.spark.driver.extraClassPath=/usr/hdp/current/hbase-regionserver/[conf:/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar](http://conf/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar){.external-link}
        -Dsplice.spark.executor.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native
        -Dsplice.spark.executor.extraClassPath=/usr/hdp/current/hbase-regionserver/[conf:/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar](http://conf/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar){.external-link}
        -Dsplice.spark.ui.retainedJobs=100
        -Dsplice.spark.ui.retainedStages=100
        -Dsplice.spark.worker.ui.retainedExecutors=100
        -Dsplice.spark.worker.ui.retainedDrivers=100
        -Dsplice.spark.streaming.ui.retainedBatches=100
        -Dsplice.spark.executor.cores=4
        -Dsplice.spark.executor.memory=8g
        -Dspark.compaction.reserved.slots=4
        -Dsplice.spark.eventLog.enabled=true
        -Dsplice.spark.eventLog.dir=hdfs:///user/splice/history
        -Dsplice.spark.local.dir=/tmp
        -Dsplice.spark.yarn.jars=/opt/splice/default/lib/\*"
        :::

    -   If you're using a version of the Spark Shuffle service earlier
        than 2.2, set these HBase Master option values instead:

        ::: {.preWrapperWide}
        export HBASE\_MASTER\_OPTS="\${HBASE\_MASTER\_OPTS}
        -Xms{{master\_heapsize}} -Xmx{{master\_heapsize}}
        \${JDK\_DEPENDED\_OPTS} -XX:+HeapDumpOnOutOfMemoryError
        -XX:MaxDirectMemorySize=2g -XX:+AlwaysPreTouch -XX:+UseParNewGC
        -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70
        -XX:+CMSParallelRemarkEnabled
        -Dcom.sun.management.jmxremote.authenticate=false
        -Dcom.sun.management.jmxremote.ssl=false
        -Dcom.sun.management.jmxremote.port=10101
        -Dsplice.spark.enabled=true
        -[Dsplice.spark.app.name](http://Dsplice.spark.app.name){.external-link}=SpliceMachine
        -Dsplice.spark.master=yarn-client -Dsplice.spark.logConf=true
        -Dsplice.spark.yarn.maxAppAttempts=1
        -Dsplice.spark.driver.maxResultSize=1g
        -Dsplice.spark.driver.cores=2
        -[Dsplice.spark.yarn.am](http://Dsplice.spark.yarn.am){.external-link}.memory=1g
        -Dsplice.spark.dynamicAllocation.enabled=true
        -Dsplice.spark.dynamicAllocation.executorIdleTimeout=120
        -Dsplice.spark.dynamicAllocation.cachedExecutorIdleTimeout=120
        -Dsplice.spark.dynamicAllocation.minExecutors=0
        -Dsplice.spark.dynamicAllocation.maxExecutors=12
        -[Dsplice.spark.io](http://Dsplice.spark.io){.external-link}.compression.lz4.blockSize=32k
        -Dsplice.spark.kryo.referenceTracking=false
        -Dsplice.spark.kryo.registrator=com.splicemachine.derby.impl.SpliceSparkKryoRegistrator
        -Dsplice.spark.kryoserializer.buffer.max=512m
        -Dsplice.spark.kryoserializer.buffer=4m
        -Dsplice.spark.locality.wait=100
        -Dsplice.spark.memory.fraction=0.5
        -Dsplice.spark.scheduler.mode=FAIR
        -Dsplice.spark.serializer=org.apache.spark.serializer.KryoSerializer
        -Dsplice.spark.shuffle.compress=false
        -Dsplice.spark.shuffle.file.buffer=128k
        -Dsplice.spark.shuffle.service.enabled=true
        -[Dsplice.spark.yarn.am](http://Dsplice.spark.yarn.am){.external-link}.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native
        -[Dsplice.spark.yarn.am](http://Dsplice.spark.yarn.am){.external-link}.waitTime=10s
        -Dsplice.spark.yarn.executor.memoryOverhead=2048
        -Dsplice.spark.driver.extraJavaOptions=-Dlog4j.configuration=[file:/etc/spark/conf/log4j.properties](http://file/etc/spark/conf/log4j.properties){.external-link}
        -Dsplice.spark.driver.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native
        -Dsplice.spark.driver.extraClassPath=/usr/hdp/current/hbase-regionserver/[conf:/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar](http://conf/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar){.external-link}
        -Dsplice.spark.executor.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native
        -Dsplice.spark.executor.extraClassPath=/usr/hdp/current/hbase-regionserver/[conf:/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar](http://conf/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar){.external-link}
        -Dsplice.spark.ui.retainedJobs=100
        -Dsplice.spark.ui.retainedStages=100
        -Dsplice.spark.worker.ui.retainedExecutors=100
        -Dsplice.spark.worker.ui.retainedDrivers=100
        -Dsplice.spark.streaming.ui.retainedBatches=100
        -Dsplice.spark.executor.cores=4
        -Dsplice.spark.executor.memory=8g
        -Dspark.compaction.reserved.slots=4
        -Dsplice.spark.eventLog.enabled=true
        -Dsplice.spark.eventLog.dir=hdfs:///user/splice/history
        -Dsplice.spark.local.dir=/tmp
        -Dsplice.spark.yarn.jars=/opt/splice/default/lib/\*"
        :::

4.  Finish updating of `hbase-env template` with the following:

    ::: {.preWrapperWide}
    ``` {.Example}
    export HBASE_REGIONSERVER_OPTS="${HBASE_REGIONSERVER_OPTS} -Xmn{{regionserver_xmn_size}} -Xms{{regionserver_heapsize}} -Xmx{{regionserver_heapsize}} ${JDK_DEPENDED_OPTS} -XX:+HeapDumpOnOutOfMemoryError -XX:MaxDirectMemorySize=2g -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:MaxNewSize=4g -XX:InitiatingHeapOccupancyPercent=60 -XX:ParallelGCThreads=24 -XX:+ParallelRefProcEnabled -XX:MaxGCPauseMillis=5000 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=10102"
    # HBase off-heap MaxDirectMemorySize
    export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS {% if hbase_max_direct_memory_size %} -XX:MaxDirectMemorySize={{hbase_max_direct_memory_size}}m {% endif %}"
    ```
    :::

5.  In `Custom hbase-site` property, add the following properties:

    ::: {.preWrapperWide}
    ``` {.Example}
    dfs.client.read.shortcircuit.buffer.size=131072
    hbase.balancer.period=60000
    hbase.client.ipc.pool.size=10
    hbase.client.max.perregion.tasks=100
    hbase.coprocessor.regionserver.classes=com.splicemachine.hbase.RegionServerLifecycleObserver
    hbase.hstore.compaction.max.size=260046848
    hbase.hstore.compaction.min.size=16777216
    hbase.hstore.compaction.min=5
    hbase.hstore.defaultengine.compactionpolicy.class=com.splicemachine.compactions.SpliceDefaultCompactionPolicy
    hbase.hstore.defaultengine.compactor.class=com.splicemachine.compactions.SpliceDefaultCompactor
    hbase.htable.threads.max=96
    hbase.ipc.warn.response.size=-1
    hbase.ipc.warn.response.time=-1
    hbase.master.loadbalance.bytable=TRUE
    hbase.mvcc.impl=org.apache.hadoop.hbase.regionserver.SIMultiVersionConsistencyControl
    hbase.regions.slop=0.01
    hbase.regionserver.global.memstore.size.lower.limit=0.9
    hbase.regionserver.lease.period=1200000
    hbase.regionserver.maxlogs=48
    hbase.regionserver.thread.compaction.large=1
    hbase.regionserver.thread.compaction.small=4
    hbase.regionserver.wal.enablecompression=TRUE
    hbase.rowlock.wait.duration=0
    hbase.splitlog.manager.timeout=3000
    hbase.status.multicast.port=16100
    hbase.wal.disruptor.batch=TRUE
    hbase.wal.provider=multiwal
    hbase.wal.regiongrouping.numgroups=16
    hbase.zookeeper.property.tickTime=6000
    hfile.block.bloom.cacheonwrite=TRUE
    io.storefile.bloom.error.rate=0.005
    splice.authentication.native.algorithm=SHA-512
    splice.authentication=NATIVE
    splice.client.numConnections=1
    splice.client.write.maxDependentWrites=60000
    splice.client.write.maxIndependentWrites=60000
    splice.compression=snappy
    splice.marshal.kryoPoolSize=1100
    splice.olap_server.clientWaitTime=900000
    splice.ring.bufferSize=131072
    splice.splitBlockSize=67108864
    splice.timestamp_server.clientWaitTime=120000
    splice.txn.activeTxns.cacheSize=10240
    splice.txn.completedTxns.concurrency=128
    splice.txn.concurrencyLevel=4096
    ```
    :::

6.  Save Changes

    Click the [Save]{.AppCommand style="color: inherit;"} button to save
    your changes. You'll be prompted to optionally add a note such
    as `Updated HDFS configuration for Splice Machine`.
    Click [Save]{.AppCommand style="color: inherit;"} again.

7.  Start HBase

    After you save your changes, you'll land back on the HBase
    Service [Configs]{.AppCommand style="color: inherit;"} tab in
    Ambari.

    Open the [Service Actions]{.AppCommand
    style="color: inherit;"} drop-down in the upper-right corner and
    select the [Start]{.AppCommand style="color: inherit;"} action to
    start HBase. Wait for the restart to complete.
:::

Start any Additional Services {#HortonworksInstall-StartanyAdditionalServices style="text-align: left;"}
-----------------------------

We started this installation by shutting down your cluster services, and
then configured and restarted each individual service used by Splice
Machine.

If you had any additional services running, such as Ambari Metrics, you
need to restart each of those services.

Optional Configuration Modifications {#HortonworksInstall-OptionalConfigurationModifications style="text-align: left;"}
------------------------------------

There are a few configuration modifications you might want to make:

-   [Modify the Authentication
    Mechanism](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Modify){.external-link} if
    you want to authenticate users with something other than the
    default *native authentication*mechanism.
-   [Modify the Log
    Location](http://docstest.splicemachine.com/onprem_install_hortonworks.html#Logging){.external-link} if
    you want your Splice Machine log entries stored somewhere other than
    in the logs for your region servers.
-   Adjust the replication factor if you have a small cluster and need
    to improve resource usage or performance.

### Modify the Authentication Mechanism {#HortonworksInstall-ModifytheAuthenticationMechanism style="text-align: left;"}

Splice Machine installs with Native authentication configured; native
authentication uses the `sys.sysusers` table in the `splice` schema for
configuring user names and passwords.

You can disable authentication or change the authentication mechanism
that Splice Machine uses to LDAP by following the simple instructions
in [Configuring Splice Machine
Authentication](http://docstest.splicemachine.com/onprem_install_configureauth.html){.external-link}

If you're using Kerberos, you need to add this option to your HBase
Master Java Configuration Options:

::: {.preWrapper style="text-align: left;"}
``` {.Example}
-Dsplice.spark.hadoop.fs.hdfs.impl.disable.cache=true
```
:::

### Modify the Log Location {#HortonworksInstall-ModifytheLogLocation style="text-align: left;"}

Splice Machine logs all SQL statements by default, storing the log
entries in your region server's logs, as described in our [Using
Logging](http://docstest.splicemachine.com/developers_tuning_logging){.external-link} topic.
You can modify where Splice Machine stores logs by adding the following
snippet to your *RegionServer Logging Advanced Configuration Snippet
(Safety Valve)* section of your HBase Configuration:

::: {.preWrapper style="text-align: left;"}
``` {.Example}
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

Verify your Splice Machine Installation {#HortonworksInstall-VerifyyourSpliceMachineInstallation style="text-align: left;"}
---------------------------------------

Now start using the Splice Machine command line interpreter, which is
referred to as [the splice prompt]{.AppCommand
style="color: inherit;"} or simply [splice\>]{.AppCommand
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

::: {#footer role="contentinfo"}
::: {.section .footer-body}
Document generated by Confluence on Feb 21, 2018 15:36

::: {#footer-logo}
[Atlassian](http://www.atlassian.com/)
:::
:::
:::
