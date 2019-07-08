# Installing and Configuring Splice Machine for Hortonworks HDP

This topic describes installing and configuring Splice Machine on a
Hortonworks Ambari-managed cluster. Follow these steps:

1. [Verify Prerequisites](#verify-prerequisites)
2. [Download and Install Splice Machine](#download-and-install-splice-machine)
3. [Stop Hadoop Services](#stop-hadoop-services)
4. [Configure Hadoop Services](#configure-hadoop-services)
5. [Start Any Additional Services](#start-any-additional-services)
6. Make any needed [Optional Configuration Modifications](#optional-configuration-modifications)
7. [Verify your Splice Machine Installation](#verify-your-splice-machine-installation)

## Verify Prerequisites

Before starting your Splice Machine installation, please make sure that
your cluster contains the prerequisite software components:

* A cluster running HDP
* Ambari installed and configured for HDP
* HBase installed
* HDFS installed
* YARN installed
* ZooKeeper installed
* Ensure that Phoenix services are **NOT** installed on your cluster, as
  they interfere with Splice Machine HBase settings.

**NOTE:** The specific versions of these components that you need depend on your
operating environment, and are called out in detail in the
[Requirements](https://doc.splicemachine.com/onprem_info_requirements.html) topic of our *Getting Started Guide*.

## Download and Install Splice Machine

Perform the following step **on each node** in your cluster:

1.  install this splicemachine rpms using following command (take version 2.5.0.1827 for example) :

    ````
    sudo rpm -ivh splicemachine-hdp2.6.1.2.5.0.1827.p0.803-1.noarch.rpm


## Stop Hadoop Services

As a first step, we stop cluster services to allow our installer to make
changes that require the cluster to be temporarily inactive.

1. Access the Ambari Home Screen

2. Click the `Actions` drop-down in the Ambari *Services* sidebar, and
    then click the `Stop All` button.

## Configure Hadoop Services

Now it's time to make a few modifications in the Hadoop services
configurations:

* [Configure and Restart ZooKeeper](#configure-and-restart-zookeeper)
* [Configure and Restart HDFS](#configure-and-restart-hdfs)
* [Configure and Restart YARN](#configure-and-restart-yarn)
* [Configure MapReduce2](#configure-mapreduce2)
* [Configure and Restart HBASE](#configure-and-restart-hbase)

### Configure and Restart ZooKeeper

To edit the ZooKeeper configuration, click `ZooKeeper` in the Ambari *Services* sidebar. Then click the `Configs` tab and follow these steps:

1. Click the *Custom zoo.cfg* drop-down arrow, then click `Add Property` to add the `maxClientCnxns` property and then again to add the `maxSessionTimeout` property,
with these values:

   ````
   maxClientCnxns=0
   maxSessionTimeout=120000
   ````

2. Click the `Save` button to save your
changes. You'll be prompted to optionally add a note such as
`Updated ZooKeeper configuration for Splice Machine`. Click `Save` again.

3. Select the `Actions` drop-down
in the Ambari *Services* sidebar, then click the `Start` action to start ZooKeeper. Wait for the restart to complete.

### Configure and Restart HDFS

To edit the HDFS configuration, click `HDFS` in the Ambari *Services* sidebar. Then click the `Configs` tab and follow these steps:

1. Edit the HDFS configuration as follows:

   <table>
        <col />
        <col />
        <tbody>
            <tr>
                <td><code>NameNode Java heap size</code></td>
                <td><code>4 GB</code></td>
            </tr>
            <tr>
                <td><code>DataNode maximum Java heap size</code></td>
                <td><code>2 GB</code></td>
            </tr>
            <tr>
                <td><code>Block replication</code></td>
                <td><p>`2` (for clusters with less than 8 nodes)</p>
                    <p>`3` (for clusters with 8 or more nodes)</td>
            </tr>
        </tbody>
   </table>

2. Add a new property:

   Click `Add Property...` under `Custom hdfs-site`, and add the following property:

   ````
   dfs.datanode.handler.count=20
   ````

3. Save Changes

   Click the `Save` button to save your
   changes. You'll be prompted to optionally add a note such as
   `Updated HDFS configuration for Splice Machine`. Click `Save` again.

4. Start HDFS

   After you save your changes, you'll land back on the `HDFS Service Configs` tab in Ambari.

   Click the `Actions` drop-down in the Ambari *Services* sidebar, then click the `Start` action to start HDFS. Wait for the restart to complete.

5. Create directories for hbase user and the Splice Machine YARN application:

   Use your terminal window to create these directories:

   ````
   sudo -iu hdfs hadoop fs -mkdir -p hdfs:///user/hbase hdfs:///user/splice/history
   sudo -iu hdfs hadoop fs -chown -R hbase:hbase hdfs:///user/hbase hdfs:///user/splice
   sudo -iu hdfs hadoop fs -chmod 1777 hdfs:///user/splice hdfs:///user/splice/history
   ````

### Configure and Restart YARN

To edit the YARN configuration, click `YARN` in the Ambari *Services* sidebar. Then click the `Configs` tab and follow these steps:

1. Update these other configuration values:

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
            <td><code>yarn.application.classpath </code></td>
            <td><code>/etc/hadoop/conf,/usr/hdp/current/hadoop-client/*,/usr/hdp/current/hadoop-client/lib/*,/usr/hdp/current/hadoop-hdfs-client/*,/usr/hdp/current/hadoop-hdfs-client/lib/*,/usr/hdp/current/hadoop-yarn-client/*,/usr/hdp/current/hadoop-yarn-client/lib/*</code></td>
        </tr>
        <tr>
            <td><code>yarn.nodemanager.aux-services.spark2_shuffle.classpath</code></td>
            <td><code>{{stack_root}}/${hdp.version}/spark2/aux/*</code></td>
        </tr>
        <tr>
            <td><code>yarn.nodemanager.aux-services.spark_shuffle.classpath</code></td>
            <td><code>{{stack_root}}/${hdp.version}/spark/aux/*</code></td>
        </tr>
        <tr>
            <td><code>yarn.nodemanager.aux-services.spark2_shuffle.class</code></td>
            <td><code>org.apache.spark.network.yarn.YarnShuffleService</code></td>
        </tr>
        <tr>
            <td><code>yarn.nodemanager.delete.debug-delay-sec</code></td>
            <td><code>86400</code></td>
        </tr>
        <tr>
            <td><code>Memory allocated for all YARN containers on a node </code></td>
            <td><code>30 GB (based on node specs)</code></td>
        </tr>
        <tr>
            <td><code>Minimum Container Size (Memory)</code></td>
            <td><code>1 GB (based on node specs)</code></td>
        </tr>
        <tr>
            <td><code>Minimum Container Size (Memory)</code></td>
            <td><code>30 GB (based on node specs)</code></td>
        </tr>
    </tbody>
   </table>

2. Save Changes

   Click the `Save` button to save your changes. You'll be prompted to optionally add a note such as `Updated YARN configuration for Splice Machine`. Click `Save` again.

3. Start YARN

   After you save your changes, you'll land back on the `YARN Service Configs` tab in Ambari.

   Open the `Service Actions` drop-down in the upper-right corner and select the `Start` action to start YARN. Wait for the restart to complete.

### Configure MapReduce2

Ambari automatically sets these values for you:

* Map Memory
* Reduce Memory
* Sort Allocation Memory
* AppMaster Memory
* MR Map Java Heap Size
* MR Reduce Java Heap Size

You do, however, need to make a few property changes for this service.

To edit the MapReduce2 configuration, click `MapReduce2` in the Ambari *Services* sidebar. Then click the `Configs` tab and follow these steps:

1. You need to replace `${hdp.version}` with the actual HDP
version number you are using in these property values:

   ````
   mapreduce.admin.map.child.java.opts
   mapreduce.admin.reduce.child.java.opts
   mapreduce.admin.user.env
   mapreduce.application.classpath
   mapreduce.application.framework.path
   yarn.app.mapreduce.am.admin-command-opts
   MR AppMaster Java Heap Size
   ````

   **NOTE:** An example of an HDP version number that you would substitute for `${hdp.version}` is `2.5.0.0-1245`.

2. Click the `Save` button to save your
changes. You'll be prompted to optionally add a note such as
`Updated MapReduce2 configuration for Splice Machine`. Click `Save` again.

3. Select the `Actions` drop-down in the Ambari *Services* sidebar, then click the `Start` action to start MapReduce2. Wait for the restart to complete.

### Configure and Restart HBASE
````
important:remove property hbase.bucketcache.ioengine or it will prevent splicemachine from starting.
use 'sudo /var/lib/ambari-server/resources/scripts/configs.sh -u $userId -p $password delete $AMABRI_HOST $CLUSTER_NAME hbase-site hbase.bucketcache.ioengine' on the ambari server node to remove the property
````


To edit the HBase configuration, click `HBase` in the Ambari *Services* sidebar. Then click the `Configs` tab and follow these steps:

1. Change the values of these settings

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
            <td><code>% of RegionServer Allocated to Write Buffer<br />(hbase.regionserver.global.memstore.size)</code></td>
            <td><code>0.25</code></td>
        </tr>
        <tr>
            <td><code>HBase RegionServer Maximum Memory<br /> (hbase_regionserver_heapsize)</code></td>
            <td><code>24 GB</code></td>
        </tr>
        <tr>
            <td><code>% of RegionServer Allocated to Read Buffers<br /> (hfile.block.cache.size)</code></td>
            <td><code>0.25</code></td>
        </tr>
        <tr>
            <td><code>HBase Master Maximum Memory<br /> (hbase_master_heapsize)</code></td>
            <td><code>5 GB</code></td>
        </tr>
        <tr>
            <td><code>Number of Handlers per RegionServer<br /> (hbase.regionserver.handler.count)</code></td>
            <td><code>200</code></td>
        </tr>
        <tr>
            <td><code>HBase RegionServer Meta-Handler Count</code></td>
            <td><code>200</code></td>
        </tr>
        <tr>
            <td><code>HBase RPC Timeout</code></td>
            <td><code>1200000 (20 minutes)</code></td>
        </tr>
        <tr>
            <td><code>Zookeeper Session Timeout</code></td>
            <td><code>120000 (2 minutes)</code></td>
        </tr>
        <tr>
            <td><code>hbase.coprocessor.master.classes</code></td>
            <td><code>com.splicemachine.hbase.SpliceMasterObserver</code></td>
        </tr>
        <tr>
            <td><code>hbase.coprocessor.region.classes</code></td>
            <td>The value of this property is shown below, in Step 2</td>
        </tr>
        <tr>
            <td><code>Maximum Store Files before Minor Compaction<br /> (hbase.hstore.compactionThreshold)<br /></code></td>
            <td><code>5</code></td>
        </tr>
        <tr>
            <td><code>Number of Fetched Rows when Scanning from Disk<br /> (hbase.client.scanner.caching)<br /></code></td>
            <td><code>1000</code></td>
        </tr>
        <tr>
            <td><code>hstore blocking storefiles<br /> (hbase.hstore.blockingStoreFiles)</code></td>
            <td><code>20</code></td>
        </tr>
        <tr>
            <td><code>Advanced hbase-env</code></td>
            <td>The value of this property is shown below, in Step 3</span>
            </td>
        </tr>
        <tr>
            <td><code>Custom hbase-site</code></td>
            <td>The value of this is shown below, in Step 4</td>
        </tr>
    </tbody>
   </table>

2. Set the value of the `hbase.coprocessor.region.classes` property to the following:

   ````
   com.splicemachine.hbase.MemstoreAwareObserver,com.splicemachine.derby.hbase.SpliceIndexObserver,com.splicemachine.derby.hbase.SpliceIndexEndpoint,com.splicemachine.hbase.RegionSizeEndpoint,com.splicemachine.si.data.hbase.coprocessor.TxnLifecycleEndpoint,com.splicemachine.si.data.hbase.coprocessor.SIObserver,com.splicemachine.hbase.BackupEndpointObserver
   ````

3. Under `Advanced hbase-env`, set the value of `hbase-env template` to the following:

   ````   
   # Set environment variables here.

	# The java implementation to use. Java 1.6 required.
	export JAVA_HOME={{java64_home}}

	# HBase Configuration directory
	export HBASE_CONF_DIR=${HBASE_CONF_DIR:-{{hbase_conf_dir}}}

	# Extra Java CLASSPATH elements. Optional.
	export HBASE_CLASSPATH=${HBASE_CLASSPATH}:{{stack_root}}/current/ext/hbase/*


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

	{% if java_version < 8 %}
	JDK_DEPENDED_OPTS="-XX:PermSize=128m -XX:MaxPermSize=128m"
	{% endif %}

	{% if security_enabled %}
	export HBASE_OPTS="$HBASE_OPTS -XX:+UseConcMarkSweepGC -XX:ErrorFile={{log_dir}}/hs_err_pid%p.log -Djava.security.auth.login.config={{client_jaas_config_file}} -Djava.io.tmpdir={{java_io_tmpdir}}"
	export HBASE_MASTER_OPTS="$HBASE_MASTER_OPTS -Xmx{{master_heapsize}} -Djava.security.auth.login.config={{master_jaas_config_file}} -Djavax.security.auth.useSubjectCredsOnly=false $JDK_DEPENDED_OPTS"
	export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS -Xmn{{regionserver_xmn_size}} -XX:CMSInitiatingOccupancyFraction=50 -XX:+UseCMSInitiatingOccupancyOnly -Xms{{regionserver_heapsize}} -Xmx{{regionserver_heapsize}} -Djava.security.auth.login.config={{regionserver_jaas_config_file}} -Djavax.security.auth.useSubjectCredsOnly=false $JDK_DEPENDED_OPTS"
	export PHOENIX_QUERYSERVER_OPTS="$PHOENIX_QUERYSERVER_OPTS -Djava.security.auth.login.config={{queryserver_jaas_config_file}}"
	{% else %}
	export HBASE_OPTS="$HBASE_OPTS -XX:+UseConcMarkSweepGC -XX:ErrorFile={{log_dir}}/hs_err_pid%p.log -Djava.io.tmpdir={{java_io_tmpdir}}"
	export HBASE_MASTER_OPTS="$HBASE_MASTER_OPTS -Xmx{{master_heapsize}} $JDK_DEPENDED_OPTS"
	export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS -Xmn{{regionserver_xmn_size}} -XX:CMSInitiatingOccupancyFraction=50 -XX:+UseCMSInitiatingOccupancyOnly -Xms{{regionserver_heapsize}} -Xmx{{regionserver_heapsize}} $JDK_DEPENDED_OPTS"
	{% endif %}

	# HBase off-heap MaxDirectMemorySize
	export HBASE_REGIONSERVER_OPTS="$HBASE_REGIONSERVER_OPTS {% if hbase_max_direct_memory_size %} -XX:MaxDirectMemorySize={{hbase_max_direct_memory_size}}m {% endif %}"
	export HBASE_MASTER_OPTS="$HBASE_MASTER_OPTS {% if hbase_max_direct_memory_size %} -XX:MaxDirectMemorySize={{hbase_max_direct_memory_size}}m {% endif %}"

	#Add Splice Jars to HBASE_PREFIX_CLASSPATH
	if [ ! "$1" = "shell" ]; then
    export HBASE_CLASSPATH_PREFIX=/var/lib/splicemachine/*:/usr/hdp/2.6.1.0-129/spark2/jars/*:/usr/hdp/2.6.1.0-129/phoenix/lib/phoenix-core-4.7.0.2.6.1.0-129.jar
	fi


	#Add Splice Specific Information to HBase Master
	export HBASE_MASTER_OPTS="${HBASE_MASTER_OPTS} -Dsplice.spark.enabled=true -Dsplice.spark.app.name=SpliceMachine -Dsplice.spark.master=yarn -Dsplice.spark.submit.deployMode=client -Dsplice.spark.logConf=true -Dsplice.spark.yarn.maxAppAttempts=1 -Dsplice.spark.driver.maxResultSize=1g -Dsplice.spark.driver.cores=2 -Dsplice.spark.yarn.am.memory=1g -Dsplice.spark.dynamicAllocation.enabled=true -Dsplice.spark.dynamicAllocation.executorIdleTimeout=120 -Dsplice.spark.dynamicAllocation.cachedExecutorIdleTimeout=120 -Dsplice.spark.dynamicAllocation.minExecutors=0 -Dsplice.spark.kryo.referenceTracking=false -Dsplice.spark.kryo.registrator=com.splicemachine.derby.impl.SpliceSparkKryoRegistrator -Dsplice.spark.kryoserializer.buffer.max=512m -Dsplice.spark.kryoserializer.buffer=4m -Dsplice.spark.locality.wait=0 -Dsplice.spark.memory.fraction=0.5 -Dsplice.spark.scheduler.mode=FAIR -Dsplice.spark.serializer=org.apache.spark.serializer.KryoSerializer -Dsplice.spark.shuffle.compress=false -Dsplice.spark.shuffle.file.buffer=128k -Dsplice.spark.shuffle.service.enabled=true -Dsplice.spark.reducer.maxReqSizeShuffleToMem=134217728 -Dsplice.spark.yarn.am.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native -Dsplice.spark.yarn.am.waitTime=10s -Dsplice.spark.yarn.executor.memoryOverhead=2048 -Dsplice.spark.yarn.am.extraJavaOptions=-Dhdp.version=2.6.1.0-129 -Dsplice.spark.driver.extraJavaOptions=-Dhdp.version=2.6.1.0-129 -Dsplice.spark.driver.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native -Dsplice.spark.driver.extraClassPath=/usr/hdp/current/hbase-regionserver/conf:/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar -Dsplice.spark.ui.retainedJobs=100 -Dsplice.spark.ui.retainedStages=100 -Dsplice.spark.worker.ui.retainedExecutors=100 -Dsplice.spark.worker.ui.retainedDrivers=100 -Dsplice.spark.streaming.ui.retainedBatches=100 -Dsplice.spark.executor.cores=2 -Dsplice.spark.executor.memory=2g -Dspark.compaction.reserved.slots=4 -Dsplice.spark.eventLog.enabled=true -Dsplice.spark.eventLog.dir=hdfs:///user/splice/history -Dsplice.spark.local.dir=/tmp -Dsplice.spark.executor.userClassPathFirst=true -Dsplice.spark.driver.userClassPathFirst=true -Dsplice.spark.executor.extraJavaOptions=-Dhdp.version=2.6.1.0-129 -Dsplice.spark.executor.extraLibraryPath=/usr/hdp/current/hadoop-client/lib/native -Dsplice.spark.executor.extraClassPath=/usr/hdp/current/hbase-regionserver/conf:/usr/hdp/current/hbase-regionserver/lib/htrace-core-3.1.0-incubating.jar:/var/lib/splicemachine/*:/usr/hdp/2.6.1.0-129/spark2/jars/*:/usr/hdp/current/hbase-master/lib/*:/usr/hdp/2.6.1.0-129/hadoop-mapreduce/* -Dsplice.spark.yarn.jars=/usr/hdp/2.6.1.0-129/spark2/jars/*"

	#Add Splice Specific Information to Region Server
	export HBASE_REGIONSERVER_OPTS="${HBASE_REGIONSERVER_OPTS} -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=10102"


   ````

4. In `Custom hbase-site` property, add the following properties:

   ````
   dfs.client.read.shortcircuit.buffer.size=131072
   hbase.balancer.period=60000
   hbase.client.ipc.pool.size=10
   hbase.client.max.perregion.tasks=100
   hbase.coprocessor.regionserver.classes=com.splicemachine.hbase.RegionServerLifecycleObserver
   hbase.hstore.compaction.min.size=136314880
   hbase.hstore.compaction.min=3
   hbase.hstore.defaultengine.compactionpolicy.class=com.splicemachine.compactions.SpliceDefaultCompactionPolicy
   hbase.hstore.defaultengine.compactor.class=com.splicemachine.compactions.SpliceDefaultCompactor
   hbase.htable.threads.max=96
   hbase.ipc.warn.response.size=-1
   hbase.ipc.warn.response.time=-1
   hbase.master.loadbalance.bytable=TRUE
   hbase.master.balancer.stochastic.regionCountCost=1500
   hbase.mvcc.impl=org.apache.hadoop.hbase.regionserver.SIMultiVersionConsistencyControl
   hbase.regions.slop=0
   hbase.regionserver.global.memstore.size.lower.limit=0.9
   hbase.regionserver.lease.period=1200000
   hbase.regionserver.maxlogs=48
   hbase.regionserver.thread.compaction.large=4
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
   hbase.master.hfilecleaner.plugins=com.splicemachine.hbase.SpliceHFileCleaner,org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner   
   ````
   
5. Save Changes

   Click the `Save` button to save your changes. You'll be prompted to optionally add a note such as `Updated HDFS configuration for Splice Machine`. Click `Save` again.   
  
   
6. Start HBase

   After you save your changes, you'll land back on the HBase Service `Configs` tab in Ambari.

   Open the `Service Actions` drop-down in the upper-right corner and select the `Start` action to start HBase. Wait for the restart to complete.

## Start any Additional Services

We started this installation by shutting down your cluster services, and
then configured and restarted each individual service used by Splice
Machine.

If you had any additional services running, such as Ambari Metrics, you
need to restart each of those services.

## Optional Configuration Modifications

There are a few configuration modifications you might want to make:

* [Modify the Authentication Mechanism](#modify-the-authentication-mechanism) if you want to
  authenticate users with something other than the default *native
  authentication* mechanism.
* [Modify the Log Location](#modify-the-log-location) if you want your Splice Machine
  log entries stored somewhere other than in the logs for your region
  servers.

### Modify the Authentication Mechanism

Splice Machine installs with Native authentication configured; native
authentication uses the `sys.sysusers` table in the `splice` schema for
configuring user names and passwords.

You can disable authentication or change the authentication mechanism
that Splice Machine uses to LDAP by following the simple instructions in
[Configuring Splice Machine
Authentication](https://doc.splicemachine.com/onprem_install_configureauth.html){: .WithinBook}

If you're using Kerberos, you need to add this option to your HBase Master Java Configuration Options:

   ````
   -Dsplice.spark.hadoop.fs.hdfs.impl.disable.cache=true
   ````

### Modify the Log Location

Splice Machine logs all SQL statements by default, storing the log
entries in your region server's logs, as described in our [Using
Logging](developers_tuning_logging) topic. You can modify where Splice
Machine stores logs by adding the following snippet to your *hbase-log4j.template* section of your HBase
Configuration:

   ````
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
   ````

## Verify your Splice Machine Installation

Now start using the Splice Machine command line interpreter, which is
referred to as `the splice prompt` or simply `splice&gt;` by launching the
`sqlshell.sh` script on any node in your cluster that is running an HBase region server.

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
            <td>Exit the command line interpreter</td>
            <td><code>splice&gt; exit;</code></td>
        </tr>
        <tr>
            <td colspan="2"><strong>Make sure you end each command with a semicolon</strong> (<code>;</code>), followed by the <em>Enter</em> key or <em>Return</em> key </td>
        </tr>
    </tbody>
   </table>

See the [Command Line (splice&gt;)  Reference](https://doc.splicemachine.com/cmdlineref_intro.html)
section of our *Developer's Guide* for information about our commands
and command syntax.
