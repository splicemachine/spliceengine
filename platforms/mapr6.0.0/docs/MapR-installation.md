
# Installing and Configuring Splice Machine for MapR

This topic describes installing and configuring Splice Machine on a
MapR-managed cluster. Follow these steps:

1. [Verify Prerequisites](#verify-prerequisites)
2. [Download and Install Splice Machine](#download-and-install-splice-machine)
3. [Configure Cluster Services](#configure-cluster-services)
4. [Stop Cluster Services](#stop-cluster-services)
5. [Restart Cluster Services](#restart-cluster-services)
6. [Create the Splice Machine Event Log Directory](#create-the-splice-machine-event-log-directory)
7. Make any needed [Optional Configuration Modifications](#optional-configuration modifications)
8. [Verify your Splice Machine Installation](#verify-your-Splice-Machine-installation)

**NOTE:** MapR Secure Clusters Only Work with the Enterprise Edition of Splice Machine;
MapR secure clusters do not support the Community Edition of Splice Machine. You can check this by:

1. Open the HBase Configuration page
2. Search the XML for the `hbase.security.authentication` setting
3. If the setting value is anything other than `simple`, you need the
   *Enterprise* edition of Splice Machine.

To read more about the additional features available in the Enterprise
Edition, see our [Splice Machine Editions](https://doc.splicemachine.com/notes_editions.html) page. To obtain a license for the Splice Machine *Enterprise Edition*, please [Contact Splice Machine Sales](http://www.splicemachine.com/company/contact-us/) today.

## Verify Prerequisites

Before starting your Splice Machine installation, please make sure that
your cluster contains the prerequisite software components:

* A cluster running MapR with MapR-FS
* HBase installed
* YARN installed
* ZooKeeper installed
* The latest `mapr-patch` should be installed to bring in all
  MapR-supplied platform patches, before proceeding with your Splice
  Machine installation.
* The MapR Ecosystem Package (MEP) should be installed on all cluster
  nodes, before proceeding with your Splice Machine installation.
* Ensure Spark services are **NOT** installed; Splice Machine cannot
  currently coexist with Spark 1.x on a cluster:
  * If `MEP version 1.x` is in use, you must remove the `Spark 1.x`
    packages from your cluster, as described below, in [Removing Spark
    Packages from Your Cluster.](#Removing)
  * MEP version 2.0 bundles Spark 2.0, and will not cause conflicts with
    Splice Machine.

## Download and Install Splice Machine

Perform the following steps **on each node**
in your cluster:

1. Download the installer for your version.

2. Get the four rpms into a directory on all nodes in the cluster that you'll be running splicemachine:

 ````
splicemachine-mapr6.0.0-hbase-bin-2.8.0.1904.p0.1_1.noarch.rpm
splicemachine-mapr6.0.0-hbase-conf-2.8.0.1904.p0.1_1.noarch.rpm
splicemachine-mapr6.0.0-region-2.8.0.1904.p0.1_1.noarch.rpm
splicemachine-mapr6.0.0-master-2.8.0.1904.p0.1_1.noarch.rpm
 ````
You can replace secure with hbase-secure-conf if you want it unsecure

3. On the splice master node:
 ````
sudo rpm -ivhp splicemachine-mapr6.0.0-hbase-bin-2.8.0.1904.p0.1_1.noarch.rpm splicemachine-mapr6.0.0-hbase-conf-2.8.0.1904.p0.1_1.noarch.rpm
splicemachine-mapr6.0.0-master-2.8.0.1904.p0.1_1.noarch.rpm
 ````

4. On all of the remaining nodes, to run a regionserver:
 ````
 sudo rpm -ivhp splicemachine-mapr6.0.0-hbase-bin-2.8.0.1904.p0.1_1.noarch.rpm splicemachine-mapr6.0.0-hbase-conf-2.8.0.1904.p0.1_1.noarch.rpm
 splicemachine-mapr6.0.0-region-2.8.0.1904.p0.1_1.noarch.rpm
 ````

5. Make one config file edit: update zookeeper quorum, on all machines:
   ````
   sudo vi /opt/mapr/hbase/hbase1.1.8-splice/conf/hbase-site.xml
   ````
replace `###ZK_QUORUM_LIST###` with your quorum, e.g. a single hostname
  `ip-10-11-128-26.ec2.internal`
or a comma separated list
  `ip-10-11-128-26.ec2.internal,ip-10-11-128-27.ec2.internal`


6. Update Yarn configs to include spark:

 on all machines running yarn:
   ````
   sudo vi /opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop/yarn-site.xml
   ````

   add in the following lines before the final closing 
    ````
   </configuration> tag

    <!-- yarn resource settings are installation specific -->
     <property><name>yarn.scheduler.minimum-allocation-mb</name><value>1024</value></property>
     <property><name>yarn.scheduler.increment-allocation-mb</name><value>512</value></property>
     <property><name>yarn.scheduler.maximum-allocation-mb</name><value>30720</value></property>
     <property><name>yarn.nodemanager.resource.memory-mb</name><value>30720</value></property>

     <property><name>yarn.scheduler.minimum-allocation-vcores</name><value>1</value></property>
     <property><name>yarn.scheduler.increment-allocation-vcores</name><value>1</value></property>
     <property><name>yarn.scheduler.maximum-allocation-vcores</name><value>19</value></property>
     <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>19</value></property>
     <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle,mapr_direct_shuffle,spark_shuffle</value></property>
     <property><name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name><value>org.apache.hadoop.mapred.ShuffleHandler</value></property>
     <property><name>yarn.nodemanager.aux-services.spark_shuffle.class</name><value>org.apache.spark.network.yarn.YarnShuffleService</value></property>
    ````

7. copy the spark yarn-shuffle jar into the yarn library so it gets onto the classpath:
  
 ````
sudo cp /opt/mapr/spark/spark-2.3.2/yarn/spark-2.3.2-mapr-1901-yarn-shuffle.jar /opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/yarn/lib/
 ````

8. Make some folders that splice needs in the maprfs:

 ````
sudo -iu mapr hadoop fs -mkdir -p maprfs:///splice-hbase
sudo -iu mapr hadoop fs -mkdir -p maprfs:///user/splice/history
sudo -iu mapr hadoop fs -chown -R mapr:mapr maprfs:///user/splice/history
sudo -iu mapr hadoop fs -chmod 777 maprfs:///user/splice/history
 ````

9. Restart all mapr components (only nodemanager/resourcemanager is strictly needed, but this is the easiest way):
`sudo service mapr-warden restart`

10. Finally, to start splice, on the appropriate machine(s):
 ````
sudo service splice-masterserver start
sudo service splice-regionserver start

 ````
      
#### OLAP Server Log

Splice Machine uses log4j to config OLAP server's log.  If you want to change the default log behavior of OLAP server,
config `splice.olap.log4j.configuration` in `hbase-site.xml`. It specifies the log4j.properties file you want to use.
This file needs to be available on HBase master server.

#### Security Audit log

Splice Machine records security related actions (e.g. CREATE / DROP USER, MODIFY PASSWORD, LOGIN) in audit log. You can modify where Splice
Machine stores audit log by adding the following snippet to your *RegionServer Logging
Advanced Configuration Snippet (Safety Valve)* section of your HBase
Configuration:

   ```
    log4j.appender.spliceAudit=org.apache.log4j.FileAppender
    log4j.appender.spliceAudit.File=${hbase.log.dir}/splice-audit.log
    log4j.appender.spliceAudit.layout=org.apache.log4j.PatternLayout
    log4j.appender.spliceAudit.layout.ConversionPattern=%d{ISO8601} %m%n
    
    log4j.logger.splice-audit=INFO, spliceAudit
    log4j.additivity.splice-audit=false
   ```

### Adjust the Replication Factor

The default namespace replication factor for Splice Machine is <code>3</code>. If you're
running a small cluster you may want to adjust the replication down to
improve resource and performance drag. To do so in MapR, use the
following command:

   ````
   maprcli volume modify -nsreplication <value>
   ````

For more information, see the MapR documentation site,
[doc.mapr.com](http://doc.mapr.com/)

## Verify your Splice Machine Installation

Now start using the Splice Machine command line interpreter, which is
referred to as *the splice prompt* or simply <code>splice&gt;</code> by launching the `sqlshell.sh`
script on any node in your cluster that is running an HBase region
server.

**NOTE:** The command line interpreter defaults to connecting on port `1527` on
`localhost`, with username `splice`, and password `admin`. You can
override these defaults when starting the interpreter, as described in
the [Command Line (splice&gt;) Reference](https://doc.splicemachine.com/cmdlineref_intro.html) topic
in our *Developer's Guide*.

Now try entering a few sample commands you can run to verify that
everything is working with your Splice Machine installation.

   <table summary="Sample commands to verify your installation">
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
