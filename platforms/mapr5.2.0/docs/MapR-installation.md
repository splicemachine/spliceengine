
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

The specific versions of these components that you need depend on your
operating environment, and are called out in detail in the
[Requirements](https://doc.splicemachine.com/onprem_info_requirements.html) topic of our *Getting Started Guide*.

### Removing Spark Packages from Your Cluster

To remove Spark packages from your cluster and update your
configuration, follow these steps

1. If you're running a Debian/Ubuntu-based Linux distribution:

   ````
   dpkg -l | awk '/ii.*mapr-spark/{print $2}' | xargs sudo apt-get purge
   ````

   If you're running on a RHEL/CentOS-based Linux distribution:

   ````
   rpm -qa \*mapr-spark\* | xargs sudo yum -y erase
   ````

2. Reconfigure node services in your cluster:

   ````
   sudo /opt/mapr/server/configure.sh -R
   ````

## Download and Install Splice Machine

Perform the following steps **on each node**
in your cluster:

1. Download the installer for your version.

   Which Splice Machine installer (gzip) package you need depends upon which Splice Machine version you're installing and which version of MapR you are using. Here are the URLs for Splice Machine Releases 2.7 and 2.5:

   <table>
    <col />
    <col />
    <col />
    <thead>
        <tr>
            <th>Splice Machine Version</th>
            <th>MapR Version</th>
            <th>Installer Package Link</th>
        </tr>
    </thead>
    <tbody>
        <tr>
           <td><strong>2.7</strong></td>
           <td><strong>2.5.0</strong></td>
           <td><a href="https://s3.amazonaws.com/splice-releases/2.6.1.1745/cluster/installer/mapr5.2.0/SPLICEMACHINE-2.6.1.1745.mapr5.2.0.p0.121.tar.gz">https://s3.amazonaws.com/splice-releases/2.6.1.1745/cluster/installer/mapr5.2.0/SPLICEMACHINE-2.6.1.1745.mapr5.2.0.p0.121.tar.gz</a></td>
        </tr>
        <tr>
           <td><strong>2.5</strong></td>
           <td><strong>2.5.0</strong></td>
           <td><a href="https://s3.amazonaws.com/splice-releases/2.5.0.1802/cluster/installer/mapr5.2.0/SPLICEMACHINE-2.5.0.1802.mapr5.2.0.p0.540.tar.gz">https://s3.amazonaws.com/splice-releases/2.5.0.1802/cluster/installer/mapr5.2.0/SPLICEMACHINE-2.5.0.1802.mapr5.2.0.p0.540.tar.gz</a></td>
        </tr>
    </tbody>
   </table>

   **NOTE:** To be sure that you have the latest URL, please check [the Splice
   Machine Community site](https://community.splicemachine.com/) or contact your Splice
   Machine representative.

2. Create the `splice` installation directory:

   ````
   mkdir -p /opt/splice
   ````

3. Download the Splice Machine package into the `splice` directory on
   the node. For example:

   ````
   cd /opt/splicecurl -O '////SPLICEMACHINE-...'
   ````

4. Extract the Splice Machine package:

   ````
   tar -xf SPLICEMACHINE-...
   ````

5. Create a symbolic link. For example:

   ````
   ln -sf SPLICEMACHINE-... default
   ````

6. Run our script as *root* user **on each
   node** in your cluster to add symbolic links to the set up the
   Splice Machine jar and to script symbolic links:

   Issue this command on each node in your cluster:

   ````
   sudo bash /opt/splice/default/scripts/install-splice-symlinks.sh
   ````

## Configure Cluster Services

The scripts used in this section all assume that password-less <code>ssh </code>and password-less <code>sudo </code>are enabled across all cluster nodes.
These scripts are designed to be run on the CLDB node in a cluster with
only one CLDB node. If your cluster has multiple CLDB nodes, do not run
these script; you need to change configuration settings manually **on each node** in the cluster. To do so, refer to
the `*.patch` files in the `/opt/splice/default/conf` directory.

If you're running on a cluster with a single CLDB node, follow these
steps:

1. Tighten the ephemeral port range so HBase doesn't bump into it:

   ````
   cd /opt/splice/default/scripts
   ./install-sysctl-conf.sh
   ````

2. Update `hbase-site.xml`:

   ````
   cd /opt/splice/default/scripts
   ./install-hbase-site-xml.sh
   ````

3. Update `hbase-env.sh`:

   ````
   cd /opt/splice/default/scripts
   ./install-hbase-env-sh.sh
   ````

4. Update `yarn-site.xml`:

   ````
   cd /opt/splice/default/scripts
   ./install-yarn-site-xml.sh
   ````

5. Update `warden.conf`:

   ````
   cd /opt/splice/default/scripts
   ./install-warden-conf.sh
   ````

6. Update `zoo.cfg`:

   ````
   cd /opt/splice/default/scripts
   ./install-zookeeper-conf.sh
   ````

## Stop Cluster Services

You need to stop all cluster services to continue with installing Splice
Machine. Run the following commands as `root` user **on each node** in your cluster

   ````
   sudo service mapr-warden stopsudo service mapr-zookeeper stop
   ````

## Restart Cluster Services

Once you've completed the configuration steps described above, start
services on your cluster; run the following commands **on each node** in your cluster:

   ````
   sudo service mapr-zookeeper startsudo service mapr-warden start
   ````


## Create the Splice Machine Event Log Directory

Create the Splice Machine event log directory by executing the following
commands on a single cluster node while MapR is up and running:

   ````
   sudo -su mapr hadoop fs -mkdir -p /user/splice/historysudo -su mapr hadoop fs -chown -R mapr:mapr /user/splicesudo -su mapr hadoop fs -chmod 1777 /user/splice/history
   ````

## Optional Configuration Modifications

There are a few configuration modifications you might want to make:

* [Modify the Authentication Mechanism](#modify-the-authentication-mechanism) if you want to
  authenticate users with something other than the default *native
  authentication* mechanism.
* [Modify the Log Location](#modify-the-log-location) if you want your Splice Machine
  log entries stored somewhere other than in the logs for your region
  servers.
* [Adjust the Replication Factor](#adjust-the-replication-factor) if you have a small cluster
  and need to improve resource usage or performance.

### Modify the Authentication Mechanism

Splice Machine installs with Native authentication configured; native
authentication uses the `sys.sysusers` table in the `splice` schema for
configuring user names and passwords.

You can disable authentication or change the authentication mechanism
that Splice Machine uses to LDAP by following the simple instructions in
[Command Line (splice&gt;) Reference](https://doc.splicemachine.com/cmdlineref_intro.html)

### Modify the Log Location

Splice Machine logs all SQL statements by default, storing the log
entries in your region server's logs, as described in our [Using
Logging](developers_tuning_logging) topic. You can modify where Splice
Machine stores logs as follows:

1. Append the following configuration information to the `/opt/mapr/hbase/hbase-1.1.1/conf/log4jd/properties` file _on each node_ in your cluster:

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

2. Use either of these methods to take the log changes live:

   * Restart the MapR service on each node in your cluster:

     ````
     service mapr-warden restart
     ````

    * OR, restart the HBase service _on each node_ in your cluster by issuing these commands from your Master node:

      ````
      sudo -su mapr maprcli node services -hbmaster restart -nodes **&lt;master node&gt;** <br />
      sudo -su mapr maprcli node services -hbregionserver restart -nodes  **&lt;regional node 1&gt; &lt;regional node 2&gt; ... &lt;regional node n&gt;**
      ````

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
