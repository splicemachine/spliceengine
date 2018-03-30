(to be updated with new PRM packaging info)

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

Setup local yum repo on ambari server node ( or a node that all the nodes in the cluster can access) :

0.make sure there is a http server on the node that your_node_url is accessable.
1.make sure createrepo is installed on the node ( use 'yum install createrepo' to confirm)
2.put the splicemachine rpm under /var/www/html/ambari-repo/ ( or the path you choose)
3.use 'createrepo /var/www/html/ambari-repo/' to create the repo metadata
4.open the url your_node_url/ambari-repo to confirm it can be accessed by yum
5.put a file named splicemachine.repo under /etc/yums.repo.d/

the content is as below 

  ````
  [splicemachine]
  name=SpliceMachine Repo
  baseurl=http://your_node_url/ambari-repo
  enabled=1
  gpgcheck=0
  ````
  
6. rum yum list | grep splicemachine to make sure the custom repo is up and running.  

Perform the following steps **on each node** in your cluster:

1.  install the splicemachine  custom ambari service rpm using following command (take version 2.5.0.1811 for example) :

    ````
    sudo yum install splicemachine_ambari_service
    ````

After install the rpm,restart ambari-server using 'service ambari-server restart'


## install splicemachine using ambari service

Follow the steps to install splicemachine server.

1.click the action button on the left buttom of the ambari page,then click on 'Add Services'

<img src="docs/add_services.jpg" alt="Add Service" width="400" height="200">

2.choose splice machine from the 'add service wizard'

<img src="docs/add_service_wizard.jpg" alt="Add Service Wizard" width="400" height="200">

3.choose hosts needed to install splice machine,only choose hosts that have hbase region server installed.Then click next.

<img src="docs/choose_hosts.jpeg" alt="Choose hosts" width="400" height="200">

4.O On the page of custom services,no properties need to customize by hand.

<img src="docs/custom_services.jpeg" alt="Custom Services" width="400" height="200">

5.please review all the configuration change made by ambari and click OK to continue.

<img src="docs/dependent_config.jpeg" alt="dependent_config.jpeg" width="400" height="200">

6.please click next all the way down to this page ,then click 'deploy',after that finishes,splice machine is installed.

<img src="docs/review.jpeg" alt="dependent_config.jpeg" width="400" height="200">

7.restart all the services affected to start splice machine!



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
Machine stores logs by adding the following snippet to your *RegionServer Logging
Advanced Configuration Snippet (Safety Valve)* section of your HBase
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
