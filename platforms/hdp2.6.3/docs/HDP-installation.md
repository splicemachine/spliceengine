(to be updated with new PRM packaging info)

# Installing and Configuring Splice Machine for Hortonworks HDP

This topic describes installing and configuring Splice Machine on a
Hortonworks Ambari-managed cluster. Follow these steps:

1. [Verify Prerequisites](#verify-prerequisites)
2. [Download and Install Splice Machine](#download-and-install-splice-machine)
3. [Install Splice Machine Using Ambari Service](#install-splice-machine-using-ambari-service)
4. [Start Any Additional Services](#start-any-additional-services)
5. Make any needed [Optional Configuration Modifications](#optional-configuration-modifications)
6. [Verify your Splice Machine Installation](#verify-your-splice-machine-installation)

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

Set up a local `yum` repo on an Ambari server node (or a node that all the nodes in the cluster can access):

1. Make sure there is a http server on the node that <your_node_url> is accessible.
2. Make sure `createrepo` is installed on the node ( use 'yum install createrepo' to confirm).
3. Put the `splicemachine` rpm under `/var/www/html/ambari-repo/` ( or the path you choose).
4. Use 'createrepo /var/www/html/ambari-repo/' to create the repo metadata.
5. Open the url `<your_node_url>/ambari-repo to` confirm it can be accessed by `yum`. 
6. Add a file named `splicemachine.repo` under `/etc/yums.repo.d/`, with the following content:

    ````
    [splicemachine]
    name=SpliceMachine Repo
    baseurl=http://your_node_url/ambari-repo
    enabled=1
    gpgcheck=0
    ````
7. Run `yum list | grep splicemachine` to make sure the custom repo is up and running.  

### Install the Splice Machine Ambari service **on each node** in your cluster:

Install the splicemachine custom ambari service rpm using the following command:

    ````
    sudo yum install splicemachine_ambari_service
    ````

After installing the rpm, restart the ambari-server using the `ambari-server restart` service.

## Install Splice Machine Using the Ambari Service

Follow the steps to install splicemachine server:

1. Click the action button on the left bottom of the Ambari page, then click `Add Services`:

   <img src="docs/add_services.jpg" alt="Add Service" width="400" height="200">

2. Choose `splice machine` from the `add service wizard`:

   <img src="docs/add_service_wizard.jpg" alt="Add Service Wizard" width="400" height="200">

3. Choose the master machine. It needs to be the HBase Master machine.

4. Choose hosts needed to install splice machine; only choose hosts that have hbase region server 
installed. Then click `next`.

   <img src="docs/choose_hosts.jpeg" alt="Choose hosts" width="400" height="200">

5. You only need to customize properties page if you want to add [Apache Ranger Support](#enabling-ranger-for-authorization).

   <img src="docs/custom_services.jpeg" alt="Custom Services" width="400" height="200">

6. Review all the configuration change made by aAbari and click `OK` to continue.

   <img src="docs/dependent_config.jpeg" alt="dependent_config.jpeg" width="400" height="200">

7. Click `next` at the bottom of the page, then click `deploy`. Once this completes, Splice Machine is installed.

   <img src="docs/review.jpeg" alt="dependent_config.jpeg" width="400" height="200">

8. Restart all affected services to start Splice Machine!


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
Authentication](https://doc.splicemachine.com/onprem_install_configureauth.html).

If you're using Kerberos, you need to add this option to your HBase Master Java Configuration Options:

   ````
   -Dsplice.spark.hadoop.fs.hdfs.impl.disable.cache=true
   ````
   
### Enabling Ranger for Authorization

Splice Machine installs with Native authorization configured; native
authorization uses the Splice Machine dictionary tables to determine permissions on database objects.


#### Configure Splice Machine Ambari Service

Do the following in the `Advanced ranger-splicemachine-audit` tab:

1. Select `audit to HDFS`
2. Select `audit to SOLR`
3. For the configuration `xasecure.audit.destination.solr.urls`, change `localhost` to the `hostname/node` 
for SOLR.
4. Set `xasecure.audit.is.enabled` to `true`.

Do the following in the `Advanced ranger-splicemachine-security` tab:

1. Update the value of `ranger.plugin.splicemachine.policy.rest.url`
2. Change `localhost` to the `host/node` for Ranger Server


#### Add Ranger Service for Splice Machine

Before changing the authorization scheme, the Splice Machine ranger service needs to be installed.  As part of the Splice Machine Ambari Service, the admin plugin for Splice Machine is added to the Ranger web application.

The service can be installed by executing the following from a command line on the machine where the Ambari Service resides, then posting this file to Ranger API. Run the command below on master. 

```
curl -sS -u admin:admin -H "Content-Type: application/json" -X POST http://localhost:6080/service/plugins/definitions -d @/var/lib/ambari-server/resources/stacks/HDP/2.6/services/SPLICEMACHINE/configuration/ranger-servicedef-splicemachine.json
```

Note: `admin:admin` here is Ranger's username and password.

1. Go to the Ranger admin web page, where you should see the SpliceMachine plug-in.
2. Click the plus sign (`+`) next to `SpliceMachine`
3. You need to add a Service, the name of which is the same name that you configured in
`ranger.plugin.splicemachine.service.name`. The default value for this name is `splicemachine`.

   Note: if you see an error such as the following, try `test connection`:

    ````
Unable to retrieve any files using given parameters, You can still save the repository and start
creating policies, but you would not be able to use autocomplete for resource names.
Check ranger_admin.log for more info.

org.apache.ranger.plugin.client.HadoopException: Unable to login to Hadoop environment [splicemachine]. 
Unable to login to Hadoop environment [splicemachine]. 
Unable to decrypt password due to error. 
Input length must be multiple of 8 when decrypting with padded cipher. 
    ````

   This error is due to a [Ranger bug](https://issues.apache.org/jira/browse/RANGER-1640?attachmentOrder=asc).
You can ignore the error and test if autocomplete is working later.

#### Configure Ranger Policies

Once you save the service, click the service name you just created.
You should see a several policies for the `splice` user.

The following policy is required so that `SYSIBM` routines can support database connectivity:

| Required Policy Name | Logic | Users |
|--------------|------|------|
| SYSIBM| `Schema=SYSIBM,routine=*,permissions=execute` | `All users/groups that will use the database`

Note that when you create a database user with a statement like this:

```sql
call syscs_util.syscs_create_user('ranger_test', 'admin');
```

The username is actually converted to uppercase; this means that you need to configure the username as `RANGER_TEST`
 in Ranger. If you want to create a database user with a lowercase name, quote the username with double
  quote within single quotes; for example:
  
```sql
call syscs_util.syscs_create_user('"ranger_test"', 'admin');
```

##### Configure HBase

Once this is done, you can change the authorization scheme to `RANGER` by adding the following option to your HBase Region Server Java Configuration Options:

   ````
   -Dsplice.authorization.scheme=RANGER
   ````

The authorization scheme value is set to **NATIVE** by default.

### Modify the Log Location

#### Query Statement log

Splice Machine logs all SQL statements by default, storing the log
entries in your region server's logs, as described in our [Using
Logging](https://doc.splicemachine.com/developers_tuning_logging) topic. You can modify where Splice
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
   
#### OLAP Server Log
   
Splice Machine uses log4j to config OLAP server's log.  If you want to change the default log behavior of OLAP server,
config `splice.olap.log4j.configuration` in `hbase-site.xml`. It specifies the log4j.properties file you want to use.
This file needs to be available on HBase master server.


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
