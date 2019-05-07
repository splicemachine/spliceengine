
# Installing and Configuring Splice Machine for Hortonworks HDP

This topic describes installing and configuring Splice Machine on a
Hortonworks Ambari-managed cluster. Follow these steps:

- [Installing and Configuring Splice Machine for Hortonworks HDP](#installing-and-configuring-splice-machine-for-hortonworks-hdp)
  - [Verify Prerequisites](#verify-prerequisites)
  - [Download and Install Splice Machine](#download-and-install-splice-machine)
  - [Install splicemachine using Ambari service](#install-splicemachine-using-ambari-service)
  - [Start any Additional Services](#start-any-additional-services)
  - [Optional Configuration Modifications](#optional-configuration-modifications)
    - [Modify the Authentication Mechanism](#modify-the-authentication-mechanism)
    - [Enabling Ranger for Authorization](#enabling-ranger-for-authorization)
      - [Config Splice Machine Ambari Service](#config-splice-machine-ambari-service)
      - [Add Ranger Service for Splice Machine](#add-ranger-service-for-splice-machine)
      - [Config Ranger Policies](#config-ranger-policies)
        - [Config HBase](#config-hbase)
    - [Modify the Log Location](#modify-the-log-location)
      - [Query Statement log](#query-statement-log)
      - [OLAP Server Log](#olap-server-log)
  - [Verify your Splice Machine Installation](#verify-your-splice-machine-installation)
  - [Upgrade from Old Version](#upgrade-from-old-version)
- [Backdown procedure](#backdown-procedure)

## Verify Prerequisites

Before starting your Splice Machine installation, please make sure that
your cluster contains the prerequisite software components:

* A cluster running HDP
* Ambari installed and configured for HDP
* HBase installed
* HDFS installed
* YARN installed
* ZooKeeper installed
* Spark 2 installed
* Ensure that Phoenix services are **NOT** installed on your cluster, as
  they interfere with Splice Machine HBase settings.

**NOTE:** The specific versions of these components that you need depend on your
operating environment, and are called out in detail in the
[Requirements](https://doc.splicemachine.com/onprem_info_requirements.html) topic of our *Getting Started Guide*.

## Download and Install Splice Machine

Setup local yum repo on ambari server node ( or a node that all the nodes in the cluster can access) :

1. Make sure there is a http server on the node that your_node_url is accessable.
2. Make sure createrepo is installed on the node ( use 'yum install createrepo' to confirm)
3. Put the splicemachine rpm under `/var/www/html/ambari-repo/` ( create directory if it doesn't exist)
4. Use `createrepo /var/www/html/ambari-repo/` to create the repo metadata.
5. Open the url `your_node_url/ambari-repo` to confirm it can be accessed by yum.

Perform the following steps 6 and 7 **on each node** in your cluster:

In order to run it all on each node, it is useful to use the "all_ssh" bash script, create the file in the master node:

````
#!/bin/bash
tmpdir=${TMPDIR:-/tmp}/pssh.$$
if [ -z "$1" ]
then
   echo "Usage: $0 <command to run on all nodes>"
   echo "userhost.lst file should contain all target nodes."
   exit
fi
echo "Output logs -> $tmpdir"
cmd=$1
echo "Running command [${cmd}] on all nodes."
mkdir -p $tmpdir
count=0
while IFS= read -r userhost; do
    ssh -n -o BatchMode=yes ${userhost} "${cmd}" > ${tmpdir}/${userhost} 2>&1 &
    count=`expr $count + 1`
done < userhost.lst
while [ $count -gt 0 ]; do
    wait $pids
    count=`expr $count - 1`
done
while IFS= read -r userhost; do
    echo
    echo "=====> START OUTPUT FROM ${userhost} [${cmd}] <======= "
    cat ${tmpdir}/${userhost}
    echo "=====> END OUTPUT FROM ${userhost} "
    echo
done < userhost.lst
#echo "Output for hosts are in $tmpdir ”
````

it will also require execute permissions, use:
`chmod 700 all_ssh`


The all_ssh script requires that a "userhost.lst" file be created in the same directory from which it is being executed,
the userhost.lst file contains a list of hostnames as in:
    ````
    host1
    host2
    host3
    ````

The all_ssh script will also require execute permissions, use:
`chmod 700 all_ssh`


6. Put a file named `splicemachine.repo` under ``/etc/yums.repo.d/` with the content is as below

  ````
  [splicemachine]
  name=SpliceMachine Repo
  baseurl=http://your_node_url/ambari-repo
  enabled=1
  gpgcheck=0
  ````
copy to the other nodes with:
  ````
  ./all_ssh "scp splice@stl-colo-srv006:/etc/yum.repos.d/splicemachine.repo /tmp/splicemachine.repo"
  ./all_ssh "sudo chown root:root /tmp/splicemachine.repo"
  ./all_ssh "sudo mv /tmp/splicemachine.repo /etc/yum.repos.d/"
  ````

7. Run `yum list | grep splicemachine` to make sure the custom repo is up and running.


Install the Splice Machine custom Ambari service rpm using the following command (take version for example) :

```
./all_ssh "sudo yum install -y splicemachine_ambari_service"
```

After install the rpm, restart ambari-server using `service ambari-server restart` on the master node.




## Install splicemachine using Ambari service

Follow the steps to install splicemachine server.

0. On the Ambari UI - Make a note of the current version of each service running on the cluster by looking at their individual config views.
Note: These versions will be used for the backout procedure when uninstalling Splice Machine

1. Click the action button on the left bottom of the ambari page,then click on 'Add Services'

<img src="docs/add_services.jpg" alt="Add Service" width="400" height="200">

2. Choose splice machine from the 'add service wizard'

<img src="docs/add_service_wizard.jpg" alt="Add Service Wizard" width="400" height="200">

3. Choose hosts needed to install splice machine. Choose both HBase master and HBase region servers. Then click next.

<img src="docs/choose_hosts.jpeg" alt="Choose hosts" width="400" height="200">

4. On the page of custom services, no properties need to customized by hand unless you would
like to add Apache Ranger Support.

<img src="docs/custom_services.jpeg" alt="Custom Services" width="400" height="200">

5. Please review all the configuration changes made by Ambari and click OK to continue.

<img src="docs/dependent_config.jpeg" alt="dependent_config.jpeg" width="400" height="200">

**Note**: Ambari will not show all the recommended values in some situations. Make sure these
important configurations are set properly by clicking "recommend" button next to the configs:

(1) In HBase's config "Advanced hbase-site", make sure `hbase.coprocessor.master.classes` includes `com.splicemachine.hbase.SpliceMasterObserver`.

(2) In HBase's config "Advanced hbase-site", make sure `hbase.coprocessor.regionserver.classes` includes `com.splicemachine.hbase.RegionServerLifecycleObserver`.

(3) In HBase's config "Advanced hbase-site", make sure `hbase.coprocessor.region.classes` includes `org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint,com.splicemachine.hbase.MemstoreAwareObserver,com.splicemachine.derby.hbase.SpliceIndexObserver,com.splicemachine.derby.hbase.SpliceIndexEndpoint,com.splicemachine.hbase.RegionSizeEndpoint,com.splicemachine.si.data.hbase.coprocessor.TxnLifecycleEndpoint,com.splicemachine.si.data.hbase.coprocessor.SIObserver,com.splicemachine.hbase.BackupEndpointObserver`. If the property is not found, you can add the property in "Custom hbase-site".

(4) In Hbase's config "hbase-env template", make sure the comments like "Splice Specific
Information" are in the configurations.


6. Please click next all the way down to this page ,then click 'deploy'. After that finishes, Splice
 Machine is installed.

<img src="docs/review.jpeg" alt="dependent_config.jpeg" width="400" height="200">

7. Create HDFS folders:
hadoop fs -mkdir /user/splice
hadoop fs -mkdir /user/splice/history
hadoop fs -mkdir /user/splice/spark-warehouse

hadoop fs -chmod 1777 /user/splice
hadoop fs -chmod 1777 /user/splice/history
hadoop fs -chmod 755 /user/splice/spark-warehouse

hadoop fs -chown hbase:hbase /user/splice
hadoop fs -chown hbase:spark /user/splice/history
hadoop fs -chown hbase:hbase /user/splice/spark-warehouse


8. Restart all the services affected to start Splice Machine!
Splice Machine is now functional, the rest of this installation procedures are optional.



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

### Enabling Ranger for Authorization

Splice Machine installs with Native authorization configured; native
authorization uses the Splice Machine dictionary tables to determine permissions on database objects.


#### Config Splice Machine Ambari Service


In the tab:  Advanced ranger-splicemachine-audit

1. Check audit to HDFS
2. Check audit to SOLR
3. For the config: xasecure.audit.destination.solr.urls change localhost to the hostname / node
for SOLR
4. Set xasecure.audit.is.enabled to true

In the tab:  Advanced ranger-splicemachine-security

1. Update ranger.plugin.splicemachine.policy.rest.url
2. Change localhost to the host / node for Ranger Server


#### Add Ranger Service for Splice Machine

Before changing the authorization scheme, the Splice Machine ranger service needs to be installed.  As part of the Splice Machine Ambari Service,
the admin plugin for Splice Machine is added to the Ranger web application.

The service can be installed by executing the following from a command line on the machine where the Ambari Service resides.

Then post this file to Ranger API. Run the command bellow on master. `admin:admin` here is
Ranger's username and password.

```
curl -sS -u admin:admin -H "Content-Type: application/json" -X POST http://localhost:6080/service/plugins/definitions -d @/var/lib/ambari-server/resources/stacks/HDP/2.6/services/SPLICEMACHINE/configuration/ranger-servicedef-splicemachine.json
```
1. Go to Ranger admin web page.
2. You should see SpliceMachine Plugin
3. Click on the plus sign (+) next to SpliceMachine
4. Need to add a Service: the service name is the same name as you configured in
`ranger.plugin.splicemachine.service.name`, which is `splicemachine` by default.

Note: if you see some error like this when click "test connection":

```
Unable to retrieve any files using given parameters, You can still save the repository and start
creating policies, but you would not be able to use autocomplete for resource names.
Check ranger_admin.log for more info.

org.apache.ranger.plugin.client.HadoopException: Unable to login to Hadoop environment [splicemachine].
Unable to login to Hadoop environment [splicemachine].
Unable to decrypt password due to error.
Input length must be multiple of 8 when decrypting with padded cipher.
```

It is because of a [Ranger bug](https://issues.apache.org/jira/browse/RANGER-1640).
You can ignore the error and test if autocomplete is working later.

#### Config Ranger Policies

Once you save the service then click on the service name you just created.
You should see several policies for the splice user.
The following policy is required so SYSIBM routines can support database connectivity.

| Required Policy Name | Logic | Users |
|--------------|------|------|
| SYSIBM| `Schema=SYSIBM,routine=*,permissions=execute` | `All users/groups that will use the database`

Note: when you create database user with

```sql
call syscs_util.syscs_create_user('ranger_test', 'admin');
```

Actually the username is parsed as uppercase. So you need to config the username as `RANGER_TEST`
 in Ranger. If you want to create a database user with lower case, quote the username with double
  quote in a single quote:

```sql
call syscs_util.syscs_create_user('"ranger_test"', 'admin');
```

##### Config HBase

Once this is done, you can change the authorization scheme to RANGER by adding this option to your HBase Region Server Java Configuration Options:

   ````
   -Dsplice.authorization.scheme=RANGER
   ````

It is set to **NATIVE** by default.

### Modify the Log Location

#### Query Statement log

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

## Upgrade from Old Version

If you are upgrading from versions before 1901, you need to follow these steps:

1. Delete Splice Ambari service on web UI.
2. Update RPM packages on each machine.
3. Restart Ambari server.
4. Re-install Splice Ambari service from web UI.

# Backdown procedure

To remove Splice Machine from the HDP cluster:

1. Remove Splice service
 Click on Splice Machine service, on the top right click on Service Actions dropdown - and select Delete Service -

2. Rollback configurations
   within each service in the config tab, select prior versions if they were changed during install and click on "Make Current".

3. From Ambari UI using the cluster Actions dropdown, select "Stop All" and wait for cluster to stop.
   From the master node terminal do :
   ````
   ./all_ssh “sudo yum remove -y splicemachine_ambari_service”
   sudo yum remove -y splicemachine_ambari_service

   ./all_ssh “sudo rm /etc/yum.repos.d/splicemachine.repo”
   sudo rm /etc/yum.repos.d/splicemachine.repo
   ````

   If the ambari-repo folder was created at install time under /var/www/html on the master node at install time, then remove it:
   `sudo rm -r /var/www/html/ambari-repo`

1. Restart cluster - From Ambari UI using the cluster Actions dropdown, select "Start All".
   Note that even after start completes, some alerts may still be active until service restarts complete. Just wait for alerts to clear.
