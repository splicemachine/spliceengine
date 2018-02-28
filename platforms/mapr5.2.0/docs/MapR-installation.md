---
title: 'Documentation : MapR Install'
---

::: {#page}
::: {#main .aui-page-panel}
::: {#main-header}
::: {#breadcrumb-section}
1.  [Documentation](index.html)
2.  [Documentation](Documentation_197787719.html)
3.  [Installation Documents](Installation-Documents_198475781.html)
:::

[ Documentation : MapR Install ]{#title-text} {#title-heading .pagetitle}
=============================================
:::

::: {#content .view}
::: {.page-metadata}
Created by [ Gary Hillerson]{.author}, last modified on Feb 07, 2018
:::

::: {#main-content .wiki-content .group}
::: {.post-content style="text-align: left;"}
::: {.TopicContent}
    [Learn about our
products](https://www.splicemachine.com/get-started/){.external-link}

Installing and Configuring Splice Machine for MapR {#MapRInstall-InstallingandConfiguringSpliceMachineforMapR}
==================================================

<div>

This topic describes installing and configuring Splice Machine on a
MapR-managed cluster. Follow these steps:

1.  [Verify
    Prerequisites](http://docstest.splicemachine.com/onprem_install_mapr.html#Verify){.external-link}
2.  [Download and Install Splice
    Machine](http://docstest.splicemachine.com/onprem_install_mapr.html#Install){.external-link}
3.  [Configure Cluster
    Services](http://docstest.splicemachine.com/onprem_install_mapr.html#Configur){.external-link}
4.  [Stop Cluster
    Services](http://docstest.splicemachine.com/onprem_install_mapr.html#Stop){.external-link}
5.  [Restart Cluster
    Services](http://docstest.splicemachine.com/onprem_install_mapr.html#StartCluster){.external-link}
6.  [Create the Splice Machine Event Log
    Directory](http://docstest.splicemachine.com/onprem_install_mapr.html#CreateLogDir){.external-link}
7.  Make any needed [Optional Configuration
    Modifications](http://docstest.splicemachine.com/onprem_install_mapr.html#Optional){.external-link}
8.  [Verify your Splice Machine
    Installation](http://docstest.splicemachine.com/onprem_install_mapr.html#Run){.external-link}

::: {.openSourceNote}
[MapR Secure Clusters Only Work with the Enterprise Edition of Splice
Machine]{.noteEnterpriseNote style="color: rgb(102,102,102);"}

MapR secure clusters do not support the Community Edition of Splice
Machine. You can check this by:

1.  Open the HBase Configuration page

2.  Search the XML for the `hbase.security.authentication` setting

3.  If the setting value is anything other than `simple`, you need
    the *Enterprise* edition of Splice Machine.

To read more about the additional features available in the Enterprise
Edition, see our [Splice Machine
Editions](http://docstest.splicemachine.com/notes_editions.html){.external-link} page.
To obtain a license for the Splice Machine *Enterprise
Edition*, [please [Contact Splice Machine
Sales](http://www.splicemachine.com/company/contact-us/){.external-link} today.]{.noteEnterpriseNote
style="color: rgb(102,102,102);"}
:::

Verify Prerequisites {#MapRInstall-VerifyPrerequisites}
--------------------

Before starting your Splice Machine installation, please make sure that
your cluster contains the prerequisite software components:

-   A cluster running MapR with MapR-FS

-   HBase installed

-   YARN installed

-   ZooKeeper installed

-   The latest `mapr-patch` should be installed to bring in all
    MapR-supplied platform patches, before proceeding with your Splice
    Machine installation.

-   The MapR Ecosystem Package (MEP) should be installed on all cluster
    nodes, before proceeding with your Splice Machine installation.

-   Ensure Spark services are **NOT** installed; Splice Machine cannot
    currently coexist with Spark 1.x on a cluster:

    -   If `MEP version 1.x` is in use, you must remove
        the `Spark 1.x` packages from your cluster, as described below,
        in [Removing Spark Packages from Your
        Cluster.](http://docstest.splicemachine.com/onprem_install_mapr.html#Removing){.external-link}

    -   MEP version 2.0 bundles Spark 2.0, and will not cause conflicts
        with Splice Machine.

The specific versions of these components that you need depend on your
operating environment, and are called out in detail in
the [Requirements](http://docstest.splicemachine.com/onprem_info_requirements.html){.external-link} topic
of our *Getting Started Guide*.

### Removing Spark Packages from Your Cluster {#MapRInstall-RemovingSparkPackagesfromYourCluster}

To remove Spark packages from your cluster and update your
configuration, follow these steps

::: {.opsStepsList style="text-align: left;"}
1.  If you're running a Debian/Ubuntu-based Linux distribution:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    dpkg -l | awk '/ii.*mapr-spark/{print $2}' | xargs sudo apt-get purge
    ```
    :::

    If you're running on a RHEL/CentOS-based Linux distribution:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    rpm -qa \*mapr-spark\* | xargs sudo yum -y erase
    ```
    :::

2.  Reconfigure node services in your cluster:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    sudo /opt/mapr/server/configure.sh -R
    ```
    :::
:::

Download and Install Splice Machine {#MapRInstall-DownloadandInstallSpliceMachine}
-----------------------------------

Perform the following steps [on each node]{.important
style="color: rgb(255,0,0);"} in your cluster:

::: {.opsStepsList style="text-align: left;"}
1.  Download the installer for your version.

    Which Splice Machine installer (gzip) package you need depends upon
    which Splice Machine version you're installing and which version of
    MapR you are using. Here are the URLs for Splice Machine Release
    2.7.0 and 2.5.0:

    ::: {.table-wrap}
    Splice Machine Release
    :::

2.  Create the `splice` installation directory:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    mkdir -p /opt/splice
    ```
    :::

3.  Download the Splice Machine package into the `splice` directory on
    the node. For example:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    cd /opt/splicecurl -O '////SPLICEMACHINE-..'
    ```
    :::

4.  Extract the Splice Machine package:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    tar -xf SPLICEMACHINE-..
    ```
    :::

5.  Create a symbolic link. For example:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    ln -sf SPLICEMACHINE-.. default
    ```
    :::

6.  Run our script as *root* user [on each node]{.important
    style="color: rgb(255,0,0);"} in your cluster to add symbolic links
    to the set up the Splice Machine jar and to script symbolic links:

    Issue this command on each node in your cluster:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    sudo bash /opt/splice/default/scripts/install-splice-symlinks.sh
    ```
    :::
:::

Configure Cluster Services {#MapRInstall-ConfigureClusterServices}
--------------------------

The scripts used in this section all assume that
password-less [ssh ]{.ShellCommand style="color: inherit;"}and
password-less [sudo ]{.ShellCommand style="color: inherit;"}are enabled
across all cluster nodes. These scripts are designed to be run on the
CLDB node in a cluster with only one CLDB node. If your cluster has
multiple CLDB nodes, do not run these script; you need to change
configuration settings manually [on each node]{.important
style="color: rgb(255,0,0);"} in the cluster. To do so, refer to
the `*.patch` files in the `/opt/splice/default/conf` directory.

If you're running on a cluster with a single CLDB node, follow these
steps:

::: {.opsStepsList style="text-align: left;"}
1.  Tighten the ephemeral port range so HBase doesn't bump into it:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    cd /opt/splice/default/scripts
    ./install-sysctl-conf.sh
    ```
    :::

2.  Update `hbase-site.xml`:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    cd /opt/splice/default/scripts
    ./install-hbase-site-xml.sh
    ```
    :::

3.  Update `hbase-env.sh`:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    cd /opt/splice/default/scripts
    ./install-hbase-env-sh.sh
    ```
    :::

4.  Update `yarn-site.xml`:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    cd /opt/splice/default/scripts
    ./install-yarn-site-xml.sh
    ```
    :::

5.  Update `warden.conf`:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    cd /opt/splice/default/scripts
    ./install-warden-conf.sh
    ```
    :::

6.  Update `zoo.cfg`:

    ::: {.preWrapperWide}
    ``` {.ShellCommand}
    cd /opt/splice/default/scripts
    ./install-zookeeper-conf.sh
    ```
    :::
:::

Stop Cluster Services {#MapRInstall-StopClusterServices}
---------------------

You need to stop all cluster services to continue with installing Splice
Machine. Run the following commands as `root` user [on each
node]{.important style="color: rgb(255,0,0);"} in your cluster

::: {.preWrapperWide}
``` {.ShellCommand}
sudo service mapr-warden stopsudo service mapr-zookeeper stop
```
:::

Restart Cluster Services {#MapRInstall-RestartClusterServices}
------------------------

Once you've completed the configuration steps described above, start
services on your cluster; run the following commands [on each
node]{.important style="color: rgb(255,0,0);"} in your cluster:

::: {.preWrapper}
``` {.ShellCommand}
sudo service mapr-zookeeper startsudo service mapr-warden start
```
:::

Create the Splice Machine Event Log Directory {#MapRInstall-CreatetheSpliceMachineEventLogDirectory}
---------------------------------------------

Create the Splice Machine event log directory by executing the following
commands on a single cluster node while MapR is up and running:

::: {.preWrapper}
``` {.ShellCommand}
sudo -su mapr hadoop fs -mkdir -p /user/splice/historysudo -su mapr hadoop fs -chown -R mapr:mapr /user/splicesudo -su mapr hadoop fs -chmod 1777 /user/splice/history
```
:::

Optional Configuration Modifications {#MapRInstall-OptionalConfigurationModifications}
------------------------------------

There are a few configuration modifications you might want to make:

-   [Modify the Authentication
    Mechanism](http://docstest.splicemachine.com/onprem_install_mapr.html#Modify){.external-link} if
    you want to authenticate users with something other than the
    default *native authentication*mechanism.
-   [Modify the Log
    Location](http://docstest.splicemachine.com/onprem_install_mapr.html#Logging){.external-link} if
    you want your Splice Machine log entries stored somewhere other than
    in the logs for your region servers.
-   [Adjust the Replication
    Factor](http://docstest.splicemachine.com/onprem_install_mapr.html#Adjust){.external-link} if
    you have a small cluster and need to improve resource usage or
    performance.

### Modify the Authentication Mechanism {#MapRInstall-ModifytheAuthenticationMechanism}

Splice Machine installs with Native authentication configured; native
authentication uses the `sys.sysusers` table in the `splice` schema for
configuring user names and passwords.

You can disable authentication or change the authentication mechanism
that Splice Machine uses to LDAP by following the simple instructions
in [Command Line (splice\>)
Reference](http://docstest.splicemachine.com/cmdlineref_intro.html){.external-link}

### Modify the Log Location {#MapRInstall-ModifytheLogLocation}

Splice Machine logs all SQL statements by default, storing the log
entries in your region server's logs, as described in our [Using
Logging](http://docstest.splicemachine.com/developers_tuning_logging){.external-link} topic.
You can modify where Splice Machine stores logs as follows:

::: {.opsStepsList style="text-align: left;"}
1.  Append the following configuration information to
    the `/opt/mapr/hbase/hbase-1.1.1/conf/log4jd/properties` file *on
    each node* in your cluster:

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

2.  Use either of these methods to take the log changes live:

    1.  Restart the MapR service on each node in your cluster:

        service mapr-warden restart

    2.  OR, restart the HBase service *on each node* in your cluster by
        issuing these commands from your Master node:

        sudo -su mapr maprcli node services -hbmaster restart
        -nodes [\<master node\>]{.Highlighted style="color: inherit;"} \
        sudo -su mapr maprcli node services -hbregionserver restart
        -nodes [\<regional node 1\> \<regional node 2\> ... \<regional
        node n\>]{.Highlighted style="color: inherit;"}
:::

### Adjust the Replication Factor {#MapRInstall-AdjusttheReplicationFactor}

The default namespace replication factor for Splice Machine
is [3]{.PlatformVariablesMapRDefaultReplication}. If you're running a
small cluster you may want to adjust the replication down to improve
resource and performance drag. To do so in MapR, use the following
command:

``` {.ShellCommand}
maprcli volume modify -nsreplication <value>
```

For more information, see the MapR documentation
site, [doc.mapr.com](http://doc.mapr.com/ "MapR documentation site"){.external-link}.

Verify your Splice Machine Installation {#MapRInstall-VerifyyourSpliceMachineInstallation}
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

</div>
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

::: {#footer role="contentinfo"}
::: {.section .footer-body}
Document generated by Confluence on Feb 21, 2018 15:36

::: {#footer-logo}
[Atlassian](http://www.atlassian.com/)
:::
:::
:::
