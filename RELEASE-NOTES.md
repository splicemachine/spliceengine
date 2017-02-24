Splice Machine 2.5 Release Notes - Feb. 27 2017
------------------------------------------------

Welcome to 2.5 of Splice Machine! The product is available to build from open source (see https:github.com/splicemachine/spliceengine), as well as prebuilt packages for use on a cluster or cloud.

See the documentation (http://doc.splicemachine.com) for the setup and use of the various versions of the system.

Supported platforms with 2.5:

* Cloudera CDH 5.7.2, 5.8.0, 5.8.3
* MapR 5.1.0 and 5.2.0
* HortonWorks HDP 2.4.2 and 2.5

Reminder that the following features will NOT work on the Community Edition of Splice Machine.  You will need to upgrade to the Enterprise version:

* Backup/Restore
* LDAP integration
* Column-level user privileges
* Kerberos enablement
* Encryption at rest


Running Standalone
-----------------------------------------------
Supported hardware for the STANDALONE release:

* Mac OS X (10.8 or greater)
* Centos (6.4 or equivalent)


New Features in 2.5 Release
-----------------------------------------------
* Performance enhancement on TPCC, TPCH, backup/restore
* Privileges at Schema level
* Incremental Backup/Restore
* Non-native Database dump/load
* ORC/Parquet implemented as External Tables
* SQL WITH clause
* Window Function enhancements
* Caching RDD's (PINNING)
* Statistics Enhancements (including Histograms)


Critical issues fixed in 2.5 Release
-----------------------------------------------
* SPLICE-1353: Export to S3 gives  java.lang.IllegalArgumentExceptionWrong FS
* SPLICE-1329: Memory leak in SpliceObserverInstructions
* SPLICE-1325: NLJoinFunction creates too many threads
* SPLICE-1062: TPCH100g : Import fails with " java.util.concurrent.ExecutionException " in /bad record file.
* SPLICE-1059: Regression: create table using text data type gives syntax error.
* SPLICE-995: TPCC test fails on CDH.5.7.2 platform
* SPLICE-1463: Reading from Spark sometimes returns deleted rows
* SPLICE-1379: The number of threads in the HBase priority executor is inadequately low
* SPLICE-1374: S3: import fails  when S3 file source is specified as /BAD file source, with error: java.lang.IllegalArgumentException
* SPLICE-1366: S3: Import fails in case of BAD directory parameter is set to null  
* SPLICE-1361: Kerberos keytab not picked up by Spark on Splice Machine 2.5
* SPLICE-961: Context manager leaks in OlapServer
* DB-5845: 2.0.x Class Not Found creating custom VTI
* DB-5682: Restore DB brokes database for creation tables in case new table creates after backup.
* DB-5645: Regression: backup_database()  is giving communication error 
* DB-5471: Class Not Found creating custom VTI
* DB-4618: database owner can be changed with improper connection string
* DB-5784: Restore from S3 fails with SQL exception Failed to start database 'splicedb'
* DB-5760: [ODBC] unable to get output of EXPLAIN statement
* DB-5716: [Backup/Restore][Incremental Backup] Unable to select from table after restoring from incremental backup.
* DB-5701: Inconsistent results from a query
* DB-5469: Running SpliceFileVTI not working


Known issues in 2.5 Release
-----------------------------------------------
Known issues can be found at:
http://doc.splicemachine.com/Administrators/ReleaseNotes/CurrentReleaseNotes.html?Highlight=release%20notes#Changes

For a full list of JIRA's for the Community/Open Source software, see https://splice.atlassian.net
