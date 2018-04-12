Splice Machine 2.5 Release Notes (2.5.0.1814)  
---------------------------------------------

Welcome to 2.5 of Splice Machine! The product is available to build from open source (see https:github.com/splicemachine/spliceengine), as well as prebuilt packages for use on a cluster or cloud.

See the documentation (http://doc.splicemachine.com) for the setup and use of the various versions of the system.

Supported platforms with 2.5:

* Cloudera 5.8.3, 5.12.0, 5.12.2, 5.13.2, 5.14.0
* MapR 5.2.0
* HortonWorks HDP 2.6.3

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

What's New in 2.5.0.1814 - April 6, 2018
------------------------------------------

_**New Features**_

* DB-6523 Stable plans for VROL business queries without stats collection on fact tables
* DB-6635 Support Group as the Authenticated User
* DB-6636 Make "splice" user configurable and test it as a group
* DB-6639 Spark Adapter, Import, and Hive Security Model
* DB-6670 Cache Role Grant Permission to avoid frequent data dictionary lookup
* SPLICE-137 Allow Default Role specification to Users, and automatically set on connecting
* SPLICE-1883 Make the row count threshold to determine spark or control execution path a tunable
* SPLICE-2102 Add Stats, truncate, and open ended execute and executeUpdate to Spark Adapter.

_**Improvements**_

* DB-6463  Graceful exit of a query after a timeout
* DB-6478 Add column to indicate 'CONTROL' or 'SPARK' mode for SYSCS_GET_RUNNING_OPERATIONS
* DB-6543 vacuum not handling disabled hbase tables
* DB-6567 Decrease DEFAULT_BROADCAST_REGION_ROW_THRESHOLD
* DB-6582 Inconsistent Export speed 
* DB-6640 LDAP Password show up in Plain Text in Region Server log when Splice is started / Initialized
* DB-6720 Restore: speed up table delete and recreation
* DB-6751 Clean up backup logging for better diagnosability
* DB-6815 Backup: long time to insert into sys.sysbackupitems for 5000+ tables
* SPLICE-1587 GRANT ALL PRIVILEGES ON SCHEMA does not include CREATE TABLE
* SPLICE-175 Run Olap server in separate YARN instance
* SPLICE-1936 Improve Sqlshell.sh to accept kerberos properties as argument
* SPLICE-1987 Add fully qualified table name into HBASE "TableDisplayName" Attribute 
* SPLICE-1991 Add info to Spark UI for Compaction jobs to indicate presence of Reference Files
* SPLICE-2008 Resolve (roll forward) transactions on flush
* SPLICE-2022 Improve spark job description for compaction
* SPLICE-2081 Excessive creation of Kryo instances
* SPLICE-2083 Remove Dead Folders for non-supported versions
* SPLICE-2085 Remove HDP 2.5.0 and HDP 2.5.3 From Build
* SPLICE-2086 CDH 5.14.0 support
* SPLICE-2096 Role Handling on Connection: Always performs scan
* SPLICE-2103 ExplainPlan Generation in the Hot Path
* SPLICE-2104 Dictionary Scans Should Use HBase Small Scans vs. the network heavy regular scan.
* SPLICE-2105 Spark Datasource requires PK for Updates and Deletes
* SPLICE-2106 Buffered Aggregators missing from the transient kryo registrator
* SPLICE-2107 SnappyPipelineCompressor generates too much garbage
* SPLICE-2109 SYSCS_UTIL.VACUUM - Add log message in a finally clause to indicate the vacuum job is complete

_**Bug Fixes**_

* DB-6194 After changing DB Property on ANALYZE default, cannot collect stats on "non-indexed cols that are explicitly enabled"
* DB-6262 cluster/RS's do not come up after HBase restart due to existence of SPLICE_INIT
* DB-6277 Full backup cleaned up incremental changes from a region if the region was taken offline during full backup
* DB-6278 Capture incremental changes from bulk import
* DB-6284 Backups could be inconsistent
* DB-6295 Minor compaction generating a storm of several hundred spark jobs within a very short duration
* DB-6343 Bulk load transaction was not able to commit
* DB-6345 Left-Outer Join results in NPE during parsing 
* DB-6391 HFile IMPORT with Sampling ON results in IllegalFormatConversionException
* DB-6440 SELECT with INlist elements and Index hint failed with StackOverflow when executing for 2 partitions
* DB-6464  ERROR XCL16: Multiple execute statement through Spark generates this error
* DB-6511 Discrepancy between internal and external Nexus repos
* DB-6516 Spark Streaming with Splice Adapter: long running executors fail with out of memory error after 40+ hours of execution.
* DB-6517 Backup fails if keytab file is a full path name
* DB-6534 Intermittent IT failure in RowCountOperationIT
* DB-6537 Cetera GP 2.5 - Export command exports different number of rows
* DB-6541 User defined function does not work on spark
* DB-6558 HBVOCC: Host CPU Saturation - Heavily loaded RegionServer causing degradation in forward loading throughput from 25K/sec to 4K/sec
* DB-6569 cannot restore database from s3a when using 'SYSCS_UTIL.SYSCS_RESTORE_DATABASE'
* DB-6574 Splice spark adapter still sends write requests to a unreachable region server
* DB-6594 Illegal merge join and NPE for FMC query
* DB-6600 Case When not matching or clause
* DB-6603 Incremental backup changes lost
* DB-6610 Restore completed when replace a data file with an empty file
* DB-6662 Splice Machine Query Timeout Not working
* DB-6684 Build failures because of VM Crash
* DB-6685 After running call syscs_utils.syscs_update_schema_owner() data dictionary cache not invalidated
* DB-6707 Restore should only delete splice tables
* DB-6721 hdp2.5.5 1808 package missing
* DB-6722 Hbase log missing after executing flatten script
* DB-6735 Fix HDP 2.6.3 Service Install now that OLAP Server is in play
* DB-6740 SerDe issue in regression test
* DB-6745 Make sure Spark job resilient to region server failure ("no route to host" exception handling)
* DB-6754 hdp2.6.3 package names are not correct.
* DB-6759 Split key with null column values not correctly encoded
* DB-6778 Fix Null Dereferences in source code
* DB-6790 Database connection corrupted after a SQL parsing error 
* DB-6816 Backup called getAllPartition info twice
* DB-6817 Call syscs_util.syscs_get_version_info() throws exception
* DB-6831 splice groupowner cannot run some operations 
* DB-6833 Index Creation failing
* DB-6839 After Kerberizing SM, running SQL with rand(<colname>) failed with java.lang.NullPointerExceptionXJ001.U
* DB-6853 Remove clear text sensitive information 
* DB-6872 tpch1 timed out after 90 mins on mapr platform
* DB-6875 NPE for select on table with schema change
* SPLICE-1717 Compaction jobs take too long (blocking RPC)
* SPLICE-1870 Incorrect result with update using index
* SPLICE-1900 [External Table]: Incorrect error message while reading data from Empty external table of AVRO file format.
* SPLICE-1927 Restart is freezing on hdp2.5.5
* SPLICE-1995 Bulk import reports wrong number of rows
* SPLICE-2004 Plan dumped in log file is not consistent with what the Explain statement returns
* SPLICE-2012 HMaster doesn't exit after shutdown
* SPLICE-2032 Reenable OlapServerIT when it's more robust
* SPLICE-2057 Killing Spark tasks doesn't relase resources when ResulStreamer is blocked
* SPLICE-2062 External OlapServer is broken in unkerberized clusters
* SPLICE-2063 Exceptions.parseException sometimes removes relevant stack traces
* SPLICE-2066 Duplicate create connection in Splicemachine context
* SPLICE-2067 Exception during External Olap Server initialization
* SPLICE-2092 Bulk import fails after kerberos token expiry
* SPLICE-2094 wrong result with prepare statement that uses multi-probe index scan
* SPLICE-2095 default value for column with fixed char type is not padded with extra spaces
* SPLICE-2112 Configure org.xerial.snappy.Snappy to avoid extracting its native library to /tmp 
* SPLICE-2115 UnsatisfiedLinkError: org.xerial.snappy.SnappyNative.maxCompressedLength on Mac 
* SPLICE-2120 Permissions granted for system procedures become invalid after each system restart
* SPLICE-2134 Column Position Sequencing is 1 greater than it should be, causes column level permissions on views to fail.
* Splice-5 Wrong error message in case of write conflict that results in FK violation

_**Critical Issues**_

* DB-6330 Document new installation procedures on CDH after DB-6066 change
* DB-6599 Clean up maven build profiles
* DB-6619 Test / Verify Splice 2.5 on CDH 5.13.2 - Needed for Anthem
* DB-6679 Merge 2.5.0.1806S branch to branch-2.5 (review/approval/test/merge)
* DB-6743 Remove MapR 5.1 Support: Not tested.
* DB-6825 hdp installation doc for hdp2.6.3 using custom ambari service
* DB-6878 Add support for CDH 5.12.2 for Anthem
* Db-6619 Test / Verify Splice 2.5 on CDH 5.13.2 - Needed for Anthem
* SPLICE-2118 Update source code copyright terms to 2018
* splice-2 Test issue before release .....
* SPLICE-1998 Splice derby timestamp format not compliant with Hadoop (Splice 2.5.x version)


What's New in 2.5.0.1749 - Jan. 19, 2018
------------------------------------------

_**New Features**_

* DB-6140 Support Major Compaction by Region
* DB-6146 Delete data by region without scanning
* DB-6217 Revoke SELECT on column not working
* DB-6428 Selectivity estimate change for parameterized value 
* SPLICE-1617 Index on skewed column - not to load DEFAULT values when creating index (a.k.a., null suppression)
* SPLICE-1700 Add new columns with DEFAULT values without materialization of existing rows
* SPLICE-1802 Create Index on Existing Table Using HFile Bulk Loading
* SPLICE-1920 Tooling to fix data dictionary corruption

_**Improvements**_

* DB-6016 Eliminate unnecessary order by clause
* DB-6165 Speed up hbase table drop and recreation for database restore
* DB-6186 Inadequate Job description in Compaction Jobs
* DB-6202 Backup: read metadata more efficiently
* DB-6332 Relax restriction to allow flattening of non-correlated subquery in where clause
* DB-6346 Allow correlated SSQ flattening for more scenarios
* DB-6362 Large variance in simple select query execution time
* DB-6385 Update 2.5 branch with kafka 0.10.0 to ensure compatibility with kafka streaming client used by customers
* DB-6453 Propagate SORT elimination to outer joins
* SPLICE-1351 Update Data Sketches to 0.8.4
* SPLICE-1372 Control-side query management
* SPLICE-1714 Ignore 'should not give a splitkey which equals to startkey' exception
* SPLICE-1760 Enhancement to provide corresponding Spark JobID when Splice jobs or queries are submitted through Spark
* SPLICE-1785 Too many tasks are launched in the last stage of bulk import
* SPLICE-1852 Make SpliceAdmin_OperationsIT more robust
* SPLICE-1948 Pass the SparkContext from an outside Spark Application into Splice Spark Adapter
* SPLICE-1951 Remove protobuf installation instructions from GETTING-STARTED
* SPLICE-1973 Exclude Kafka jars from splice-uber.jar for all platforms
* SPLICE-1984 Parallelize MultiProbeScan and Union operations

_**Bug Fixes**_

* DB-4300 TPCH query 20 fails
* DB-5914 LIKE query on ACCT_NUM6 - Tablescan is chosen unless Index is hinted
* DB-5953 Database fails to initialize on first boot with Kerberos authentication
* DB-5974 Spark jobs fail to run in multi-Master environment if there is a Master fail-over
* DB-6047 Backup intermittently gives error Java exception: 'org.apache.zookeeper.KeeperException$NotEmptyException: KeeperErrorCode = Directory not empty for /backup'.
* DB-6075 [Kerberos] unable to backup database
* DB-6113 Regression: Backup will fail if someone else tries to run another backup in parallel
* DB-6124 optimizer not choosing Index after upgrading Splice to a version > 2.5.0.1722 and causes query failure / exception on SortMergeJoin
* DB-6127 syscs_util.syscs_delete_old_backups gives ERROR 40XC0: Dead statement.
* DB-6134 Fixed value widths inserted with union 
* DB-6155 Cost estimation difference in plans compared to actual row counts - queries with long IN list
* DB-6157 Error when deleting backup
* DB-6164 NULLS FIRST needs to work fully within window functions
* DB-6167 Regression in query optimization where indexes are not used (2.5.0.1729)
* DB-6183 Clean up rpc for backup
* DB-6187 sqlshell no return
* DB-6191 SYSCS_DELETE_OLD_BACKUPS failed to delete backup stored in S3
* DB-6203 Collected statistics severely worsen query execution time
* DB-6214 SYSCS_UTIL.SYSCS_DELETE_OLD_BACKUPS() should prevent breaking backup chain
* DB-6228 intermittent wrong result for TPCH1 Q9
* DB-6230 API call 'SYSCS_UTIL.SYSCS_RESTORE_DATABASE()' no return
* DB-6231 Optimizer - RKV2 DQ Query Changes: #1 Unable to push down IN-List of ACCTNUMs to MultiProbe scan #2. New plan running large TABLESCAN at Hbase "control"
* DB-6234 Incremental backup failed
* DB-6248 Intermittent failure in DefaultIndexIT.estBulkHFileImport() with NonTransientConnectionException
* DB-6252 Add back synchronous backup/restore
* DB-6267 Modify code using SPLICE_TXN to be resilient to missing data
* DB-6269 Handle transactions not in SPLICE_TXN
* DB-6270 Backup deletes wrong directory for cleanup when it fails 
* DB-6315 Keep alive long running backup
* DB-6349 Splice spark adapter is not allowed to choose SparkDataSetProcessor if it is run by spark_submit
* DB-6350 Fix parquet dependency in splice_machine
* DB-6360 HFile Index creation for AU fact is failing to complete
* DB-6367 Most region based operations throw NPE if schema or table is null
* DB-6373 NPE during bulk delete with Index hint
* DB-6399 Splice spark adapter fails Kerberos authentication
* DB-6408 Backup: no current connection
* DB-6409 Backup: backup cancellation is not checked before backup is launched to spark
* DB-6410 MERGE_DATA_FROM_FILE omits references to SCHEMA NAME
* DB-6411 MERGE_DATA_FROM_FILE encounters ClassCastException
* DB-6412 Outer join's result row count not less than left table 
* DB-6416 Additional cases with region based operations which throw NPE if any parameter is null
* DB-6418 Backup times out
* DB-6438 Standard Queries Fail when used CPD-DT & INDEX in conjunction	
* DBAAS-780 Test full Backup and Restore
* SPLICE-1349 Update query fails with exception
* SPLICE-1813 Transaction are not popped from transaction stack when releasing savepoints
* SPLICE-1850 Couldn't find subpartitions in range exception with external tables
* SPLICE-1860 Error analyzing table when columns contains zero length data
* SPLICE-1867 SHOW TABLES is broken
* SPLICE-1874 Large table scan runs on control with predicate of high selectivity
* SPLICE-1895 CancellationException shows up in log after running query in Spark
* SPLICE-1906 Prevent unexpected exceptions from removing Splice coprocessors
* SPLICE-1921 wrong result with sort merge join that takes spark path
* SPLICE-1928 System procedure get_region() throws ArrayIndexOutOfBounds exception
* SPLICE-1930 Unable to compile spliceengine
* SPLICE-1934 WordUtils.wrap() from commons-lang3 3.5 is broken, affects Splice
* SPLICE-1937 Add support for cdh 5.12.0 platform
* SPLICE-1961 Missing splice_spark module in pom.xml
* SPLICE-1970 Exclude metrics jars from splice-uber.jar to avoid class loader issues when using Spark Adapter
* SPLICE-1971 Spurious jenkins failures due to SpliceAdmin_OperationsIT
* SPLICE-1976 OlapClientTest intermittent failure in Jenkins
* SPLICE-1978 NPE in GET_RUNNING_OPERATIONS
* SPLICE-1983 OOM in Spark executors while running TPCH1 repeatedly
* SPLICE-865 [Authorization] REVOKE after GRANT can throw NullPointerException
* Splice-1784 Query does not scale on 4000 regions

_**Critical Issues**_

* DB-1908 Document STarting/Stopping DB topics in Getting Started
* DB-6042 Cloudera certification requirements for 2.5
* DB-6066 Splice install should not make changes to yarn-site.xml , or add jars in yarn/lib, in shared environment
* DB-6102 Max old Transaction ID needs to be included in backup metadata
* DB-6136 Restore hanging with "Failed to start database 'splice db' " exception, when first restore was aborted and restarted restore.
* DB-6233 Need backup ID/number in the server logs
* DB-6299 TPCH query 21 failure
* DB-6342 Extension of SPLICE-1372 - Centralized monitor/kill query
* DB-6352 Fixing spark2.2 with branch 2.5
* DB-6354 Temporary tables don't go completely away upon session end
* DB-6456 Fix ITs for HDP platform, enable builds in Jenkins
* DB-6484 Regression: class not found error after metrics jar taken out


What's New in 2.5.0.1729 - Aug. 1, 2017
-----------------------------------------------
_**New Features**_

* DB-5831 add a system procedure MERGE_DATA_FROM_FILE to achieve a limited fashion of merge-into
* DB-5875 Implement Kerberos JDBC support (2.5)
* SPLICE-1482 HBase Bulk Import
* SPLICE-1591 Allow Physical Deletes in a Table
* SPLICE-1603 Support sample statistics collection (via Analyze)
* SPLICE-1671 Enable snapshot with bulk load procedure 

_**Improvements**_

* DB-5872 Bcast implementation dataset vs rdd
* DB-5908 prune query blocks based on unsatisfiable conditions
* DB-5933 Add logging to Vacuum process
* DB-5934 Restore breaks transaction semantics(2.5)
* SPLICE-398 Support 'drop view if exists' for 2.5
* SPLICE-774 Support upgrade from K2 (2.5)
* Splice-1479 iterator based stats collection (2.5)
* SPLICE-1500 Skip WAL for unsafe imports (2.5)
* SPLICE-1516 Enable compression for WritePipeline
* SPLICE-1555 Enable optimizer trace info for costing
* SPLICE-1681 Introduce query hint "skipStats"
* SPLICE-1701 JXM mbean server for cache and enginedriver exec service
* SPLICE-1729 Support 'drop table t_name if exists' for 2.5
* SPLICE-1769 Improve distributed boot process
* SPLICE-1756 introduce database property collectIndexStatsOnly to specify the collect stats behavior

_**Bug Fixes**_

* DB-1585 null checking for REGEXP_LIKE
* DB-5860 Resubmit to Spark if we consume too many resouces in control
* DB-5876 Fix a couple issues that cause backup to hang
* DB-5898 Backups block flushes forever if not stopped cleanly
* DB-5900 bind select statement only once in insert into select
* DB-5905 concatenate all iterables at once to avoid stack overflow error
* DB-5910 fix hash join column ordering
* DB-5911 throw BR014 for concurrent backup
* DB-5912 fix incremental backup hang
* DB-5933 Continue processing tables when one doesn't have a namespace
* DB-5943 correct postSplit
* DB-5982 disable dictionary cache for hbase master and spark executor
* DB-5988 Making sure schema is ejected from the cache correctly
* DB-5992 Disable Spark block cache and fix broadcast costing
* DB-5993 Fixing ClosedConnectionException handling
* DB-5998 clean up backup endpoint to avoid hang
* DB-6001 update error message when partial record is found (2.5)
* DB-6008 suppress false constraint violation during retry
* DB-6012 avoid deleting a nonexist snapshot
* DB-6035 cleanup failed backup from old build
* DB-6037 correct a query to find indexes of a table
* DB-6057 Spark job has problems renewing a kerberos ticket
* DB-6060 support ColumnPosition in GroupBy list
* DB-6068 fix wrong result for broadcast with implicit cast from int to numeric type
* DB-6106 Fix limit on multiple partitions on Spark
* SPLICE-77 order by column in subquery not projected should not be resolved
* SPLICE-79 generate correct insert statement to import char for bit column
* SPLICE-612 fix wrong result in right outer join with expression in join condition
* SPLICE-774 Reset statistics during upgrade
* SPLICE-774 Wait for master to clear upgrade znode
* SPLICE-1023 add more info in the message for data import error
* SPLICE-1098 prevent nonnull selectivity from being 0
* SPLICE-1294 Poor Costing when first part of PK is not =
* SPLICE-1395 add a generic error message for import failure from S3
* SPLICE-1423 clean up import error messages for bad file
* SPLICE-1438 fix the explain plan issues
* SPLICE-1443 Skip cutpoint that create empty partitions
* SPLICE-1452 correct cardinality estimation when there is missing partition stats
* SPLICE-1461 Wrap exception parsing against errors
* SPLICE-1469 Set hbase.rowlock.wait.duration to 0 to avoid deadlock
* SPLICE-1470 Make sure user transaction rollbacks on Spark failure
* SPLICE-1473 Allow user code to load com.splicemachine.db.iapi.error
* SPLICE-1478 Fixing Statement Limits
* SPLICE-1497 Add flag for inserts to skip conflict detection
* SPLICE-1526 Handle CodecPool manually to avoid leaking memory
* SPLICE-1533 eliminate duplicates in the IN list
* SPLICE-1541,SPLICE-1543 fix IN-list issues with dynamic bindings and char column
* SPLICE-1559 bulkImportDirectory is case sensitive
* SPLICE-1582 Apply memory limit on consecutive broadcast joins
* SPLICE-1584 fix IndexOutOfBound exception when not all column stats are collected and we try to access column stats for estimation.
* SPLICE-1586 Prevent NPE when Spark job fails
* SPLICE-1589 All transactions are processed by pre-created region 0
* SPLICE-1601 fix wrong result for min/max/sum on empty table without groupby
* SPLICE-1609 normalize row source for split_table_or_index procedure
* SPLICE-1622 Return only latest version for sequences
* SPLICE-1624 Load pipeline driver at RS startup
* SPLICE-1628 HFile bulk load is slow to copy/move HFile to regions
* SPLICE-1637 Enable compression for HFile gen in bulk loader
* SPLICE-1639 Fix NPE due to Spark static initialization missing
* SPLICE-1640 apply memory limit check for consecutive outer broadcast join and derived tables
* SPLICE-1660 Delete Not Using Index Scan due to index columns being required for the scan.
* SPLICE-1675 merge partition stats at the stats collection time
* SPLICE-1682 Perform accumulator check before txn resolution (2.5)
* SPLICE-1684 fix stats collection logic for ArrayIndexOutOfBoundsException in the presence of empty partition and some column stats disabled
* SPLICE-1690 Merge statistics on Spark (2.5)
* SPLICE-1692 Perform (anti)tombstone txn resolution only when needed
* SPLICE-1696 Add ScanOperation and SplcieBaseOperation to Kryo
* SPLICE-1702 refresh/resolve changes with latest master branch
* SPLICE-1737 fix value outside the range of the data type INTEGER error for analyze table statement.
* SPLICE-1749 fix delete over nestedloop join
* SPLICE-1759 HBase Master generates 1.1GB/s of network bandwidth even when cluster is idle
* SPLICE-1781 Fixing Object Creation on IndexTransformFunction
* SPLICE-1782 Code Cleanup on BulkInsertRowIndex
* SPLICE-1784 Fixing Serial Cutpoint Generation
* SPLICE-1791 Make username's more specific to resolve concurrent conflicts
* SPLICE-1792 BroadcastJoinMemoryLimitIT must be executed serially
* SPLICE-1795 fix NullPointerExeption for update with expression, and uncomment test case in HdfsImport related to this bug
* SPLICE-1798 Parallel Queries can fail on SPS Descriptor Update...

New Features in 2.5 GA Release (2.5.0.1707) - Mar. 1 2017
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
<http://doc.splicemachine.com/2.5/Administrators/ReleaseNotes/CurrentReleaseNotes.html>

For a full list of JIRA's for the Community/Open Source software, see <https://splice.atlassian.net>
