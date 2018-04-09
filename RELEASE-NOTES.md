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

*  DB-6140 fix encoded name for RangedClientPartition
*  DB-6146 delete region
*  DB-6217 add ITs for column and table level privileges. make table level select privileges to column level
*  DB-6428 adjust selectivity estimation by excluding skewed default values
*  DB-6523 add hint useDefaultRowCount and defaultSelectivityFactor, als…
*  DB-6635 Support ldap group as authenticated user
*  DB-6636 add configurable setting to map ldap group to splice user
*  DB-6639 Proxy HDFS access for Spark Adapter apps
*  DB-6670 cache role grant permissions
*  SPLICE-137 allow default roles to be set automatically in a session
*  SPLICE-1617 support for index exclude null/default keys
*  SPLICE-1700 avoid updating existing physical rows when adding not-nul…
*  SPLICE-1802 create index by HFile loading
*  SPLICE-1883 Add rowcount threshold parameter to determine Spark or Control
*  SPLICE-1920 Procedure to delete rows from dictionary tables
*  SPLICE-2102 Stats and truncate functions, a bit of scaladoc writing

_**Improvements**_

*  DB-6016 avoid sort if some of the index columns are bound with constant.
*  DB-6165 Speed up table drop during database restore
*  DB-6186 Add region name and id to compaction job description
*  DB-6202 Backup - read metadata more efficiently
*  DB-6332 flatten non-correlated aggregate subquery in where clause
*  DB-6346 flatten correlated Scalar Subquery in Select clause
*  DB-6362 Large variance in simple select query execution time
*  DB-6385 update branch with kafka 0.10.0 for cdh5.8.3
*  DB-6453 avoid sort for outer join when applicable
*  DB-6463 Add JDBC timeout support
*  DB-6478 Add result columns SUBMITTED, ENGINE and JOBTYPE in SYSCS_GET_RUNNING_OPERATIONS / LOCAL
*  DB-6543 vacuum disabled table
*  DB-6567 Decrease DEFAULT_BROADCAST_REGION_ROW_THRESHOLD
*  DB-6582 improve export performance
*  DB-6640 avoid ldap passwords while dumping all properties
*  DB-6720 bigger pool size
*  DB-6720 speed up table drop/recreation during restore
*  DB-6751 Clean up backup logging for better diagnosability
*  DB-6815 batch insert to sys.sysbaclupitems
*  SPLICE-1351 Upgrade Sketching Library from 0.8.1 - 0.8.4
*  SPLICE-1372 List and kill running operations
*  SPLICE-1587 add modifyPriv for Schema level privilege SPLICE-1587 add authorization check for PIN table statement SPLICE-1587 add IT SPLICE-1587 add upgrade logic
*  SPLICE-1587 change the version threshold to trigger the upgrade script from 2.7.1 to 2.5.1 to avoid confusion
*  SPLICE-1714 Ignore 'should not give a splitkey which equals to startkey' exception
*  SPLICE-175 External Olap server. Kill OlapServerMaster in stop-splice-its script
*  SPLICE-1760 Correlate Spark job id with user session
*  SPLICE-1785 load more regions per task in last stage of bulk import
*  SPLICE-1852 Make SpliceAdmin_OperationsIT more robust
*  SPLICE-1936 Refactored and stream-line execute in one place.
*  SPLICE-1948 Initialize Splice Spark context with user context
*  SPLICE-1951 Remove protobuf installation instructions from
*  SPLICE-1973 Exclude Kafka jars from splice-uber.jar for all platforms
*  SPLICE-1984 Parallelizes MultiProbeTableScan and Union Operatons.  Customers with large number of in list elements or with numerous real-time union operations should see a reduction in execution time
*  SPLICE-1987 Add fully qualified table name into HBASE "TableDisplayName" Attribute
*  SPLICE-1991 Add info to Spark UI for Compaction jobs to indicate presence of Reference Files
*  SPLICE-2008 Resolve transactions during flushes
*  SPLICE-2022 Improve the spark job description for compaction
*  SPLICE-2081 Excessive creation of Kryo instances
*  SPLICE-2083 Remove Folders that have already been removed from parent pom
*  SPLICE-2096 Remove Role Scan Hit
*  SPLICE-2103 Remove Explain Generation from Hot Path for real-time que…
*  SPLICE-2104 Dictionary Scans Should Use HBase Small Scans vs. the network heavy regular scan.
*  SPLICE-2105 Throw exception if you attempt to delete or update a table without a primary key
*  SPLICE-2106 Fixing Transient Kryo Registrator and adding a few serde tests
*  SPLICE-2109 Add log message in a finally clause to indicate the vacuum job is complete

_**Bug Fixes**_

*  DB-4300 enhance subquery processing logic to convert eligible where s…
*  DB-5914 fix inconsistent selectivity with predicate order change
*  DB-5953 Allow first connection on Kerberized cluster
*  DB-5974 Reconnect to OlapServer in case of failure
*  DB-6047 create child persistent node to avoid ZOOKEEPER-2052
*  DB-6075 Kerberos support for backup/restore
*  DB-6113 fix backup concurrency control regression
*  DB-6124 fix conglomerate average row width estimation, also use localcost to prune join plan search space
*  DB-6134 change resultant datatype to varchar if union of char types with different length
*  DB-6155 adjust selectivity estimation logic for range condition
*  DB-6157 fix null pointer exception when deleting a backup
*  DB-6164 honor Null ordering specification in Spark and Control path for Window function
*  DB-6167 fix maximum version to trigger the 2.0 to 2.5 upgrade script K2UpgradeScript()
*  DB-6183 clean up rpc for backup
*  DB-6187 run backup asynchronously
*  DB-6191 delete old backups from S3
*  DB-6194 add system procedure to enable/disable all column statistics
*  DB-6203 honor Spark dataset processor once picked, and fix regression in SPLICE-1874
*  DB-6214 reserve a valid backup chain when deleting old backups
*  DB-6228 Add logs around task failure in Spark
*  DB-6230 run restore asynchronously
*  DB-6231 enhance MultiProbeIndexScan to apply for cases where inlist is not the leading index/PK column.
*  DB-6234 add more logging. clean up backup metadata
*  DB-6248 move the bulkimport/bulkdelete test in DefaultIndexIT to a separate file under hbase_sql so it won't run for mem platform
*  DB-6252 add back synchronous backup/restore
*  DB-6262 Improve failure handling during initialization
*  DB-6267 Add parameter to ignore missing transactions from SPLICE_TXN
*  DB-6269 Handle transactions that are not in SPLICE_TXN
*  DB-6270 Backup deletes wrong directory for cleanup when it fails
*  DB-6277 clean up incremental changes from offline regions that were offline before full backup.
*  DB-6278 incremental backup for bulk import
*  DB-6284 add ignore txn cache. Ignore txn started after backup
*  DB-6295 Add maximum concurrent compactions parameter
*  DB-6315 keep alive backup
*  DB-6343 Handle CannotCommit exception correctly
*  DB-6345 fix query failure with non-covering index as right table of a…
*  DB-6349 allow distributed execution for splice client
*  DB-6350 Fix parquet dependency in splice_machine
*  DB-6360 avoid index creation timeout
*  DB-6367 null checking for region operations
*  DB-6373 set rowlocation for index lookup
*  DB-6391 fix HFile import logging
*  DB-6399 Broadcast Kerberos tokens to executors instead of keytabs
*  DB-6408 add more logging for trouble shooting. use nested connection for backup
*  DB-6409 terminate backup timely if it has been cancelled
*  DB-6410 prepend schemaname to columns in PK conditions in update statement genereated by MERGE_DATA_FROM_FILE
*  DB-6411 fix column misalignment for join update through mergesort join. Make UpdateFromSubqueryIT sequential to avoid concurrent update on the same table and lead to non-deterministic result
*  DB-6412 Bound the outer join row count by the left outer table's row count
*  DB-6416 parameter sanity check for region operations
*  DB-6438 populate default value for column of DATE type in defaultRow with the right type (regression fix for SPLICE-1700)
*  DB-6440 reduce lengthy lineage of transformation for MultiProbeTableScan operation
*  DB-6464 set isOpen to true for an operation for Spark execution
*  DB-6511 Discrepancy between internal and external Nexus repos. Added public repository in distributionManagement.
*  DB-6516 disable dependency manager for spark
*  DB-6517 get keytab file name correctly
*  DB-6534 Close FutureIterator to avoid race condition
*  DB-6537 fix upsert index
*  DB-6541 User defined function does not work on spark
*  DB-6558 cache more database properties
*  DB-6569 restore from s3 regression
*  DB-6574 retry write workload on host unreachable exceptions
*  DB-6594 fix illegal merge join
*  DB-6595 Script to check mismatched build numbers in pom.xml files
*  DB-6600 Case When Not Matching or Clause
*  DB-6603 incremental restore set timestamp from latest incremental backup. prevent incremental backup from losing changes
*  DB-6610 log warning messages if backup is corrupted
*  DB-6662 Make QueryTimeout more robust
*  DB-6684 fixed ZooKeeperServerMain arguments
*  DB-6685 invalidate dictionary cache after update schema owner DB-6685 address review comments by adding IT for multiple region servers and invalidate nameTdCache too
*  DB-6707 restore only deletes splice table
*  DB-6721 hdp2.5.5/mapr5.2.0 packages missing
*  DB-6722 Fixing Log4j inclusion in hbase_sql jar
*  DB-6735 Fixing HDP2.6.3 build to support OLAP Server properly
*  DB-6740 fix SerDe issue for WindowFunctionInfo
*  DB-6745 invalidate HBase connection cache on retry
*  DB-6754 Add build number for hdp2.6.3
*  DB-6759 encode split key with null value correctly
*  DB-6778 Fix null Dereferences
*  DB-6790 Handling Throwable SQLException
*  DB-6816 cache region partition info
*  DB-6817 Add Information to HBASE_REGIONSERVER_OPTS
*  DB-6831 add checks for groupuser for grant operation
*  DB-6833 fix compilation error for mem platform
*  DB-6839 fix NPE when calling system functions from a user without default schema
*  DB-6853 Remove clear text sensitive information
*  DB-6872 Set hbase.rootdir in all platforms
*  DB-6875 properly compute the referencd columns for index lookup operation
*  SPLICE-1349 serialize and initialize BatchOnceOperation correctly
*  SPLICE-1717 Asynchronous transaction resolution in compactions. Moved executorService into SIDriver
*  SPLICE-1813 Remove transactions from the savepoint stack
*  SPLICE-1850 choose dataset impl for broadcast join of RHS is an external table
*  SPLICE-1860 fix analyze on varchar column with emptystring
*  SPLICE-1867 honor scan column list to avoid decoding column with serialized stored procedure
*  SPLICE-1870 fix update through index lookup path
*  SPLICE-1874 determine Spark path based on output row count computed using predicate against key value.
*  SPLICE-1895 Wait for wrap up before closing remote query client
*  SPLICE-1900 Incorrect error message while reading data from Empty external table of AVRO file format.
*  SPLICE-1906 Make coprocessors throw only IOExceptions
*  SPLICE-1921 fix wrong result with sort merge inclusion join for spark path
*  SPLICE-1927 Amend pattern string for detecting splice machine ready to accept connections
*  SPLICE-1928 decode region start/end key by fetching actual rowkey
*  SPLICE-1930 Fixes an issue where maven uses platform installed protobuf
*  SPLICE-1934 WordUtils.wrap() from commons-lang3 3.5 is broken
*  SPLICE-1961 Missing splice_spark module in pom.xml
*  SPLICE-1970 Exclude metrics jars from splice-uber jar to avoid class
*  SPLICE-1971 Wrap SqlException into StandardException
*  SPLICE-1978 Add null check to GET_RUNNING_OPERATIONS
*  SPLICE-1983 OOM in Spark executors while running TPCH1 repeatedly
*  SPLICE-1995 correct imported rows
*  SPLICE-2004 fix inconsistency between plan logged in log and that in the explain
*  SPLICE-2012 HMaster doesn't exit after shutdown
*  SPLICE-2057 Release blocked thread if Spark task is killed
*  SPLICE-2062 Set standalone yarn user to logged in user
*  SPLICE-2062 Set yarn user without affecting Spark
*  SPLICE-2063 Log original stack trace when parsing exception
*  SPLICE-2066 Duplicate create connection in Splicemachinecontext
*  SPLICE-2067 Fix for Client cannot authenticate via:[TOKEN, KERBEROS]
*  SPLICE-2092 Relogin from keytab on OlapServerMaster when needed
*  SPLICE-2094 fix wrong result with Multi-Probe scan under prepare mode
*  SPLICE-2095 pad default values with extra space if it is shorter than the column size of fixed-length char type
*  SPLICE-2120 update SYS.SYSROUTINEPERMS when upgrading system procedures
*  SPLICE-2134 Fixing Column Sequencing for views, issue shows up with column level permissions on views
*  SPLICE-865  Check if enterprise version is activated and if the user try to use column privileges.
*  Splice-1784 Query does not scale on 4000 regions

_**Critical Issues**_

*  DB-1908 check for actionAllowed
*  DB-6042 exclude flatten and restart scripts
*  DB-6066 build changes for mapr5.2/vanilla yarn support
*  DB-6102 backup/restore old transaction id
*  DB-6136 fix a couple of restore issues
*  DB-6233 add backup id in server log
*  DB-6299 Synchronize access to shared ArrayList
*  DB-6330 updated cdh installation guide,change olapserver memory setting
*  DB-6342 Disable test for mem profile. Monitor/kill running operations for all nodes
*  DB-6352 integrate spark2.2 changes on branch-2.5
*  DB-6354 Fix automatic DROP of temp tables
*  DB-6456 add missing getEncodedName() method
*  DB-6484 exclude metrics jar for cdh platforms
*  DB-6619 Splice 5.13.2 Platform Support
*  DB-6743 Remove mapr5.1.0
*  DB-6825 using custom ambari service to install splicemachine
*  SPLICE-1998 Modify splice 2.5 log file's time stamp format to ISO8601


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
