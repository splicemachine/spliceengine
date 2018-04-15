Splice Machine 2.7 Release Notes (2.7.0.1814)  
---------------------------------------------

Welcome to 2.7 of Splice Machine! The product is available to build from open source (see https:github.com/splicemachine/spliceengine), as well as prebuilt packages for use on a cluster or cloud.

See the documentation (http://doc.splicemachine.com) for the setup and use of the various versions of the system.

Supported platforms with 2.7:

* Cloudera CDH 5.12.0, 5.12.2, 5.14.0
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

What's New in 2.7.0.1814 - April 6, 2018
-----------------------------------------------


_**New Features**_

* DB-5875 Kerberos-capable JDBC Client
* DB-5964 Add getConnection Method on SpliceMachineContext to control commit mode
* DB-6140 Support Major Compaction by Region
* DB-6146 Delete data by region without scanning
* DB-6217 Revoke SELECT on column not working
* DB-6428 Selectivity estimate change for parameterized value 
* DB-6523 Stable plans for VROL business queries without stats collection on fact tables
* DB-6635 Support Group as the Authenticated User
* DB-6636 Make "splice" user configurable and test it as a group
* DB-6639 Spark Adapter, Import, and Hive Security Model
* DB-6670 Cache Role Grant Permission to avoid frequent data dictionary lookup
* SPLICE-1178 Installer Linkage to Libraries on clusters
* SPLICE-137 Allow Default Role specification to Users, and automatically set on connecting
* SPLICE-1373 Mechanism to log/view all queries run
* SPLICE-1482 HBase Bulk Import
* SPLICE-1603 Support sample statistics collection (via Analyze)
* SPLICE-1617 Index on skewed column - not to load DEFAULT values when creating index (a.k.a., null suppression)
* SPLICE-1669 Bulk delete by loading HFiles
* SPLICE-1671 Enable snapshot with bulk load procedure
* SPLICE-1700 Add new columns with DEFAULT values without materialization of existing rows
* SPLICE-1802 Create Index on Existing Table Using HFile Bulk Loading
* SPLICE-1883 Make the row count threshold to determine spark or control execution path a tunable
* SPLICE-1920 Tooling to fix data dictionary corruption
* SPLICE-2102 Add Stats, truncate, and open ended execute and executeUpdate to Spark Adapter.
* SPLICE-835 TEXT type issue writing data frames through Splice JDBC Driver.
* Splice-1701 Enable monitoring and reporting capability of memory usage for HBase's JVM via JMX

_**Improvements**_

* DB-5818 BackupPartition.restore() destroys Configuration on every invocation
* DB-5908 False predicate query gets executed with left outer join
* DB-6016 Eliminate unnecessary order by clause
* DB-6165 Speed up hbase table drop and recreation for database restore
* DB-6186 Inadequate Job description in Compaction Jobs
* DB-6202 Backup: read metadata more efficiently
* DB-6212 Allow for hint in query to set splitsize (or numsplits)
* DB-6332 Relax restriction to allow flattening of non-correlated subquery in where clause
* DB-6346 Allow correlated SSQ flattening for more scenarios
* DB-6362 Large variance in simple select query execution time
* DB-6453 Propagate SORT elimination to outer joins
* DB-6463  Graceful exit of a query after a timeout
* DB-6478 Add column to indicate 'CONTROL' or 'SPARK' mode for SYSCS_GET_RUNNING_OPERATIONS
* DB-6543 vacuum not handling disabled hbase tables
* DB-6567 Decrease DEFAULT_BROADCAST_REGION_ROW_THRESHOLD
* DB-6582 Inconsistent Export speed 
* DB-6640 LDAP Password show up in Plain Text in Region Server log when Splice is started / Initialized
* DB-6720 Restore: speed up table delete and recreation
* DB-6751 Clean up backup logging for better diagnosability
* DB-6815 Backup: long time to insert into sys.sysbackupitems for 5000+ tables
* SPLICE-1023 Import - Identify the column number that is failing
* SPLICE-1122 Deleting a table needs to remove the pin for the table.
* SPLICE-1290 Insert and InsertAndFetch Need Batch Operations
* SPLICE-1302 Set minimum parallelism for Spark shuffles
* SPLICE-1320 Add support for array data type
* SPLICE-1324 support TINYINT ( in external tables)
* SPLICE-1335 Upgrade Spark to 2.1.0
* SPLICE-1351 Update Data Sketches to 0.8.4
* SPLICE-1359 SanityManager.DEBUG messages create a lot of noise in derby.log
* SPLICE-1360 Add SQLArray Data Type: Basic Serde Functions
* SPLICE-1372 Control-side query management
* SPLICE-1410 DRDAConnThread#parseTimestamp recompiled regular expression
* SPLICE-1424 Remove Expensive Visitor from FromTable
* SPLICE-1453 Incorrect Serde for Statistics Collections for Decimal and Array Types
* SPLICE-1462 Add Mesos Support to Splice Machine Build
* SPLICE-1467 System.out.println Accidentally Included in Optimizer Commit
* SPLICE-1475 Add Spark Streaming to uber jar for DBAAS.
* SPLICE-1480 Allow N Tree Logging
* SPLICE-1481 Unnecessary Interface Modifier
* SPLICE-1489 Make ORC Perform Predicate Pushdown
* SPLICE-1490 Bringing Derby Style Forward
* SPLICE-1491 Remove Array Copy for Key From Insert
* SPLICE-1494 Remove jleach in header and remove dead files
* SPLICE-1512 Multiple Distinct Operations in Aggregates Support
* SPLICE-1517 Vectorized ORC Reader to re-use presto's reader
* SPLICE-1540 Array Operator Node Does not Allow Hashable Joins
* SPLICE-1550 Remove Recursive init calls
* SPLICE-1555 Enable optimizer trace info for costing 
* SPLICE-1561 Spark Executors do not pick up Dictionary Changes, cache needs to be disabled.
* SPLICE-1562 Array Implementation is in db-engine module, needs to be in db-shared
* SPLICE-1563 Remove Dead SpliceParquetVTI
* SPLICE-1565 Move HBaseRowLocation to db-engine
* SPLICE-1568 Core Spark Adapter Functionality With Maven Build
* SPLICE-1587 GRANT ALL PRIVILEGES ON SCHEMA does not include CREATE TABLE
* SPLICE-1591 Allow Physical Deletes in a Table
* SPLICE-1604 Remove Legacy spark-assembly-id in descriptor
* SPLICE-1605 Cleanup RowKeyStatisticsFunction
* SPLICE-1610 Use Patched Version of Spark Mesos to Support CNI
* SPLICE-1611 TPC-C workload causes many prepared statement recompilations
* SPLICE-1619 Upgrade to Spark 2.1.1
* SPLICE-1626 Support CNI for Splice Machine
* SPLICE-1628 HFile bulk load is slow to copy/move HFile to regions
* SPLICE-1637 Compress HFiles in Bulk loader
* SPLICE-1660 Delete via Index (a.k.a. constant delete)
* SPLICE-1666 db-client private methods declared final- redundancy
* SPLICE-1667 removing unnecessary semicolons
* SPLICE-1675 Stats enhancement: merge partition stats once to reduce overhead at query time 
* SPLICE-1681 Introduce Hint to bypass statistics for costing 
* SPLICE-1696 Add ScanOperation and SpliceBaseOperation to Kryo
* SPLICE-1697 Maven Cleanup
* SPLICE-1698 StringBuffer to StringBuilder
* SPLICE-1699 Remove Unused Imports
* SPLICE-1702 Removed LocatedRow Construct From Execution Tree
* SPLICE-1703 size == 0 style change to isEmpty()
* SPLICE-1704 Remove string.equals("") with isEmpty()
* SPLICE-1707 Fix Tail Recursion Issues
* SPLICE-1708 Change Keyset to EntrySet
* SPLICE-1711 Replace concat() with +
* SPLICE-1712 Remove Constant Array Creation
* SPLICE-1713 Unnecessary Interface Modifier 2
* SPLICE-1714 Ignore 'should not give a splitkey which equals to startkey' exception
* SPLICE-1718 TCP keepalives needed
* SPLICE-1719 Bulk Operation can be used instead of Iteration.
* SPLICE-1729 Drop Table If Exists
* SPLICE-1733 Support Inserting Varchar and Char into SmallInt and BigInt Columns
* SPLICE-1744 Vacuum Task Does Not Need Dictionary Understanding...
* SPLICE-1748 Role Descriptor Lookup Not Using Optional Correctly.
* SPLICE-175 Run Olap server in separate YARN instance
* SPLICE-1756 Add a configuration to set default behavior for Analyze collect stats on indexed columns only
* SPLICE-1760 Enhancement to provide corresponding Spark JobID when Splice jobs or queries are submitted through Spark
* SPLICE-1773 Unify Thread Pools and Make Them Monitorable
* SPLICE-1781 Remove Object Creation in IndexTransformFunction
* SPLICE-1782 Cleanup Code Issues on BulkInsertRowIndexGenerationFunction
* SPLICE-1785 Too many tasks are launched in the last stage of bulk import
* SPLICE-1787 Remove Dead Heap and Index Property Creation...
* SPLICE-1788 Mem Database should traverse the same cache as HBase
* SPLICE-1826 Remove Dead Parquet Code
* SPLICE-1830 Remove Dead Merge Partitioner Code
* SPLICE-1831 Remove HBaseBulkLoadReducer
* SPLICE-1832 Remove SpliceRecordConverter
* SPLICE-1833 Remove HostnameUtil class
* SPLICE-1834 Remove EFS FileSystem
* SPLICE-1835 Remove MBeanResultSet
* SPLICE-1836 Remove SpliceCsvTokenizer
* SPLICE-1837 Remove Old Cost Estimate Implementation...
* SPLICE-1838 Remove Left Over Aggregate Plumbing
* SPLICE-1839 Remove Serial Encoding Package
* SPLICE-1840 Remove Dead PhysicalStatsStore
* SPLICE-1841 Remove ScanInfo class and Interfaces
* SPLICE-1842 Derby Utils Dead Code Cleanup
* SPLICE-1845 Tweak Kryo Serde for Missing Elements
* SPLICE-1851 Remove concurrent.traffic package
* SPLICE-1852 Make SpliceAdmin_OperationsIT more robust
* SPLICE-1879 KeyBy Function on Control is a multimap index vs. a map function
* SPLICE-1880 Modify ReduceByKey to execute lazily and not use Multimaps.
* SPLICE-1882 Remove Clones from table scans.
* SPLICE-1896 Primary key Bind Lookup: getSubKeyConstraint is O(n) 
* SPLICE-1925 Remove TXN Lookups from dictionary writes
* SPLICE-1931 [Statement Logging]: Need to improve representation of failed statements in the splice-statment.log
* SPLICE-1936 Improve Sqlshell.sh to accept kerberos properties as argument
* SPLICE-1948 Pass the SparkContext from an outside Spark Application into Splice Spark Adapter
* SPLICE-1951 Remove protobuf installation instructions from GETTING-STARTED
* SPLICE-1973 Exclude Kafka jars from splice-uber.jar for all platforms
* SPLICE-1974 Splice derby timestamp format not compliant with Hadoop
* SPLICE-1975 Allow JavaRDD<Row> to be passed for CRUD operations in SplicemachineContext
* SPLICE-1984 Parallelize MultiProbeScan and Union operations
* SPLICE-1987 Add fully qualified table name into HBASE "TableDisplayName" Attribute 
* SPLICE-1991 Add info to Spark UI for Compaction jobs to indicate presence of Reference Files
* SPLICE-2008 Resolve (roll forward) transactions on flush
* SPLICE-2022 Improve spark job description for compaction
* SPLICE-2081 Excessive creation of Kryo instances
* SPLICE-2083 Remove Dead Folders for non-supported versions
* SPLICE-2085 Remove HDP 2.5.0 and HDP 2.5.3 From Build
* SPLICE-2086 CDH 5.14.0 support
* SPLICE-2096 Role Handling on Connection: Always performs scan
* SPLICE-2100 Fix misspelling in pom files
* SPLICE-2103 ExplainPlan Generation in the Hot Path
* SPLICE-2104 Dictionary Scans Should Use HBase Small Scans vs. the network heavy regular scan.
* SPLICE-2105 Spark Datasource requires PK for Updates and Deletes
* SPLICE-2106 Buffered Aggregators missing from the transient kryo registrator
* SPLICE-2107 SnappyPipelineCompressor generates too much garbage
* SPLICE-2109 SYSCS_UTIL.VACUUM - Add log message in a finally clause to indicate the vacuum job is complete
* SPLICE-398 Drop view if exists functionality
* SPLICE-949 Support Round Function
* SPLICe-1517 Vectorized ORC Reader to re-use presto's reader
* Splice-1479 Make Statistics Collection and Display Iterator Based Vs. Collection Based.
* Splice-1720 Code style clean up
* db-6001 Bulk IMPORT - Print out partial record when encounter newline character in input record

_**Major Bug Fixes**_

* DB-3927 Window Function Frame Processing Error
* DB-4300 TPCH query 20 fails
* DB-5469 Running SpliceFileVTI not working
* DB-5471 Class Not Found creating custom VTI
* DB-5716 [Backup/Restore][Incremental Backup] Unable to select from table after restoring from incremental backup.
* DB-5744 Gather statistics on external tables (ORC) does not work
* DB-5745 'Cannot resolve column name' - Error when selecting against ORC table
* DB-5773 IMS - mergejoin algorithm generates incorrect result in a particular query
* DB-5784 Restore from S3 fails with SQL exception Failed to start database 'splicedb'
* DB-5792 Data file import issues 
* DB-5831 Upsert_file impacts auto-generated column values
* DB-5832 Spark job failing while collecting stats for large external table
* DB-5852 [Backup/Restore] Unabe to restore database from backup
* DB-5860 SELECT DISTINCT goes through CONTROL by default - which crashed Region Server
* DB-5876 Splice Backup procedure hangs
* DB-5905 Cetera: broadcast join failed with StackOverflow
* DB-5910 ClassCastException while using String Functions
* DB-5911 [Backup/Restore] First bakcup (after flatten) is always fails
* DB-5912 [Backup/Restore] Incremental backup freezes
* DB-5914 LIKE query on ACCT_NUM6 - Tablescan is chosen unless Index is hinted
* DB-5933 Vacuum does not work
* DB-5934 Restore breaks transaction semantics
* DB-5950 Insert using SplicemachineContext is failing with class cast exception
* DB-5951 In Zeppelin after interpreter restart splicemachineContext results in InvalidDriver error
* DB-5953 Database fails to initialize on first boot with Kerberos authentication
* DB-5966 RocketFuel - NPE when running customer query
* DB-5974 Spark jobs fail to run in multi-Master environment if there is a Master fail-over
* DB-5982 HFile loading fails after dropping an index
* DB-5988 Schema Owner cannot select data or create tables in their own schema
* DB-5992 INFOR: disable block cache in Spark Executor for 440-way join
* DB-5998 Backup of TPCH100g hangs if backup started without restarting hbase
* DB-6008 FIS POC: Invalid constraint violations while importing data
* DB-6012 Snapshot doesn't exist exception: while taking snapshot 
* DB-6018 ANALYZE TABLE fails with external table (ORC)
* DB-6035 Backup blocking cache flush - can't write index
* DB-6047 Backup intermittently gives error Java exception: 'org.apache.zookeeper.KeeperException$NotEmptyException: KeeperErrorCode = Directory not empty for /backup'.
* DB-6057 Spark job has problems renewing a kerberos ticket
* DB-6068 Get wrong results when using a Broadcast Join
* DB-6075 [Kerberos] unable to backup database
* DB-6106 Insert fails with java.lang.IllegalArgumentException
* DB-6113 Regression: Backup will fail if someone else tries to run another backup in parallel
* DB-6124 optimizer not choosing Index after upgrading Splice to a version > 2.5.0.1722 and causes query failure / exception on SortMergeJoin
* DB-6155 Cost estimation difference in plans compared to actual row counts - queries with long IN list
* DB-6157 Error when deleting backup
* DB-6164 NULLS FIRST needs to work fully within window functions
* DB-6194 After changing DB Property on ANALYZE default, cannot collect stats on "non-indexed cols that are explicitly enabled"
* DB-6203 Collected statistics severely worsen query execution time
* DB-6231 Optimizer - RKV2 DQ Query Changes: #1 Unable to push down IN-List of ACCTNUMs to MultiProbe scan #2. New plan running large TABLESCAN at Hbase "control"
* DB-6234 Incremental backup failed
* DB-6252 Add back synchronous backup/restore
* DB-6262 cluster/RS's do not come up after HBase restart due to existence of SPLICE_INIT
* DB-6267 Modify code using SPLICE_TXN to be resilient to missing data
* DB-6274 Wrong result for TPCH1 Q18
* DB-6278 Capture incremental changes from bulk import
* DB-6284 Backups could be inconsistent
* DB-6286 Query runs slow with correlated scalar subquery(SSQ) in SELECT clause 
* DB-6295 Minor compaction generating a storm of several hundred spark jobs within a very short duration
* DB-6320 Regression on master for update with subquery
* DB-6343 Bulk load transaction was not able to commit
* DB-6345 Left-Outer Join results in NPE during parsing 
* DB-6349 Splice spark adapter is not allowed to choose SparkDataSetProcessor if it is run by spark_submit
* DB-6356 Forward port branch-2.5 decoupling spark jar changes (CDH)
* DB-6367 Most region based operations throw NPE if schema or table is null
* DB-6373 NPE during bulk delete with Index hint
* DB-6391 HFile IMPORT with Sampling ON results in IllegalFormatConversionException
* DB-6399 Splice spark adapter fails Kerberos authentication
* DB-6408 Backup: no current connection
* DB-6409 Backup: backup cancellation is not checked before backup is launched to spark
* DB-6410 MERGE_DATA_FROM_FILE omits references to SCHEMA NAME
* DB-6411 MERGE_DATA_FROM_FILE encounters ClassCastException
* DB-6418 Backup times out
* DB-6438 Standard Queries Fail when used CPD-DT & INDEX in conjunction	
* DB-6440 SELECT with INlist elements and Index hint failed with StackOverflow when executing for 2 partitions
* DB-6516 Spark Streaming with Splice Adapter: long running executors fail with out of memory error after 40+ hours of execution.
* DB-6517 Backup fails if keytab file is a full path name
* DB-6537 Cetera GP 2.5 - Export command exports different number of rows
* DB-6558 HBVOCC: Host CPU Saturation - Heavily loaded RegionServer causing degradation in forward loading throughput from 25K/sec to 4K/sec
* DB-6569 cannot restore database from s3a when using 'SYSCS_UTIL.SYSCS_RESTORE_DATABASE'
* DB-6574 Splice spark adapter still sends write requests to a unreachable region server
* DB-6594 Illegal merge join and NPE for FMC query
* DB-6600 Case When not matching or clause
* DB-6610 Restore completed when replace a data file with an empty file
* DB-6662 Splice Machine Query Timeout Not working
* DB-6684 Build failures because of VM Crash
* DB-6685 After running call syscs_utils.syscs_update_schema_owner() data dictionary cache not invalidated
* DB-6707 Restore should only delete splice tables
* DB-6735 Fix HDP 2.6.3 Service Install now that OLAP Server is in play
* DB-6745 Make sure Spark job resilient to region server failure ("no route to host" exception handling)
* DB-6759 Split key with null column values not correctly encoded
* DB-6816 Backup called getAllPartition info twice
* DB-6831 splice groupowner cannot run some operations 
* DB-6872 tpch1 timed out after 90 mins on mapr platform
* DB-6875 NPE for select on table with schema change
* DBAAS-608 Getting user 'SPLICE' does not have EXECUTE permissions for import
* DBAAS-616 Import : Getting java.sql.SQLException: Data file not found: for BAD file directory.
* DBAAS-780 Test full Backup and Restore

For a full list of JIRA's for the Community/Open Source software, see <https://splice.atlassian.net>
