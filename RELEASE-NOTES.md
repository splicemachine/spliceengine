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
* DB-5964 Add a getConnection Method on SpliceMachineContext
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


_**Critical Issues**_

* DB-1908 Document STarting/Stopping DB topics in Getting Started
* DB-5872 INFOR - very slow 440-way join when using Spark
* DB-5898 Backups block flushes forever if not stopped cleanly
* DB-5942 Remove ConfigMock from EEManagerTest
* DB-5946 Regression:  S3 Import can't load TPCH1 Lineitem table
* DB-6006 URL Required for Spark Notebooks
* DB-6021 Enable TLS for JDBC
* DB-6024 Support AVRO format for external tables.
* DB-6066 Splice install should not make changes to yarn-site.xml , or add jars in yarn/lib, in shared environment
* DB-6102 Max old Transaction ID needs to be included in backup metadata
* DB-6136 Restore hanging with "Failed to start database 'splice db' " exception, when first restore was aborted and restarted restore.
* DB-6233 Need backup ID/number in the server logs
* DB-6247 Spark-2.2 integration to master branch
* DB-6275 Exclude flatten, restart script from 2.6 and master branch as well
* DB-6299 TPCH query 21 failure
* DB-6342 Extension of SPLICE-1372 - Centralized monitor/kill query
* DB-6354 Temporary tables don't go completely away upon session end
* DB-6429 Test coverage for Diablo
* DB-6456 Fix ITs for HDP platform, enable builds in Jenkins
* DB-6484 Regression: class not found error after metrics jar taken out
* DB-6522 Ensure Query Logging shows userid
* DB-6599 Clean up maven build profiles
* DB-6619 Test / Verify Splice 2.5 on CDH 5.13.2 - Needed for Anthem
* DB-6679 Merge 2.5.0.1806S branch to branch-2.5 (review/approval/test/merge)
* DB-6825 hdp installation doc for hdp2.6.3 using custom ambari service
* DB-6878 Add support for CDH 5.12.2 for Anthem
* DBAAS-509 All Compaction Jobs failing on Service?
* DBAAS-890 use newer CDH version (CDH 5.12.0)
* SPLICE-1098 Selectivity 0 problem with all NULL values
* SPLICE-1105 add support for CDH 5.8.5
* SPLICE-1537 testPrimaryKeyMultipleLevelSelectivityIT fails sporadically
* SPLICE-1817 add support for CDH 5.12.0
* SPLICE-1818 fix compilation issues for CDH 5.12.0
* SPLICE-2078 Cleanup logs in master and branch-2.7
* SPLICE-2118 Update source code copyright terms to 2018
* SPLICE-8 CTAS with data : when used with case sensitive table name , loads data into source table instead of target

For a full list of JIRA's for the Community/Open Source software, see <https://splice.atlassian.net>
