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

What's New in 2.5.0.1814 - April 6, 2018
-----------------------------------------------


_**New Features**_

*  DB-5875 Implement Kerberos JDBC support
*  DB-5964 Added SplicemachineContext.getConnection() to enable commit/rollback in Scala
*  DB-6140 fix encoded name for RangedClientPartition
*  DB-6217 make table level select privileges to column level
*  DB-6428 adjust selectivity estimation by excluding skewed default values
*  DB-6523 add hint useDefaultRowCount and defaultSelectivityFactor, als…
*  DB-6635 get the group user when it exists
*  DB-6636 use ldap group to splice user mapping
*  DB-6639 Proxy HDFS access for Spark Adapter apps
*  DB-6670 cache role grant permissions
*  SPLICE-137 allow default roles to be set automatically in a session
*  SPLICE-1373 Extended query logging
*  SPLICE-1482 HBase bulk load
*  SPLICE-1482 fix system procedure to split table and index
*  SPLICE-1603 support sample stats 1. add system procedure collect_table_sample_statistics; 2. collect stats based on the sample percentage specified; 3. introduce two new columns statsType, sampleFraction to the system table sys.systablestats, and to the system view sys.systablestatistics 4. adjust estimation logic to exptrapolate/scale based on the sample fraction 5. add upgrade logic for the addition of two new columns to the dictionary tables,    and to update the view defintiion for systablestatistics 6. add syntax support of sample stats for analyze command
*  SPLICE-1617 support for index exclude null/default keys
*  SPLICE-1669 Support bulk delete.
*  SPLICE-1671 Table/Schema snapshot
*  SPLICE-1700 avoid updating existing physical rows when adding not-null column with default value (master)
*  SPLICE-1802 create index by HFile loading
*  SPLICE-1883 Add rowcount threshold parameter to determine Spark or Control
*  SPLICE-1920 Procedure to delete rows from dictionary tables
*  SPLICE-2102 Stats and truncate functions, a bit of scaladoc writing
*  SPLICE-835 Mapping TEXT column creation to CLOB
*  SPLICE-835 fix ITs
*  Splice-1701 JXM mbean server for cache and enginedriver exec service

_**Improvements**_

*  DB-5818 remove useless code that caused performance problem for restore
*  DB-5908 prune query blocks based on unsatisfiable conditions
*  DB-6001 update the error message when partial record is found
*  DB-6016 avoid sort if some of the index columns are bound with constant.
*  DB-6165 Speed up table drop during database restore
*  DB-6186 Add region name and id to compaction job description
*  DB-6202 Backup - read metadata more efficiently
*  DB-6212 Let the user specify number of splits for a table
*  DB-6332 flatten non-correlated aggregate subquery in where clause
*  DB-6346 allow more scenarios to qualify for CSSQ flattening
*  DB-6362 Large variance in simple select query execution time
*  DB-6453 avoid sort for outer join when applicable
*  DB-6463 Add JDBC timeout support
*  DB-6478 Add result columns SUBMITTED, ENGINE and JOBTYPE in SYSCS_GETRUNNING_OPERATIONS
*  DB-6543 vacuum disabled table
*  DB-6567 Decrease DEFAULT_BROADCAST_REGION_ROW_THRESHOLD
*  DB-6582 improve export performance
*  DB-6582 remove SparkListener for export
*  DB-6640 take out ldap password from logs
*  DB-6720 speed up table drop/recreation during restore
*  DB-6751 Clean up backup logging for better diagnosability
*  DB-6815 bulk insert into sys.sysbackupitems
*  DB-6815:batch insert to sys.sysbaclupitems
*  SPLICE-1023 add more info in the message for data import error
*  SPLICE-1122 Deleting a table needs to remove the pin for the table.
*  SPLICE-1290 Adding Batch Writes to the dictionary
*  SPLICE-1302 Add minimum parallelism for Spark shuffles
*  SPLICE-1320 Fix Assertion Placement and Comment Out test to get build out
*  SPLICE-1324 Making Sure SQLTinyInt can SerDe
*  SPLICE-1335 Upgrade to Spark 2.1.0
*  SPLICE-1351 Upgrade Sketching Library from 0.8.1 - 0.8.4
*  SPLICE-1359 SanityManager.DEBUG messages create a lot of noise in derby.log
*  SPLICE-1360 Adding SQL Array Data Type Basic Serde Functions
*  SPLICE-1372 List and kill running operations
*  SPLICE-1410 Make the compilation of the pattern static
*  SPLICE-1424 Removing Unneeded Visitor from FromTable
*  SPLICE-1453 Fixing Calculating Stats on Array Types
*  SPLICE-1453 Remove Commented Out Code
*  SPLICE-1462 Adding Mesos Scheduling Option to Splice Machine
*  SPLICE-1467 Accidental System.out.println inclusion
*  SPLICE-1475  Add Spark Streaming to uber jar for DBAAS.
*  SPLICE-1480 Allow N Tree Logging
*  SPLICE-1481 Unnecessary Interface Modifier
*  SPLICE-1489 Make Predicate Pushdown defaulted for ORC
*  SPLICE-1490 Bringing Derby Style Forward
*  SPLICE-1491 Remove Array Copy for Key From Insert
*  SPLICE-1494 Remove jleach in header and remove dead files
*  SPLICE-1512 Multiple distinct aggregates
*  SPLICE-1540 Fix ArrayOperatoNode
*  SPLICE-1550 Recursive Init Calls
*  SPLICE-1555 Enable optimizer trace info for costing 	modified   db-engine/src/main/java/com/splicemachine/db/impl/sql/compile/Level2OptimizerTrace.java 	modified   db-engine/src/main/java/com/splicemachine/db/impl/sql/compile/OptimizerImpl.java 	modified   splice_machine/src/main/java/com/splicemachine/derby/impl/sql/compile/SpliceLevel2OptimizerImpl.java
*  SPLICE-1561 Allowing Clients to turn off cache and lazily execute, tests are coming in the spark_adapter commit
*  SPLICE-1562 Moving Array Implementation to db-shared vs. db-engine so the client can have the array code
*  SPLICE-1563 Remove Dead Parquet VTI
*  SPLICE-1565 Moving HBaseRowLocation to Engine for Serde
*  SPLICE-1568 Forgot to move the version from the branch version to the new master version broke after cleaning
*  SPLICE-1587 add modifyPriv for Schema level privilege SPLICE-1587 add authorization check for PIN table statement SPLICE-1587 add IT SPLICE-1587 add upgrade logic
*  SPLICE-1591 purge deleted rows during major compaction
*  SPLICE-1604 Remove Legacy Descriptor
*  SPLICE-1605 Remove Extra Packages
*  SPLICE-1610 Mesos CNI Support
*  SPLICE-1611 TPC-C workload causes many prepared statement recompilations
*  SPLICE-1619 Upgrade to Spark 2.1.1, adding custom mesos patch for named networks
*  SPLICE-1626 Adding CNI Support For Splice Machine
*  SPLICE-1628 Parallelize hstore bulkLoad step in Spark
*  SPLICE-1637 Enable compression for HFile gen in bulk loader
*  SPLICE-1660 Delete Not Using Index Scan due to index columns being required for the scan.  This will happen most of the time when a table has more than one index
*  SPLICE-1666 Cleanup redundant private final
*  SPLICE-1667 Removing unnecessary semicolons
*  SPLICE-1675 merge partition stats at the stats collection time
*  SPLICE-1681 Introduce query hint "skipStats" after a table identifier to bypass fetching real stats from dictionary tables + fix IT related to sample stats and skipStats.
*  SPLICE-1681 Introduce query hint "skipStats" after a table identifier to bypass fetching real stats from dictionary tables.
*  SPLICE-1696 Add ScanOperation and SplcieBaseOperation to Kryo
*  SPLICE-1697 Pom.xml cleanup
*  SPLICE-1698 StringBuffer to StringBuilder
*  SPLICE-1699 Removing Unused Imports
*  SPLICE-1702 refresh/resolve changes with latest master branch
*  SPLICE-1703 replace size() == 0 with isEmpty()
*  SPLICE-1704 Replace double quotes with isEmpty
*  SPLICE-1707 Fix Tail Recursion Issues
*  SPLICE-1708 Do not use KeySet where entryset will work
*  SPLICE-1711 Replace concat with +
*  SPLICE-1712 Remove Constant Array Creation Style
*  SPLICE-1713 Unnecessary interface modifier
*  SPLICE-1714 Ignore 'should not give a splitkey which equals to startkey' exception
*  SPLICE-1718 Fixes TCP/IP Timeout and Sets Default to 60 seconds
*  SPLICE-1719 Bulk Opertion vs. iteration
*  SPLICE-1729 Handle 'drop table table_name if exists'
*  SPLICE-1733 Support type conversion Varchar to INT
*  SPLICE-1744 Removing Dictionary Check
*  SPLICE-1748 Fixing Role Cache Usage
*  SPLICE-175 Kill OlapServerMaster in stop-splice-its script
*  SPLICE-1756 introduce database property collectIndexStatsOnly to specify the collect stats behavior
*  SPLICE-1760 Correlate Spark job id with user session
*  SPLICE-1773 Unifying the thread pools
*  SPLICE-1781 Fixing Object Creation on IndexTransformFunction
*  SPLICE-1782 Code Cleanup on BulkInsertRowIndex
*  SPLICE-1782 i -> j
*  SPLICE-1785 load more regions per task in last stage of bulk import
*  SPLICE-1787 Remove Dead Heap and Index Property Creation
*  SPLICE-1788 Mem Database should traverse the same cache as HBase implementations
*  SPLICE-1826 Remove Dead Parquet Code
*  SPLICE-1830 Remove Dead Merge Partitioner Code
*  SPLICE-1834 Remove Old EFS Filesystem
*  SPLICE-1836 Dead Code
*  SPLICE-1837 Remove Old Cost Estimate Implementation
*  SPLICE-1838 Remove Old Aggregate Plumbing
*  SPLICE-1839 Remove Dead Serializer Plumbing
*  SPLICE-1840 Remove Dead Code
*  SPLICE-1841 Remove ScanInfo class and Interfaces
*  SPLICE-1842 Derby Utils Dead Code Cleanup
*  SPLICE-1845 Tweek Kryo Serde for Missing Elements
*  SPLICE-1851 Remove unused classes that cause test failures
*  SPLICE-1852 Make SpliceAdmin_OperationsIT more robust
*  SPLICE-1879 Modify Key By function to look more like map()
*  SPLICE-1880 Modify ReduceByKey to execute lazily and not use multimaps for cloning optimization coming soon
*  SPLICE-1896 Primary Key Bind Lookup is O(n) where n is the number of primary keys.  Should use scan begin and end key and instead qualifies the record after retrieving from dictionary
*  SPLICE-1925 Remove TXN Lookups from dictionary writes
*  SPLICE-1931 Log prepare statement messages to the Statement logger
*  SPLICE-1936 Refactored and stream-line execute in one place.
*  SPLICE-1948 Increase test timeout
*  SPLICE-1948 Initialize Splice Spark context with user context
*  SPLICE-1948 Resolve conflicts and some IT bugs
*  SPLICE-1951 Remove protobuf installation instructions from GETTING-STARTED
*  SPLICE-1973 Exclude Kafka jars from splice-uber.jar for all platforms
*  SPLICE-1974 Splice derby timestamp format not compliant with Hadoop
*  SPLICE-1975 Allow JavaRDD<Row> to be passed for CRUD operations in
*  SPLICE-1984 Parallelizes MultiProbeTableScan and Union Operatons.  Customers with large number of in list elements or with numerous real-time union operations should see a reduction in execution time
*  SPLICE-1987 Add fully qualified table name into HBASE "TableDisplayName" Attribute
*  SPLICE-1991 Add info to Spark UI for Compaction jobs to indicate presence of Reference Files
*  SPLICE-2008 Resolve transactions during flushes
*  SPLICE-2008 SPLICE-1717 Add null check in txn resolution when restoring
*  SPLICE-2022 Improve the spark job description for compaction
*  SPLICE-2081 Excessive creation of Kryo instances
*  SPLICE-2083 Remove Folders that have already been removed from parent pom
*  SPLICE-2085 Removing very old version of HDP
*  SPLICE-2086 CDH 5.14.0 support (master)
*  SPLICE-2096 Remove Role Scan Hit
*  SPLICE-2100 Fix misspelling in pom files
*  SPLICE-2103 Remove Explain Generation from Hot Path for real-time que…
*  SPLICE-2104 Dictionary Scans Should Use HBase Small Scans vs. the network heavy regular scan.
*  SPLICE-2105 Throw exception if you attempt to delete or update a table without a primary key
*  SPLICE-2106 Fixing Transient Kryo Registrator and adding a few serde tests
*  SPLICE-2109 Add log message in a finally clause to indicate the vacuum job is complete
*  SPLICE-398 Support 'drop view if exists'
*  SPLICE-949 added round to nearest int function, beginning round to chosen decimal place function
*  SPLICe-1517 Orc Reader Additions


_**Critical Issues**_

*  DB-1908 add check for actionAllowed
*  DB-5872 add getBroadcastDatasetCostThreshold to EEManagerTest
*  DB-5898 clean up timeout backup
*  DB-5942 Remove ConfigMock from EEManagerTest
*  DB-5943 correct postSplit
*  DB-5946 Fixing S3 File System Implementation
*  DB-6006 First commit, code cleanup
*  DB-6021 enable jdbc clients like sqlshell.sh and standalone server startup profiles for SSL mode
*  DB-6024 Add avro support
*  DB-6066 build changes for mapr5.2/vanilla yarn support
*  DB-6102 backup/restore old transaction id
*  DB-6136 fix a couple of restore issues
*  DB-6233 add backup id in server log
*  DB-6247 spark2.2 on master branch
*  DB-6275 exclude flatten and restart scripts
*  DB-6299 Synchronize access to shared ArrayList
*  DB-6342 Monitor/kill running operations for all nodes
*  DB-6354 Fix automatic DROP of temp tables
*  DB-6429 test coverage for diablo
*  DB-6456 add missing getEncodedName() method
*  DB-6522 Logging userId when compiling and executing statement
*  DB-6599 Removed out dated profiles. Merge from DB-6599 to master
*  DB-6825 hdp2.6.3 installation doc using custom ambari service
*  DB-6878 Add support for CDH 5.12.2
*  SPLICE-1098 prevent nonnull selectivity from being 0
*  SPLICE-1817 adding support for CDH 5.12.0
*  SPLICE-1818 - fix compilation errors for cdh5.12.0
*  SPLICE-8 make CTAS work with case sensitive names

For a full list of JIRA's for the Community/Open Source software, see <https://splice.atlassian.net>
