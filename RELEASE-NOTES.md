Splice Machine 2.6 GA Release Notes (2.6.0.1729)
------------------------------------------------

Welcome to 2.6 releaes of Splice Machine! The product is available to build from open source (see https:github.com/splicemachine/spliceengine), as well as prebuilt packages for use on a cluster or cloud.

See the documentation (http://doc.splicemachine.com) for the setup and use of the various versions of the system.

Supported platforms with 2.6:

* Cloudera CDH 5.8.0, 5.8.3
* MapR 5.1.0 and 5.2.0
* HortonWorks HDP 2.5 and 2.5.5

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


What's New in 2.6 GA Release (2.6.0.1729) - Aug. 1 2017
-------------------------------------------------------
_**New Features**_

* DB-5831 add a system procedure MERGE_DATA_FROM_FILE to achieve a limited fashion of merge-into
* DB-5875 Implement Kerberos JDBC support
* DB-4293 Support BLOB/CLOB in ODBC
* DB-5887 HAProxy and connection load balancing solution
* SPLICE-879 VTI Support for Hive ORC Files
* SPLICE-1320 Add Support For Array Data Type
* SPLICE-1482 HBase Bulk Import
* SPLICE-1512 Multiple Distinct Operations in Aggregates Support
* SPLICE-1591 Allow Physical Deletes in a Table
* SPLICE-1603 Support sample statistics collection (via Analyze)
* SPLICE-1669 Bulk delete by loading HFiles
* SPLICE-1671 Enable snapshot with bulk load procedure
* SPLICE-1701 Enable monitoring and reporting capability of memory usage for HBase's JVM via JMX


_**Improvements**_

* DB-5964 Added SplicemachineContext.g
* DB-5872 Bcast implementation dataset vs rddetConnection() to enable commit/rollback in Scala
* DB-5872 Bcast implementation dataset vs rdd
* DB-5933 Add logging to Vacuum process
* SPLICE-398 Support 'drop view if exists'
* SPLICE-1222 Implement in-memory subtransactions
* SPLICE-1351 Upgrade Sketching Library from 0.8.1 - 0.8.4
* SPLICE-1372 Control-side query control
* Splice-1479 iterator based stats collection
* SPLICE-1497 Add flag for inserts to skip conflict detection
* SPLICE-1500 Skip WAL for unsafe imports
* SPLICE-1513 Create Spark Adapter that supports both 1.6.x and 2.1.0 versions of Spark
* SPLICE-1516 Enable compression for WritePipeline
* SPLICe-1517 Orc Reader Additions
* SPLICE-1555 Enable optimizer trace info for costing
* SPLICE-1568 Core Spark Adapter Functionality With Maven Build
* SPLICE-1619 Update the Spark Adapter to 2.1.1
* SPLICE-1681 Introduce query hint "skipStats" after a table identifier to bypass fetching real stats from dictionary tables
* SPLICE-1729 Handle 'drop table table_name if exists'
* SPLICE-1733 Support type conversion Varchar to INT
* SPLICE-1739 Added CREATE SCHEMA IF NOT EXISTS functionality
* SPLICE-1752 Support inserting int types to char types
* SPLICE-1756 introduce database property collectIndexStatsOnly to specify the collect stats behavior


_**Bug Fixes**_

* DB-5471 Allow more packages to be loaded from user code
* Db-5539 drop and re-create foreign key write handler after truncating a table
* DB-5716 name space null check
* DB-5743 fix table number to allow predicate push down
* DB-5744 remove check for collecting schema level stats for external table
* DB-5773 Explicitly unset ordering
* DB-5789 redo nested connection on Spark fix
* DB-5792 avoid bad file naming collision
* DB-5806 allow inner table of broadcast join to be any FromTable
* DB-5832 fix stat collection on external table textfile
* DB-5860 Resubmit to Spark if we consume too many resouces in control
* DB-5876 Fix a couple issues that cause backup to hang
* DB-5898 clean up timeout backup
* DB-5900 bind select statement only once in insert into select
* DB-5905 concatenate all iterables at once to avoid stack overflow error
* DB-5908 prune query blocks based on unsatisfiable conditions
* DB-5910 fix hash join column ordering
* DB-5911 throw BR014 for concurrent backup
* DB-5912 fix incremental backup hang
* DB-5920 restore cleanup
* DB-5933 Continue processing tables when one doesn't have a namespace
* DB-5934 restore a chain of backup(2.5)
* DB-5943 correct postSplit
* DB-5946 Fixing S3 File System Implementation
* DB-5950 Allowing SpliceClient.isClient to allow distributed execution for inserts
* DB-5951 Fix Driver Loading in Zeppelin where it fails initially
* DB-5966 fix a problem while reading orc byte stream
* DB-5982 disable dictionary cache for hbase master and spark executor
* DB-5986 Fix Orc Partition Pruning
* DB-5988 Making sure schema is ejected from the cache correctly
* DB-5992 Disable Spark block cache and fix broadcast costing
* DB-5993 Fixing ClosedConnectionException
* DB-5998 clean up backup endpoint to avoid hang
* DB-6001 update the error message when partial record is found
* DB-6008 suppress false constraint violation during retry
* DB-6012 avoid deleting a nonexist snapshot
* DB-6018 keep the column indexes zero based for new orc stats collection job
* DB-6037 correct a query to find indexes of a table
* DB-6057 Spark job has problems renewing a kerberos ticket
* DB-6060 support ColumnPosition in GroupBy list
* DB-6068 fix wrong result for broadcast with implicit cast from int to numeric type
* DB-6106 Fix limit on multiple partitions on Spark
* SPLICE-8 make CTAS work with case sensitive names
* SPLICE-57 drop backing index and write handler when dropping a unique constraint
* SPLICE-77 order by column in subquery not projected should not be resolved
* SPLICE-79 generate correct insert statement to import char for bit column
* SPLICE-612 fix wrong result in right outer join with expression in join condition
* SPLICE-737 getTableNumber should return -1 if it cannot be determined
* SPLICE-835 Mapping TEXT column creation to CLOB
* SPLICE-865 Check if enterprise version is activated and if the user try to use column privileges.
* SPLICE-976 do not allow odbc and jdbc requests other than splice driver
* SPLICE-1023 add more info in the message for data import error
* SPLICE-1062 retry if region location cannot be found
* SPLICE-1098 prevent nonnull selectivity from being 0
* SPLICE-1116 Fixing OrderBy Removal from set operations since they are now hashed based
* SPLICE-1122 Deleting a table needs to remove the pin for the table.
* SPLICE-1160 report syntax error instead of throwing NPE for topN in set operation
* SPLICE-1208 fix mergeSortJoin overestimate by 3x
* SPLICE-1211 open and close latency calculated twice for NLJ
* SPLICE-1290 Adding Batch Writes to the dictionary
* SPLICE-1294 Poor Costing when first part of PK is not =
* SPLICE-1320 Fix Assertion Placement and Comment Out test to get build out
* SPLICE-1323 add null check for probevalue
* SPLICE-1324 Making Sure SQLTinyInt can SerDe
* SPLICE-1329 Memory leak in SpliceObserverInstructions
* SPLICE-1349 serialize and initialize BatchOnceOperation correctly
* SPLICE-1353 export to S3
* SPLICE-1357 Fix wrong result for right outer join that is performed through spark engine
* SPLICE-1358  CREATE EXTERNAL TABLE can failed with some specific users in hdfs,
* SPLICE-1359 SanityManager.DEBUG messages create a lot of noise in derby.log
* SPLICE-1360 Adding SQL Array Data Type Basic Serde Functions
* SPLICE-1361 Kerberos keytab not picked up by Spark on Splice Machine 2.5/2.6
* SPLICE-1362 synchronize access to internalConnection's contextManager
* SPLICE-1369 store external table on S3
* SPLICE-1370 INSERT, UPDATE, DELETE error message for pin tables
* SPLICE-1374 bad file in S3
* SPLICE-1375 Fix concurrency issues reporting failedRows
* SPLICE-1379 The number of threads in the HBase priority executor is inadequately low
* SPLICE-1386 load jar file from S3
* SPLICE-1395 add a generic error message for import failure from S3
* SPLICE-1410 Make the compilation of the pattern static
* SPLICE-1411 resolve over clause expr when alias from inner query is used in window fn
* SPLICE-1423 add null check for bad file directory
* SPLICE-1424 Removing Unneeded Visitor from FromTable
* SPLICE-1425 Fix data type inconsistencies with unary functions and external table based on TEXT data format
* SPLICE-1430 remove pin from dictionary and rely on spark cache to get the status of pins & Fix race condition on OlapNIOLayer
* SPLICE-1433 Fix drop pinned table
* SPLICE-1438 fix the explain plan issues
* SPLICE-1443 Add logging to debug "can't find subpartitions" exception
* SPLICE-1443 Skip cutpoint that create empty partitions
* SPLICE-1446 Check schema for ext table only if there's data
* SPLICE-1448 Make sure SpliceSpark.getContext/Session isn't misused
* SPLICE-1452 correct cardinality estimation when there is missing partition stats
* SPLICE-1453 Fixing Calculating Stats on Array Types
* SPLICE-1461 Add null check on exception parsing
* SPLICE-1461 Wrap exception parsing against errors
* SPLICE-1462 Adding Mesos Scheduling Option to Splice Machine
* SPLICE-1463 Sort results in MemStoreKVScanner when needed
* SPLICE-1464 bypass schema checking for csv file
* SPLICE-1469 Set hbase.rowlock.wait.duration to 0 to avoid deadlock
* SPLICE-1470 Make sure user transaction rollbacks on Spark failure
* SPLICE-1473 Allow user code to load com.splicemachine.db.iapi.error
* SPLICE-1478 Fixing Statement Limits
* Splice-1479 iterator based stats collection
* SPLICE-1480 Allow N Tree Logging
* SPLICE-1481 Unnecessary Interface Modifier
* SPLICE-1489 Make Predicate Pushdown defaulted for ORC
* SPLICE-1490 Bringing Derby Style Forward
* SPLICE-1491 Remove Array Copy for Key From Insert
* SPLICE-1526 Handle CodecPool manually to avoid leaking memory
* SPLICE-1531 fixed ThreadLocal in AbstractTimeDescriptorSerializer
* SPLICE-1533 eliminate duplicates in the IN list
* SPLICE-1541,1543 fix IN-list issues with dynamic bindings and char column
* SPLICE-1550 Recursive Init Calls
* SPLICE-1559 bulkImportDirectory is case sensitive
* SPLICE-1561 Allowing Clients to turn off cache and lazily execute
* SPLICE-1567 set remotecost for merge join
* Splice-1578 Upgrade from 2.5 to 2.6
* SPLICE-1582 Apply memory limit on consecutive broadcast joins
* SPLICE-1584 fix IndexOutOfBound exception when not all column stats are collected and we try to access column stats for estimation.
* SPLICE-1586 Prevent NPE when Spark job fails
* SPLICE-1589 All transactions are processed by pre-created region 0
* SPLICE-1597 Fix issue with cache dictionary when  SYSCS_UTIL.SYSCS_UPDATE_SCHEMA_OWNER is called .
* SPLICE-1601 fix wrong result for min/max/sum on empty table without groupby
* SPLICE-1609 normalize row source for split_table_or_index procedure
* SPLICE-1611 TPC-C workload causes many prepared statement recompilations
* SPLICE-1613 Ignore saveSourceCode IT for now
* SPLICE-1621 Fix select from partitioned orc table error
* SPLICE-1622 Return only latest version for sequences
* SPLICE-1624 Load pipeline driver at RS startup
* SPLICE-1628 Don't raise exception if path doesn't exist
* SPLICE-1628 Parallelize hstore bulkLoad step in Spark
* SPLICE-1637 Enable compression for HFile gen in bulk loader
* SPLICE-1639 Fix NPE due to Spark static initialization missing
* SPLICE-1640 apply memory limit check for consecutive outer broadcast join and derived tables
* SPLICE-1660 Delete Not Using Index Scan due to index columns being required for the scan. 
* SPLICE-1675 merge partition stats at the stats collection time
* SPLICE-1682 Perform accumulator check before txn resolution
* SPLICE-1684 fix stats collection logic for ArrayIndexOutOfBoundsException in the presence of empty partition and some column stats disabled
* SPLICE-1690 Merge statistics on Spark
* SPLICE-1696 Add ScanOperation and SplcieBaseOperation to Kryo
* SPLICE-1698 StringBuffer to StringBuilder
* SPLICE-1699 Removing Unused Imports
* SPLICE-1703 replace size() == 0 with isEmpty()
* SPLICE-1704 Replace double quotes with isEmpty
* SPLICE-1707 Fix Tail Recursion Issues
* SPLICE-1708 Do not use KeySet where entryset will work
* SPLICE-1711 Replace concat with +
* SPLICE-1712 Remove Constant Array Creation Style
* SPLICE-1737 fix value outside the range of the data type INTEGER error for analyze table statement.
* SPLICE-1744 Removing Dictionary Check
* SPLICE-1748 Fixing Role Cache Usage
* SPLICE-1749 fix delete over nestedloop join
* SPLICE-1759 HBase Master generates 1.1GB/s of network bandwidth even when cluster is idle
* SPLICE-1769 Improve distributed boot process
* SPLICE-1773 Unifying the thread pools
* SPLICE-1781 Fixing Object Creation on IndexTransformFunction
* SPLICE-1784 Fixing Serial Cutpoint Generation
* SPLICE-1791 Make username's more specific to resolve concurrent conflicts
* SPLICE-1792 BroadcastJoinMemoryLimitIT must be executed serially
* SPLICE-1795 fix NullPointerExeption for update with expression, and uncomment test case in HdfsImport related to this bug
* SPLICE-1798 Parallel Queries can fail on SPS Descriptor Update...
* SPLICE-1824 NullPointer when collecting stats on ORC table



Known issues and workarounds in 2.6 Release
-----------------------------------------------
Known issues and workarounds can be found at:
<http://doc.splicemachine.com/onprem_info_release.html>

For a full list of JIRA's for the Community/Open Source software, see <https://splice.atlassian.net>


