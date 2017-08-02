Splice Machine 2.5 Release Notes (2.5.0.1729)  
---------------------------------------------

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
