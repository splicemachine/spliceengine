# Current State of Statistics

## Completed

### Framework
+ Synchronous manual collection of all tables and indices in a schema
+ Synchronous manual collection of a table and its indices
+ System tables for storage (sys.systablestats, sys.syscolumnstats, sys.sysphysicalstats)
+ Human-friendly views (sys.systablestatistics, sys.syscolumnstatistics)
+ Statistics Cache for table and partition-level statistics 
+ Integrated into Query optimizer

### Collection
+ Cardinality estimation
+ High-skew frequent elements
+ Row Counts
+ Order statistics
+ Row and Column size
+ local and remote read latency measurement
+ index fetch latency measurement (stored as remote latency on the index table row)

## Missing features
 
### Trivial Changes
+ Asynchronous "fire-and-forget" collection procedure: Submit the collection job and return immediately, without waiting for results (Trivial)
+ Collect for specific columns: Only perform collection for a specific column (or set of columsn) (Trivial)
+ Collect for specific key range: Only re-collect for data within a range of keys (Trivial)
+ Enable/Disable statistics for every column in the table simultaneously (Trivial)

### Easy Changes
+ Stale-partition detection: Where we detect which partitions have enough modifications to reasonably expect statistics are out of date (Easy)
+ Purge old statistics: Purse rows from the statistics table (Easy)
+ Stale-only collection: Collect only the partitions which are known to be stale, instead of every partition (as currently done) (Easy--depends on stale detection)
+ Auto-collect: Automatically initiate asynchronous collection for a partition when the partition is known to be stale (Easy--depends on stale detection)
+ Non-uniform distribution logic (requires Histograms) (Easy)

### Moderate Changes
+ Integration with SYSIBM.SQLSTATISTICS (Moderate)
+ More accurate FrequentElements for some pathological data sets (Moderate)

### Hard Changes
+ Viewing past collections: Seeing what statistics looks like in the past.  (Hard)
+ Invalidate Procedure caches when statistics are collected (hard when taking auto-collect into account) (Hard)
+ Better Join selectivity estimation (requires modification to the optimizer to provide more information) (Hard)
+ Support finer-grained partitioning, so that the entire data set doesn't have to be traversed (LSM-tree sampling? Out-of-band reading of HFiles instead of using HBase scanners?) (Hard)
+ Histograms (Hard)

