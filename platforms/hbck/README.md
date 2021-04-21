# Apache HBase HBCK2 Tool

_HBCK2_ is the repair tool for Apache HBase clusters.

hbck2 is the HBase 2.0 replacement for hbck, the HBase metadata repair tool.  This tool is used to repair `hbase:meta`, the HBase metadata table that contains the mappings from regions to regionservers. hbck/hbck2 is not used in normal operations, but is used if `hbase:meta` is damaged in some way. In some circumstances, hbck2 is the only way to recover data stored in HBase.

In cases where `hbase:meta` is so corrupt that hbck2 cannot be used, HBase supports another tool, `OfflineMetaRepair`.  Again, this tool is not used for normal operations.  It is a special-purpose tool needed for particular `hbase:meta` corruption on HBase 1.0.  The 1.0 tool cannot be used against HBase 2.0 files. If it has been used, it causes data corruption which must be repaired using hbck2. There is a version of OfflineMetaRepair for hbase 2.0 that is not available in initial release of HBase 2.0.

If hbase:meta is corrupt, then HBase cannot start. Data stored in HBase, including Splice Machine tables, is not accessible.

## Obtaining _HBCK2_
Releases can be found under the HBase distribution directory. See the
[HBASE Downloads Page](http://hbase.apache.org/downloads.html).

## Building _HBCK2_

Run:
```
$ git clone https://github.com/apache/hbase-operator-tools.git
$ cd hbase-operator-tools
$ mvn clean install
```
The built _HBCK2_ jar will be in the `target` sub-directory of hbase-hbck2.

## Running _HBCK2_

To reassign regions with hbck, where `35cf997c258033655b74ba88e916b455` is the corrupt region

`HBASE_CLASSPATH_PREFIX=/tmp/hbase-hbck2-STABLE.jar hbase org.apache.hbase.HBCK2 -skip assigns 35cf997c258033655b74ba88e916b455`

You can also split the regions into a file and pass the file into the tool

1. Get the list of regionservers and divided them into groups of size X. `split regionlist -l [Size of Group] regionlists`
This results in X-row files regionlistsaa, regionlistsab...  
2. Assign the regions from the files using hbck `hbase hbck -j hbase-hbck2-STABLE.jar assigns -i regionlistsaa`

If you have corrupt metadata and need to run `OfflineMetaRepair` 

1. Find the Hbase region that is corrupt i.e. `35cf997c258033655b74ba88e916b455`
2. Shutdown the cluster
3. Run `HBASE_CLASSPATH_PREFIX=/tmp/hbase-hbck2-STABLE.jar hbase org.apache.hbase.hbck1.OfflineMetaRepair -details`
4. Restart the cluster
5. Run `HBASE_CLASSPATH_PREFIX=/tmp/hbase-hbck2-STABLE.jar hbase org.apache.hbase.HBCK2 -skip assigns 35cf997c258033655b74ba88e916b455`
6. Verify HMaster loaded and all tables are DISABLED (expected)
7. From hbase shell ran enable_all '.*' to enable all tables 

More information on how to use Hbck2 can be found at the [official hbase operator tools repository](https://github.com/apache/hbase-operator-tools/tree/master/hbase-hbck2)