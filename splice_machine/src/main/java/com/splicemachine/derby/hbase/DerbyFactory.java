package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.Store.*;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.scheduler.BaseJobControl;
import com.splicemachine.derby.impl.job.scheduler.JobMetrics;
import com.splicemachine.pipeline.api.BulkWritesInvoker;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.mrio.api.MemstoreAware;
import com.splicemachine.mrio.api.SpliceRegionScanner;

public interface DerbyFactory<Transaction> {
		Filter getAllocatedFilter(byte[] localAddress);
		SpliceBaseOperationRegionScanner getOperationRegionScanner(RegionScanner s, Scan scan, HRegion region, TransactionalRegion txnRegion) throws IOException;
		List<HRegion> getOnlineRegions(RegionServerServices services, byte[] tableName) throws IOException;
		void removeTableFromDescriptors(MasterServices masterServices, String tableName) throws IOException;
		HRegionInfo loadRegionInfoFileContent(FileSystem fileSystem, Path path) throws IOException;
		BaseJobControl getJobControl(CoprocessorJob job, String jobPath,SpliceZooKeeperManager zkManager, int maxResubmissionAttempts, JobMetrics jobMetrics);
		void writeScanExternal(ObjectOutput output, Scan scan) throws IOException;
		Scan readScanExternal(ObjectInput in) throws IOException;
		void checkCallerDisconnect(HRegion region) throws IOException;
		InternalScanner noOpInternalScanner();
		void writeRegioninfoOnFilesystem(HRegionInfo regionInfo, Path regiondir, FileSystem fs, Configuration conf) throws IOException;
		Path getRegionDir(HRegion region);
		void bulkLoadHFiles(HRegion region, List<Pair<byte[], String>> paths) throws IOException;
		BulkWritesInvoker.Factory getBulkWritesInvoker(HConnection connection, byte[] tableName);
		long computeRowCount(Logger LOG, String tableName,SortedSet<Pair<HRegionInfo, ServerName>> baseRegions, Scan scan);
		void setMaxCardinalityBasedOnRegionLoad(String tableName, LanguageConnectionContext lcc);
		Filter getSuccessFilter(List<byte[]> failedTasks);
		int getRegionsSizeMB(String tableName);
		Filter getHBaseEntryPredicateFilter(EntryPredicateFilter epf);
		Filter getSkippingScanFilter(List<Pair<byte[], byte[]>> startStopKeys, List<byte[]> predicates);
		HTableInterface getTable(RegionCoprocessorEnvironment rce, byte[] tableName) throws IOException;
		int getReduceNumberOfRegions(String tableName, Configuration conf) throws IOException;
		ConstantAction getDropIndexConstantAction(String fullIndexName, String indexName,String tableName,String schemaName,UUID tableId,long tableConglomerateId);
		void SYSCS_GET_REQUESTS(ResultSet[] resultSet) throws SQLException;
		void SYSCS_GET_SCHEMA_INFO(final ResultSet[] resultSet) throws SQLException;
	    void SYSCS_GET_REGION_SERVER_STATS_INFO(final ResultSet[] resultSet, List<Pair<String, JMXConnector>> connections) throws SQLException;
		ObjectName getRegionServerStatistics() throws MalformedObjectNameException;
		ServerName getServerName(String serverName);
		ExceptionTranslator getExceptionHandler();
        SparkUtils getSparkUtils();
        SpliceRegionScanner getSplitRegionScanner(Scan scan, HTable htable) throws IOException;
        KeyValueScanner getMemstoreFlushAwareScanner(HRegion region, Store store, ScanInfo scanInfo, Scan scan, 
				final NavigableSet<byte[]> columns, long readPt, AtomicReference<MemstoreAware> memstoreAware, MemstoreAware initialValue) throws IOException;
}