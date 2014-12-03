package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.SortedSet;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.scheduler.BaseJobControl;
import com.splicemachine.derby.impl.job.scheduler.JobMetrics;
import com.splicemachine.pipeline.api.BulkWritesInvoker;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceZooKeeperManager;

public interface DerbyFactory<Transaction> {
		Filter getAllocatedFilter(byte[] localAddress);
		SpliceBaseOperationRegionScanner getOperationRegionScanner(RegionScanner s,
																															 Scan scan, HRegion region, TransactionalRegion txnRegion) throws IOException;
		List<HRegion> getOnlineRegions(RegionServerServices services, byte[] tableName) throws IOException;
		void removeTableFromDescriptors(MasterServices masterServices, String tableName) throws IOException;
		HRegionInfo loadRegionInfoFileContent(FileSystem fileSystem, Path path) throws IOException;
		BaseJobControl getJobControl(CoprocessorJob job, String jobPath,SpliceZooKeeperManager zkManager, int maxResubmissionAttempts, JobMetrics jobMetrics);
		void writeScanExternal(ObjectOutput output, Scan scan) throws IOException;
		Scan readScanExternal(ObjectInput in) throws IOException;
		void checkCallerDisconnect(HRegion region) throws IOException;
		InternalScanner noOpInternalScanner();
		void writeRegioninfoOnFilesystem(HRegionInfo regionInfo, Path regiondir,
																		 FileSystem fs, Configuration conf) throws IOException;
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
		ConstantAction getDropIndexConstantAction(String fullIndexName,
																							String indexName,String tableName,String schemaName,UUID tableId,long tableConglomerateId);
		void SYSCS_GET_REQUESTS(ResultSet[] resultSet) throws SQLException;
		void SYSCS_GET_SCHEMA_INFO(final ResultSet[] resultSet) throws SQLException;
		ServerName getServerName(String serverName);

		ExceptionTranslator getExceptionHandler();

}