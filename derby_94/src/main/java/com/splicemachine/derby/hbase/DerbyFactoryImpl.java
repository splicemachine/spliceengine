package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.job.scheduler.JobControl;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.scheduler.BaseJobControl;
import com.splicemachine.derby.impl.job.scheduler.JobMetrics;
import com.splicemachine.derby.impl.sql.execute.operations.SkippingScanFilter;
import com.splicemachine.derby.impl.store.access.base.SpliceGenericCostController;
import com.splicemachine.hase.debug.HBaseEntryPredicateFilter;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.hbase.ThrowIfDisconnected;
import com.splicemachine.pipeline.api.BulkWritesInvoker.Factory;
import com.splicemachine.pipeline.impl.BulkWritesRPCInvoker;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;

public class DerbyFactoryImpl implements DerbyFactory {

	@Override
	public Filter getAllocatedFilter(byte[] localAddress) {
		return new AllocatedFilter(localAddress);
	}

	@Override
	public SpliceBaseOperationRegionScanner getOperationRegionScanner(
			RegionScanner s, Scan scan, HRegion region,
			TransactionalRegion txnRegion) throws IOException {
		return new SpliceOperationRegionScanner(s,scan,region,txnRegion);
	}

	@Override
	public List<HRegion> getOnlineRegions(RegionServerServices services,
			byte[] tableName) throws IOException {
		return services.getOnlineRegions(tableName);
	}

	@Override
	public void removeTableFromDescriptors(MasterServices masterServices,
			String tableName) throws IOException {
	      masterServices.getTableDescriptors().remove(tableName);
	}

	@Override
	public HRegionInfo loadRegionInfoFileContent(FileSystem fileSystem,
			Path path) throws IOException{
		return HRegion.loadDotRegionInfoFileContent(fileSystem, path);
	}

	@Override
	public BaseJobControl getJobControl(CoprocessorJob job, String jobPath,
			SpliceZooKeeperManager zkManager, int maxResubmissionAttempts,
			JobMetrics jobMetrics) {
		return new JobControl(job,jobPath,zkManager,maxResubmissionAttempts,jobMetrics);
	}

	@Override
	public void writeScanExternal(ObjectOutput output, Scan scan)
			throws IOException {
		scan.write(output);		
	}

	@Override
	public Scan readScanExternal(ObjectInput in) throws IOException {
		Scan scan = new Scan();
		scan.readFields(in);
		return scan;
	}

	@Override
	public void checkCallerDisconnect(HRegion region) throws IOException {
		// TODO Auto-generated method stub
        RpcCallContext currentCall = HBaseServer.getCurrentCall();
        if(currentCall!=null){
        	ThrowIfDisconnected.getThrowIfDisconnected().invoke(currentCall, region.getRegionNameAsString());
        }
	}

	@Override
	public InternalScanner noOpInternalScanner() {
		return new NoOpInternalScanner();
	}

	@Override
	public void writeRegioninfoOnFilesystem(HRegionInfo regionInfo,
			Path regiondir, FileSystem fs, Configuration conf)
			throws IOException {
		HRegion.writeRegioninfoOnFilesystem(regionInfo, regiondir, fs, conf);
	}

	@Override
	public Path getRegionDir(HRegion region) {
		return region.getRegionDir();
	}

	@Override
	public void bulkLoadHFiles(HRegion region, List<Pair<byte[], String>> paths) throws IOException {
		region.bulkLoadHFiles(paths);
	}

	@Override
	public boolean isCallTimeoutException(Throwable t) {
		return t instanceof HBaseClient.CallTimeoutException;
	}

	@Override
	public boolean isFailedServerException(Throwable t) {
		return t instanceof HBaseClient.FailedServerException;
	}

	@Override
	public Factory getBulkWritesInvoker(HConnection connection, byte[] tableName) {
		return new BulkWritesRPCInvoker.Factory(connection,tableName);
	}

	@Override
	public long computeRowCount(Logger LOG,String tableName,
			SortedSet<Pair<HRegionInfo, ServerName>> baseRegions, Scan scan) {		
	    Map<String,RegionLoad> baseRegionLoads = HBaseRegionLoads.getCachedRegionLoadsMapForTable(tableName);
		return internalComputeRowCount(LOG,baseRegions,baseRegionLoads,SpliceConstants.hbaseRegionRowEstimate,SpliceConstants.regionMaxFileSize,scan); 
	}
	
	   protected static long internalComputeRowCount(Logger LOG, SortedSet<Pair<HRegionInfo,ServerName>> regions, Map<String,RegionLoad> regionLoads, long constantRowSize, long hfileMaxSize, Scan scan) {
			long rowCount = 0;
			int numberOfRegionsInvolved = 0;
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "computeRowCount {regions={%s}, regionLoad={%s}, constantRowSize=%d, hfileMaxSize=%d, scan={%s}",
						regions==null?"null":Arrays.toString(regions.toArray()), regionLoads==null?"null":Arrays.toString(regionLoads.keySet().toArray()), constantRowSize, hfileMaxSize, scan);		
			for (Pair<HRegionInfo,ServerName> info: regions) {
				if (SpliceGenericCostController.isRegionInScan(scan,info.getFirst())) {
					if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "regionInfo with encodedname {%s} and region name as string %s", info.getFirst().getEncodedName(), info.getFirst().getRegionNameAsString());				
					numberOfRegionsInvolved++;
					rowCount+=getRowSize(LOG,constantRowSize,regionLoads==null?null:regionLoads.get(info.getFirst().getRegionNameAsString()),hfileMaxSize);	
				}
			}
			if (numberOfRegionsInvolved == 1 && scan.getStartRow() != null && !Bytes.equals(scan.getStartRow(),HConstants.EMPTY_START_ROW) && scan.getStopRow() != null && !Bytes.equals(scan.getStopRow(),HConstants.EMPTY_END_ROW) ) {
				rowCount=(long) ( ( (double)rowCount)*SpliceConstants.extraStartStopQualifierMultiplier);
			}
			return rowCount;
		}

		public static long getRowSize(Logger LOG, long constantRowSize, RegionLoad regionLoad, long hfileMaxSize) {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "getRowSize with constantRowSize %d and regionLoad %s and hfileMaxSize %d",constantRowSize, regionLoad, hfileMaxSize);
			if (regionLoad==null)
				return constantRowSize; // No Metrics
			float rowSize = (float) constantRowSize*((float) HBaseRegionLoads.memstoreAndStorefileSize(regionLoad)/(float) hfileMaxSize);
			return rowSize < SpliceConstants.optimizerTableMinimalRows?SpliceConstants.optimizerTableMinimalRows:(long) rowSize;
		}

		@Override
		public void setMaxCardinalityBasedOnRegionLoad(String tableName,
				LanguageConnectionContext lcc) {
			  Collection<RegionLoad> regionLoads =
		              HBaseRegionLoads
		                     .getCachedRegionLoadsForTable(tableName);
		      if (regionLoads != null
		              && regionLoads.size() > lcc.getStatementContext().getMaxCardinality()){
		          lcc.getStatementContext().setMaxCardinality(regionLoads.size());
		      }			
		}

		@Override
		public Filter getSuccessFilter(List<byte[]> failedTasks) {
			return new SuccessFilter(failedTasks);
		}

		@Override
		public int getRegionsSizeMB(String tableName) {
			int regionSizeMB = -1;
			
			Collection<RegionLoad> loads =
		            HBaseRegionLoads.getCachedRegionLoadsForTable(tableName);
		        if (loads != null && loads.size() == 1) {
		            regionSizeMB = HBaseRegionLoads
		                               .memstoreAndStorefileSize(loads.iterator().next());
		        }
		    return regionSizeMB;
		}

		@Override
		public Filter getHBaseEntryPredicateFilter(EntryPredicateFilter epf) {
			return new HBaseEntryPredicateFilter(epf);
		}

		@Override
		public Filter getSkippingScanFilter(
				List<Pair<byte[], byte[]>> startStopKeys,
				List<byte[]> predicates) {
			return new SkippingScanFilter(startStopKeys,predicates);
		}

		@Override
		public HTableInterface getTable(RegionCoprocessorEnvironment rce,
				byte[] tableName) throws IOException {
			return rce.getTable(tableName);
		}

		@Override
		public int getReduceNumberOfRegions(String tableName, Configuration conf) throws IOException {
			HTable outputTable = null;
			try {
				outputTable = new HTable(conf,tableName);
				return outputTable.getRegionsInfo().size();
			} finally {
				Closeables.closeQuietly(outputTable);
			}
		}
}
