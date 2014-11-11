package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Pair;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.scheduler.BaseJobControl;
import com.splicemachine.derby.impl.job.scheduler.JobMetrics;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.utils.SpliceZooKeeperManager;


public interface DerbyFactory {
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
	boolean isCallTimeoutException(Throwable t);
	boolean isFailedServerException(Throwable t);
}