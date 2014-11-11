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
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Pair;

import com.splicemachine.derby.impl.job.scheduler.JobControl;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.scheduler.BaseJobControl;
import com.splicemachine.derby.impl.job.scheduler.JobMetrics;
import com.splicemachine.hbase.ThrowIfDisconnected;
import com.splicemachine.si.api.TransactionalRegion;
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
		
}
