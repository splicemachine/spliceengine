package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Pair;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.scheduler.BaseJobControl;
import com.splicemachine.derby.impl.job.scheduler.JobControl;
import com.splicemachine.derby.impl.job.scheduler.JobMetrics;
import com.splicemachine.hbase.HBaseServerUtils;
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
		try {
		return services.getOnlineRegions(TableName.valueOf(tableName));
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void removeTableFromDescriptors(MasterServices masterServices,
			String tableName) {
		try {
	      masterServices.getTableDescriptors().remove(TableName.valueOf(tableName));
		} catch (Exception e) {
			new RuntimeException(e);
		}
	}
	
	@Override
	public HRegionInfo loadRegionInfoFileContent(FileSystem fileSystem,
			Path path) throws IOException {
		return HRegionFileSystem.loadRegionInfoFileContent(fileSystem, path);
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
			byte[] bytes = ProtobufUtil.toScan(scan).toByteArray();
			output.writeInt(bytes.length);
			output.write(bytes);		
	}

	@Override
	public Scan readScanExternal(ObjectInput in) throws IOException {
		byte[] scanBytes = new byte[in.readInt()];
		in.readFully(scanBytes);
		ClientProtos.Scan scan1 = ClientProtos.Scan.parseFrom(scanBytes);
		return ProtobufUtil.toScan(scan1);
	}

	
	@Override
	public void checkCallerDisconnect(HRegion region) throws IOException {
		HBaseServerUtils.checkCallerDisconnect(region, "RegionWrite");
	}
	
	@Override
	public InternalScanner noOpInternalScanner() {
		return new NoOpInternalScanner();
	}
	@Override
	public void writeRegioninfoOnFilesystem(HRegionInfo regionInfo,
			Path regiondir, FileSystem fs, Configuration conf)
			throws IOException {
		HRegionFileSystem.createRegionOnFileSystem(conf, fs, regiondir, regionInfo);
	}
	@Override
	public Path getRegionDir(HRegion region) {
		return region.getRegionFileSystem().getRegionDir();
	}	
	@Override
	public void bulkLoadHFiles(HRegion region, List<Pair<byte[], String>> paths) throws IOException {
		region.bulkLoadHFiles(paths,true); // TODO
	}
	@Override
	public boolean isCallTimeoutException(Throwable t) {
		return t instanceof RpcClient.CallTimeoutException;
	}
	@Override
	public boolean isFailedServerException(Throwable t) {
		return t instanceof RpcClient.FailedServerException;
	}
		
	
}