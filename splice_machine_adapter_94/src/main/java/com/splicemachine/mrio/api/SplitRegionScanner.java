package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;
/*
 * 
 * Split Scanner for multiple region scanners
 * 
 */
public class SplitRegionScanner implements SpliceRegionScanner {
    protected static final Logger LOG = Logger.getLogger(SplitRegionScanner.class);
	protected List<RegionScanner> regionScanners = new ArrayList<RegionScanner>(2);	
	protected RegionScanner currentScanner;
	protected FileSystem fileSystem;
	protected HRegion region;
	
	public SplitRegionScanner(Scan scan, HTable table) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "init");
		ClientSideRegionScanner clientSideRegionScanner = 
				new ClientSideRegionScanner(table.getConfiguration(),FSUtils.getCurrentFileSystem(table.getConfiguration()), FSUtils.getRootDir(table.getConfiguration()),
						table.getTableDescriptor(),table.getRegionLocation(scan.getStartRow()).getRegionInfo(),
						scan,null);		
		region = clientSideRegionScanner.region;
		registerRegionScanner(clientSideRegionScanner);
	}
	
	public void registerRegionScanner(RegionScanner regionScanner) {
		if (currentScanner != null)
			regionScanners.add(regionScanner);
		else
			currentScanner = regionScanner;
	}
	
	public boolean next(List<KeyValue> results) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "next with results=%s",results);
		boolean next = currentScanner.nextRaw(results,null);
		if (!next && !regionScanners.isEmpty()) {
			currentScanner = regionScanners.remove(0);
			return next(results);
		}
		return next;
	}


	@Override
	public void close() throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "close");
		currentScanner.close();
		for (RegionScanner rs: regionScanners) {
			rs.close();
		}
	}

	@Override
	public HRegionInfo getRegionInfo() {
		return currentScanner.getRegionInfo();
	}

	@Override
	public boolean isFilterDone() {
		return currentScanner.isFilterDone();
	}

	@Override
	public boolean reseek(byte[] row) throws IOException {
		throw new RuntimeException("Reseek not supported");
	}

	@Override
	public long getMvccReadPoint() {
		return currentScanner.getMvccReadPoint();
	}
	@Override
	public HRegion getRegion() {
		return region;
	}

	@Override
	public boolean next(List<KeyValue> results, String arg1) throws IOException {
		return next(results);
	}

	@Override
	public boolean next(List<KeyValue> results, int arg1) throws IOException {
		return next(results);
	}

	@Override
	public boolean next(List<KeyValue> results, int arg1, String arg2)
			throws IOException {
		return next(results);
	}

	@Override
	public boolean nextRaw(List<KeyValue> results, String arg1) throws IOException {
		return next(results);
	}

	@Override
	public boolean nextRaw(List<KeyValue> results, int arg1, String arg2)
			throws IOException {
		return next(results);
	}
	
}
