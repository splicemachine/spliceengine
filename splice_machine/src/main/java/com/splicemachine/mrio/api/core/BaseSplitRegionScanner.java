package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.utils.SpliceLogUtils;
/*
 * 
 * Split Scanner for multiple region scanners
 * 
 */
public abstract class BaseSplitRegionScanner<T> implements SpliceRegionScanner {
    protected static final Logger LOG = Logger.getLogger(BaseSplitRegionScanner.class);
	protected List<RegionScanner> regionScanners = new ArrayList<RegionScanner>(2);	
	protected RegionScanner currentScanner;
	protected FileSystem fileSystem;
	protected HRegion region;
	protected int scannerPosition = 1;
	protected int scannerCount = 0;
	protected Scan scan;
	protected HTable htable;
	protected List<T> holderResults = new ArrayList<T>();
	protected boolean holderReturn;
	SDataLib dataLib = HTransactorFactory.getTransactor().getDataLib();
	
	public BaseSplitRegionScanner(Scan scan, HTable table, List<HRegionLocation> locations) throws IOException {
		if (LOG.isDebugEnabled()) {
			SpliceLogUtils.debug(LOG, "init split scanner with scan=%s, table=%s, location_number=%d ,locations=%s",scan,table,locations.size(),locations);
		}
		this.scan = scan;
		this.htable = table;
		boolean hasAdditionalScanners = true;
		while (hasAdditionalScanners) {
			try {
				for (int i = 0; i< locations.size(); i++) {
					Scan newScan = new Scan(scan);
				    byte[] startRow = scan.getStartRow();
				    byte[] stopRow = scan.getStopRow();
				    byte[] regionStartKey = locations.get(i).getRegionInfo().getStartKey();
				    byte[] regionStopKey = locations.get(i).getRegionInfo().getEndKey();
				    // determine if the given start an stop key fall into the region
				    if ((startRow.length == 0 || regionStopKey.length == 0 ||
				          Bytes.compareTo(startRow, regionStopKey) < 0) &&
				          (stopRow.length == 0 ||
				           Bytes.compareTo(stopRow, regionStartKey) > 0)) { 
				    	  byte[] splitStart = startRow.length == 0 ||
				    			  Bytes.compareTo(regionStartKey, startRow) >= 0 ?
				    					  regionStartKey : startRow;
				    	  byte[] splitStop = (stopRow.length == 0 ||
				    			  Bytes.compareTo(regionStopKey, stopRow) <= 0) &&
				    			  regionStopKey.length > 0 ? regionStopKey : stopRow;
				    	  newScan.setStartRow(splitStart);
				    	  newScan.setStopRow(splitStop);
				    	  SpliceLogUtils.debug(LOG, "adding Split Region Scanner for startKey=%s, endKey=%s",splitStart,splitStop);
				    	  createAndRegisterClientSideRegionScanner(table,newScan);
				    }
				 }
				 hasAdditionalScanners = false;
			} catch (DoNotRetryIOException ioe) {
				if (LOG.isDebugEnabled())
					SpliceLogUtils.debug(LOG, "exception logged creating split region scanner %s",StringUtils.stringifyException(ioe));
				hasAdditionalScanners = true;
				try {Thread.sleep(200);} catch (Exception e) {}; // Pause for 200 ms...
				locations = getRegionsInRange(scan);
				close();
			}
		}
			
	}
	
	abstract List<HRegionLocation> getRegionsInRange(Scan scan) throws IOException;
	
	abstract void createAndRegisterClientSideRegionScanner(HTable table, Scan newScan) throws IOException;
	
	public void registerRegionScanner(RegionScanner regionScanner) {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "registerRegionScanner %s",regionScanner);
		if (currentScanner == null)
			currentScanner = regionScanner;
		regionScanners.add(regionScanner);
	}
	
	public boolean nextInternal(List<T> results) throws IOException {
		if (holderReturn) {
			holderReturn = false;
			results.addAll(holderResults);
			return true;
		}
		boolean next = dataLib.internalScannerNext(currentScanner, results);
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "next with results=%s and row count {%d}",results, scannerCount);
		scannerCount++;
		if (!next && scannerPosition<regionScanners.size()) {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "scanner [%d] exhausted after {%d} records",scannerPosition,scannerCount);
			currentScanner = regionScanners.get(scannerPosition);
			scannerPosition++;
			scannerCount = 0;
			holderResults.clear();
			holderReturn = nextInternal(holderResults);
			return holderReturn;
		}

		return next;
	}

	@Override
	public void close() throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "close");
		if (currentScanner != null)
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
	
}
