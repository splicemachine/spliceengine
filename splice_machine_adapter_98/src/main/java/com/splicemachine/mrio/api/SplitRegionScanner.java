package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.StringUtils;
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
	protected int scannerPosition = 1;
	protected int scannerCount = 0;
	protected Scan scan;
	protected HTable htable;
	protected List<Cell> holderResults = new ArrayList<Cell>();
	protected boolean holderReturn;
	
	public SplitRegionScanner(Scan scan, HTable table, List<HRegionLocation> locations) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "init");
		this.scan = scan;
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
				    	  SpliceLogUtils.trace(LOG, "adding Split Region Scanner for startKey=%s, endKey=%s",splitStart,splitStop);
				  		  ClientSideRegionScanner clientSideRegionScanner = 
				  				  new ClientSideRegionScanner(table.getConfiguration(),FSUtils.getCurrentFileSystem(table.getConfiguration()), FSUtils.getRootDir(table.getConfiguration()),
										table.getTableDescriptor(),table.getRegionLocation(newScan.getStartRow()).getRegionInfo(),
										newScan,null);
				  				region = clientSideRegionScanner.region;
				  				registerRegionScanner(clientSideRegionScanner);			    			  
				      }
				 }
				 hasAdditionalScanners = false;
			} catch (SplitScannerDNRIOException ioe) {
				if (LOG.isDebugEnabled())
					SpliceLogUtils.debug(LOG, "exception logged creating split region scanner %s",StringUtils.stringifyException(ioe));
				hasAdditionalScanners = true;
				try {Thread.sleep(200);} catch (Exception e) {}; // Pause for 200 ms...
				locations = htable.getRegionsInRange(scan.getStartRow(), scan.getStopRow(), true);
				close();
			}
		}
			
	}
	
	public void registerRegionScanner(RegionScanner regionScanner) {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "registerRegionScanner %s",regionScanner);
		if (currentScanner == null)
			currentScanner = regionScanner;
		regionScanners.add(regionScanner);
	}
	
	@Override
	public boolean next(List<Cell> results) throws IOException {
		if (holderReturn) {
			holderReturn = false;
			results.addAll(holderResults);
			return true;
		}
		boolean next = currentScanner.nextRaw(results);
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
			holderReturn = next(holderResults);
			return holderReturn;
		}

		return next;
	}

	@Override
	public boolean next(List<Cell> result, int limit) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "next with results=%s and limit=%d",result,limit);
		return next(result);
	}

	@Override
	public void close() throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "close");
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
	public boolean isFilterDone() throws IOException {
		return currentScanner.isFilterDone();
	}

	@Override
	public boolean reseek(byte[] row) throws IOException {
		throw new RuntimeException("Reseek not supported");
	}

	@Override
	public long getMaxResultSize() {
		return currentScanner.getMaxResultSize();
	}

	@Override
	public long getMvccReadPoint() {
		return currentScanner.getMvccReadPoint();
	}

	@Override
	public boolean nextRaw(List<Cell> result) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "nextRaw with results=%s",result);
		return next(result);
	}

	@Override
	public boolean nextRaw(List<Cell> result, int limit) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "nextRaw with results=%s and limit=%d",result,limit);
		return next(result, limit);
	}
	@Override
	public HRegion getRegion() {
		return region;
	}
	
	
	
}
