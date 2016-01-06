package com.splicemachine.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.splicemachine.concurrent.Clock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.*;
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
public class SplitRegionScanner implements RegionScanner {
    protected static final Logger LOG = Logger.getLogger(SplitRegionScanner.class);
	protected List<RegionScanner> regionScanners =new ArrayList<>(2);
	protected RegionScanner currentScanner;
    protected HRegion region;
	protected int scannerPosition = 1;
	protected int scannerCount = 0;
	protected Scan scan;
	protected Table htable;
    private Connection connection;
	protected List<Cell> holderResults = new ArrayList<>();
	protected boolean holderReturn;

    public SplitRegionScanner(Scan scan,
                              Table table,
                              Connection connection,
                              Clock clock,
                              List<HRegionLocation> locations) throws IOException {
		if (LOG.isDebugEnabled()) {
			SpliceLogUtils.debug(LOG, "init split scanner with scan=%s, table=%s, location_number=%d ,locations=%s",scan,table,locations.size(),locations);
		}
		this.scan = scan;
		this.htable = table;
        this.connection = connection;
		boolean hasAdditionalScanners = true;
		while (hasAdditionalScanners) {
			try {
                //noinspection ForLoopReplaceableByForEach
                for (int i = 0; i< locations.size(); i++) {
					Scan newScan = new Scan(scan);
				    byte[] startRow = scan.getStartRow();
				    byte[] stopRow = scan.getStopRow();
				    byte[] regionStartKey = locations.get(i).getRegionInfo().getStartKey();
				    byte[] regionStopKey = locations.get(i).getRegionInfo().getEndKey();
				    // determine if the given start an stop key fall into the region
				    if ((startRow.length == 0 || regionStopKey.length == 0 ||
				          Bytes.compareTo(startRow, regionStopKey) < 0) && (stopRow.length == 0 ||
				           Bytes.compareTo(stopRow, regionStartKey) > 0)) { 
				    	  byte[] splitStart = startRow.length == 0 ||
				    			  Bytes.compareTo(regionStartKey, startRow) >= 0 ? regionStartKey : startRow;
				    	  byte[] splitStop = (stopRow.length == 0 ||
				    			  Bytes.compareTo(regionStopKey, stopRow) <= 0) && regionStopKey.length > 0 ? regionStopKey : stopRow;
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
				try {
                    clock.sleep(200,TimeUnit.MILLISECONDS);// Pause for 200 ms...
                } catch (InterruptedException ignored) {
                   Thread.currentThread().interrupt();
                }
				locations = getRegionsInRange(scan);
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
	
	public boolean nextInternal(List<Cell> results) throws IOException {
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

	public HRegion getRegion() {
		return region;
	}

	void createAndRegisterClientSideRegionScanner(Table table, Scan newScan) throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "createAndRegisterClientSideRegionScanner with table=%s, scan=%s, tableConfiguration=%s",table,newScan, table.getConfiguration());
		Configuration conf = table.getConfiguration();
		if (System.getProperty("hbase.rootdir") != null)
			conf.set("hbase.rootdir",System.getProperty("hbase.rootdir"));

		throw new UnsupportedOperationException("IMPLEMENT");
//        try{
//			ClientSideRegionScanner skeletonClientSideRegionScanner=
//					new ClientSideRegionScanner(conf, FSUtils.getCurrentFileSystem(conf), FSUtils.getRootDir(conf),
//							table.getTableDescriptor(),getRegionInfo(newScan),
//							newScan,null);
//			this.region = skeletonClientSideRegionScanner.getRegion();
//			registerRegionScanner(skeletonClientSideRegionScanner);
//		} catch (Exception e) {
//			throw new IOException(e);
//		}
	}

    private HRegionInfo getRegionInfo(Scan newScan) throws IOException{
        List<HRegionLocation> containedRegions = getRegionsInRange(newScan);
        assert containedRegions.size()==1: "Programmer error: too many regions contain this scan!";
        return containedRegions.get(0).getRegionInfo();
    }


    @Override
    public boolean isFilterDone() throws IOException {
        return currentScanner.isFilterDone();
    }

    @Override
    public long getMaxResultSize() {
        return currentScanner.getMaxResultSize();
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return this.nextInternal(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result, int limit) throws IOException {
        return this.nextInternal(result);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        return this.nextInternal(results);
    }

    @Override
    public boolean next(List<Cell> result, int limit) throws IOException {
        return this.nextInternal(result);
    }


    /* ***************************************************************************************************************/
    /*private helper classes*/
    private List<HRegionLocation> getRegionsInRange(Scan scan) throws IOException {
        try(RegionLocator regionLocator=connection.getRegionLocator(htable.getName())){
            List<HRegionLocation> allRegionLocations=regionLocator.getAllRegionLocations();
            if(allRegionLocations.size()<=1) return allRegionLocations; //only one region in the table. Should always be 1 or more
            Collections.sort(allRegionLocations); //make sure that we are sorted--we probably are, but let's be safe
            byte[] start = scan.getStartRow();
            byte[] stop = scan.getStopRow();
            if(start==null||start.length<=0){
                if(stop==null||stop.length<=0) return allRegionLocations; //the scan asks for everything
                /*
                 * The start key is null, meaning that we want all the initial regions,
                 * but the stop key is not empty, so we want to stop at some point. Thus, we want to find
                 * all the regions which overlap the range (-Inf,stop). Generally, this is all the regions
                 * whose regionStart <= stop.
                 */
                List<HRegionLocation> inRange = new ArrayList<>(allRegionLocations.size());
                for(HRegionLocation location:allRegionLocations){
                    byte[] regionStart = location.getRegionInfo().getStartKey();
                    if(regionStart==null||regionStart.length<=0)
                        inRange.add(location);
                    else if(Bytes.compareTo(regionStart,stop)<0){
                        //this region starts before the stop, so it's contained
                        inRange.add(location);
                    }else{
                        //this is the first region not contained. All subsequence regions are also not contained, so we can stop
                        break;
                    }
                }
                return inRange;
            }else if(stop==null||stop.length<=0){
                /*
                 * The start key is not null, but the stop key is, so we want all regions which overlap
                 * the range [start,Inf). This is all the regions whose regionStop>start (==start is excluded,
                 * since regionStop is NOT in the region).
                 */
                List<HRegionLocation> inRange = new ArrayList<>(allRegionLocations.size());
                for(HRegionLocation location:allRegionLocations){
                    byte[] regionEnd = location.getRegionInfo().getEndKey();
                    if(regionEnd==null||regionEnd.length<=0){
                        /*
                         * This is the last region in the table: Either  start is contained in this location
                         * or it is contained in a region before this. Therefore, this region is automatically
                         * included.
                         */
                        inRange.add(location);
                    }else if(Bytes.compareTo(start,regionEnd)<0){
                        inRange.add(location); //the region end is after start, so we are interested
                    }
                }
                return inRange;
            }else if(Bytes.equals(start,stop)){
                /*
                 * Since we are exclusive on the end key during scans, this scan won't return anything; thus,
                 * no region owns it, and we can safely return an empty collection here
                 */
                return Collections.emptyList();
            }else{
                List<HRegionLocation> inRange = new ArrayList<>(allRegionLocations.size());
                for(HRegionLocation location:allRegionLocations){
                    byte[] regionStart = location.getRegionInfo().getStartKey();
                    byte[] regionEnd = location.getRegionInfo().getEndKey();
                    if(regionStart==null||regionStart.length<=0){
                        //we don't need to check regionEnd because there are always more than 1 region, so regionEnd!=null
                        if(Bytes.compareTo(start,regionEnd)<0){
                            inRange.add(location);
                        }
                    }else if(regionEnd==null||regionEnd.length<=0){
                        if(Bytes.compareTo(regionStart,stop)<0)
                            inRange.add(location);
                    }else if(Bytes.compareTo(regionStart,stop)<0){
                        inRange.add(location);
                    }else if(Bytes.compareTo(start,regionEnd)<0){
                        inRange.add(location);
                    }
                }
                return inRange;
            }
        }
    }
}
