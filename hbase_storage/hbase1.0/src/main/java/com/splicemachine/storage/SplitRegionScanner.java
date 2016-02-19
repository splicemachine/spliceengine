package com.splicemachine.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.google.common.base.Throwables;
import com.splicemachine.access.client.HBase10ClientSideRegionScanner;
import com.splicemachine.access.client.SkeletonClientSideRegionScanner;
import com.splicemachine.concurrent.Clock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
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
                              Partition partition) throws IOException {
        List<Partition> partitions = getPartitionsInRange(partition,scan);
		if (LOG.isDebugEnabled()) {
			SpliceLogUtils.debug(LOG, "init split scanner with scan=%s, table=%s, location_number=%d ,partitions=%s", scan, table, partitions.size(), partitions);
		}
		this.scan = scan;
		this.htable = table;
        this.connection = connection;
		boolean hasAdditionalScanners = true;
		while (hasAdditionalScanners) {
			try {
                //noinspection ForLoopReplaceableByForEach
                for (int i = 0; i< partitions.size(); i++) {
					Scan newScan = new Scan(scan);
				    byte[] startRow = scan.getStartRow();
				    byte[] stopRow = scan.getStopRow();
				    byte[] regionStartKey = partitions.get(i).getStartKey();
				    byte[] regionStopKey = partitions.get(i).getEndKey();
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
				    	  createAndRegisterClientSideRegionScanner(table,newScan,partitions.get(i));
				    }
				 }
				 hasAdditionalScanners = false;
			} catch (DoNotRetryIOException ioe) {
                boolean rethrow = shouldRethrowException(ioe);
                if (!rethrow) {
                    hasAdditionalScanners = true;
                    regionScanners.clear();
                    partitions = getPartitionsInRange(partition, scan);
                    close();
                }
                else
                    throw ioe;
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

	void createAndRegisterClientSideRegionScanner(Table table, Scan newScan, Partition partition) throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "createAndRegisterClientSideRegionScanner with table=%s, scan=%s, tableConfiguration=%s",table,newScan, table.getConfiguration());
		Configuration conf = table.getConfiguration();
		if (System.getProperty("hbase.rootdir") != null)
			conf.set("hbase.rootdir",System.getProperty("hbase.rootdir"));

        try{
			SkeletonClientSideRegionScanner skeletonClientSideRegionScanner=
					new HBase10ClientSideRegionScanner(table,
							FSUtils.getCurrentFileSystem(conf),
							FSUtils.getRootDir(conf),
							table.getTableDescriptor(),
                            ((RangedClientPartition) partition).getRegionInfo(),
							newScan);
			this.region = skeletonClientSideRegionScanner.getRegion();
			registerRegionScanner(skeletonClientSideRegionScanner);
		} catch (Exception e) {
			throw new IOException(e);
		}
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

    private boolean shouldRethrowException(Exception e) {

        // recreate region scanners if the exception was throw due to a region split. In that case, the
        // root cause could be an DoNotRetryException or an RemoteWithExtrasException with class name of
        // DoNotRetryException

        Throwable rootCause = Throwables.getRootCause(e);
        boolean rethrow = true;
        if (rootCause instanceof DoNotRetryIOException)
            rethrow = false;
        else if (rootCause instanceof RemoteWithExtrasException) {
            String className = ((RemoteWithExtrasException) rootCause).getClassName();
            if (className.compareTo(DoNotRetryIOException.class.getName()) == 0) {
                rethrow = false;
            }
        }

        if (!rethrow) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "exception logged creating split region scanner %s", StringUtils.stringifyException(e));
            try {
                Thread.sleep(200);
            } catch (Exception ex) {
            }
        }

        return rethrow;
    }

    public List<Partition> getPartitionsInRange(Partition partition, Scan scan) {
        List<Partition> partitions = null;
        boolean refresh = false;
        while (true) {
            partitions = partition.subPartitions(scan.getStartRow(), scan.getStopRow(),refresh);
            if (partitions==null|| partitions.isEmpty()) {
                refresh = true;
                continue;
            } else {
                break;
            }
        }
        return partitions;
    }

}
