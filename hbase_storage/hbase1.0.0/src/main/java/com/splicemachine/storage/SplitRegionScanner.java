/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.splicemachine.hbase.CellUtils;
import org.spark_project.guava.base.Throwables;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.pipeline.utils.PipelineUtils;
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
    private final int maxRetries;
    protected List<RegionScanner> regionScanners = new ArrayList<>(2);
    protected RegionScanner currentScanner;
    protected HRegion region;
    protected int scannerPosition;
    protected int scannerCount;
    protected int reInitCount;
    protected int scanExceptionCount;
    protected int totalScannerCount;
    protected Scan scan;
    protected Table htable;
    private Clock clock;
    private Partition clientPartition;
    private Scan initialScan;

    public SplitRegionScanner(Scan scan,
                              Table table,
                              Clock clock,
                              Partition clientPartition, SConfiguration configuration) throws IOException {
        this.scan = scan;
        this.initialScan = new Scan(scan);
        this.htable = table;
        this.clock = clock;
        this.clientPartition = clientPartition;
        totalScannerCount = 0;
        reInitCount = 0;
        scanExceptionCount = 0;
        maxRetries = configuration.getMaxRetries();
        init(false);
    }

    private void init(boolean refresh) throws IOException {
        scannerPosition = 1;
        scannerCount = 0;
        List<Partition> partitions = getPartitionsInRange(clientPartition, scan, refresh);
        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "init split scanner with scan=%s, table=%s, location_number=%d ,partitions=%s", scan, htable, partitions.size(), partitions);
        }
        boolean hasAdditionalScanners = true;
        while (hasAdditionalScanners) {
            try {
                //noinspection ForLoopReplaceableByForEach
                for (int i = 0; i < partitions.size(); i++) {
                    Scan newScan = new Scan(scan);
                    byte[] startRow = scan.getStartRow();
                    byte[] stopRow = scan.getStopRow();
                    Partition partition = partitions.get(i);
                    byte[] regionStartKey = partition.getStartKey();
                    byte[] regionStopKey = partition.getEndKey();
                    // determine if the given start an stop key fall into the region
                    byte[] splitStart = startRow.length == 0 ||
                            Bytes.compareTo(regionStartKey, startRow) >= 0 ? regionStartKey : startRow;
                    byte[] splitStop = (stopRow.length == 0 ||
                            Bytes.compareTo(regionStopKey, stopRow) <= 0) && regionStopKey.length > 0 ? regionStopKey : stopRow;
                    newScan.setStartRow(splitStart);
                    newScan.setStopRow(splitStop);
                    if (LOG.isDebugEnabled())
                        SpliceLogUtils.debug(LOG, "adding Split Region Scanner for startKey='%s', endKey='%s' on partition ['%s', '%s']",
                                CellUtils.toHex(splitStart), CellUtils.toHex(splitStop),
                                CellUtils.toHex(regionStartKey), CellUtils.toHex(regionStopKey));
                    createAndRegisterClientSideRegionScanner(htable, newScan, partitions.get(i));
                }
                hasAdditionalScanners = false;
            } catch (Exception ioe) {
                boolean rethrow = shouldRethrowException(ioe);
                if (!rethrow) {
                    reInitCount++;
                    hasAdditionalScanners = true;
                    close();
                    partitions = getPartitionsInRange(clientPartition, scan, true);
                    SpliceLogUtils.warn(LOG, "re-init split scanner with scan=%s, table=%s, location_number=%d ,partitions=%s", scan, htable, partitions.size(), partitions);
                } else
                    throw new IOException(ioe);
            }
        }
    }

    public void registerRegionScanner(RegionScanner regionScanner) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "registerRegionScanner %s", regionScanner);
        if (currentScanner == null)
            currentScanner = regionScanner;
        regionScanners.add(regionScanner);
    }

    public boolean nextInternal(List<Cell> results) throws IOException {
        try {
            boolean next = currentScanner.nextRaw(results);
            scannerCount++;
            totalScannerCount++;
            if (!next && scannerPosition < regionScanners.size()) {
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG, "scanner [%d] exhausted after {%d} records with results=%s", scannerPosition, scannerCount, results);
                currentScanner = regionScanners.get(scannerPosition);
                scannerPosition++;
                scannerCount = 0;
                return nextInternal(results);
            }
            return next;
        } catch (IOException ioe) { // Move Issue
            boolean rethrow = shouldRethrowException(ioe);
            if (!rethrow) {
                scanExceptionCount++;
                Cell topCell = ((SkeletonClientSideRegionScanner) this.currentScanner).getTopCell();
                if (topCell != null) {
                    scan.setStartRow(Bytes.add(topCell.getRow(), new byte[]{0})); // set to previous start row
                }
                close();
                SpliceLogUtils.warn(LOG, "re-init split scanner with scan=%s, table=%s",scan,htable);
                init(true); // Refresh
                results.clear();
                return nextInternal(results);
            } else
                close(); // Close Scans
                throw new IOException(ioe);
        }
    }

    @Override
    public void close() throws IOException {
        SpliceLogUtils.warn(LOG, "close table [%s], scan [%s] with rowCount=%d, reinitCount=%d, scannerExceptionCount=%d",htable.getName().toString(),initialScan,totalScannerCount,reInitCount,scanExceptionCount);
        for (RegionScanner rs : regionScanners) {
            rs.close();
        }
        regionScanners.clear();
        currentScanner = null;

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

    void createAndRegisterClientSideRegionScanner(Table table, Scan newScan, Partition partition) throws Exception {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "createAndRegisterClientSideRegionScanner with table=%s, scan=%s, tableConfiguration=%s", table, newScan, table.getConfiguration());
        Configuration conf = table.getConfiguration();
        if (System.getProperty("hbase.rootdir") != null)
            conf.set("hbase.rootdir", System.getProperty("hbase.rootdir"));

        SkeletonClientSideRegionScanner skeletonClientSideRegionScanner =
                new HBase10ClientSideRegionScanner(table,
                        FSUtils.getCurrentFileSystem(conf),
                        FSUtils.getRootDir(conf),
                        table.getTableDescriptor(),
                        ((RangedClientPartition) partition).getRegionInfo(),
                        newScan, partition.owningServer().getHostAndPort());
        this.region = skeletonClientSideRegionScanner.getRegion();
        registerRegionScanner(skeletonClientSideRegionScanner);
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
                clock.sleep(200l, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
            }
        }

        return rethrow;
    }

    /**
     * Get Partitions in Range without refreshing the underlying cache.
     *
     * @param partition
     * @param scan
     * @return
     */
    public List<Partition> getPartitionsInRange(Partition partition, Scan scan) throws IOException {
        return getPartitionsInRange(partition, scan, false);
    }

    /**
     * Get the partitions in range with optional refreshing of the cache
     *
     * @param partition
     * @param scan
     * @param refresh
     * @return
     */
    public List<Partition> getPartitionsInRange(Partition partition, Scan scan, boolean refresh) throws IOException {
        List<Partition> partitions;
        int tries = 0;
        while (tries < maxRetries) {
            partitions = partition.subPartitions(scan.getStartRow(), scan.getStopRow(), refresh);
            tries++;
            if (partitions == null || partitions.isEmpty()) {
                if (!refresh) {
                    // try again with a refresh
                    refresh = true;
                    continue;
                } else {
                    // Not Good, partition missing...
                    SpliceLogUtils.warn(LOG,"Couldn't find subpartitions in range for %s and scan %s",partition,scan);
                    try {
                        clock.sleep(PipelineUtils.getPauseTime(tries,10),TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException(e);
                    }
                }
            } else {
                return partitions;
            }
        }
        throw new IOException("Couldn't find subpartitions in range");
    }


}
