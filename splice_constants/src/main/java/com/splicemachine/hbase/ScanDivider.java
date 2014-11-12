package com.splicemachine.hbase;

import com.google.common.collect.Lists;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Divides one scan into many.
 */
public class ScanDivider {

    /**
     * Divide one scan into many based on how keys are distributed by the specified RowKeyDistributor.
     */
    public static List<Scan> divide(Scan original, RowKeyDistributor rowKeyDistributor) throws IOException {
        Pair<byte[], byte[]>[] intervals = rowKeyDistributor.getDistributedIntervals(original.getStartRow(), original.getStopRow());
        List<Scan> scans = Lists.newArrayListWithCapacity(intervals.length);
        for (Pair<byte[], byte[]> interval : intervals) {
            Scan newScan = new Scan(original);
            newScan.setStartRow(interval.getFirst());
            newScan.setStopRow(interval.getSecond());
            newScan.setFilter(original.getFilter());
            scans.add(newScan);
        }
        return scans;
    }

    /**
     * Divide one scan based on the provided region info.  Here we have a bounded or unbounded scan over an arbitrary
     * user table and we want to perform multiple parts in parallel.  The HRegionInfo collection may be a bit stale, but
     * this is ok (correctness wise) as long the union of our divided scans equals the original scan.
     */
    public static List<Scan> divide(Scan original, SortedSet<Pair<HRegionInfo,ServerName>> regions) throws IOException {
        checkArgument(regions.size() > 1, "expected more than one region");

        final int MAX_SCAN_COUNT = 16;
        final int STEP = Math.max(1, (int) Math.floor((double) regions.size() / MAX_SCAN_COUNT));
        final int REGION_COUNT = regions.size();

        List<Scan> newScans = Lists.newLinkedList();
        List<Pair<HRegionInfo,ServerName>> sortedRegionList = Lists.newArrayList(regions);
        HRegionInfo lastRegion = null;

        for (int i = STEP - 1; i < REGION_COUNT; i = i + STEP) {
        	Pair<HRegionInfo,ServerName> pair = sortedRegionList.get(i);
            HRegionInfo currentRegion = pair.getFirst();
            Scan newScan = new Scan(original);
            newScans.add(newScan);

            // first scan
            if (i == STEP - 1) {
                newScan.setStartRow(original.getStartRow());
                newScan.setStopRow(currentRegion.getEndKey());
            }
            // last scan
            else if (i + STEP >= REGION_COUNT) {
                newScan.setStartRow(lastRegion.getEndKey());
                newScan.setStopRow(original.getStopRow());
            }
            // middle scans
            else {
                newScan.setStartRow(lastRegion.getEndKey());
                newScan.setStopRow(currentRegion.getEndKey());
            }
            lastRegion = currentRegion;
        }

        return newScans;
    }


}
