package com.splicemachine.hbase;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.apache.hadoop.hbase.util.Bytes.toInt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ScanDividerTest {

    private static final byte[] TABLE = toBytes("TABLE");

    @Test
    public void divideForParallelScan_twoRegions() throws IOException {

        Filter originalFilter = new KeyOnlyFilter();

        Scan originalScan = new Scan();
        originalScan.setStartRow(toBytes(15));
        originalScan.setStopRow(toBytes(25));
        originalScan.setFilter(originalFilter);

        // [10, 20], [20, 30]
        SortedSet<Pair<HRegionInfo,ServerName>> regions = getTestRegions(2);

        List<Scan> scanList = ScanDivider.divide(originalScan, regions);

        assertEquals(2, scanList.size());

        // divided scan 1
        assertEquals(15, toInt(scanList.get(0).getStartRow()));
        assertEquals(20, toInt(scanList.get(0).getStopRow()));
        assertSame(originalFilter, scanList.get(0).getFilter());

        // divided scan 2
        assertEquals(20, toInt(scanList.get(1).getStartRow()));
        assertEquals(25, toInt(scanList.get(1).getStopRow()));
        assertSame(originalFilter, scanList.get(1).getFilter());
    }

    @Test
    public void divideForParallelScan_threeRegions() throws IOException {

        Filter originalFilter = new KeyOnlyFilter();

        Scan originalScan = new Scan();
        originalScan.setStartRow(toBytes(15));
        originalScan.setStopRow(toBytes(35));
        originalScan.setFilter(originalFilter);

        // [10, 20], [20, 30], [30, 40]
        SortedSet<Pair<HRegionInfo,ServerName>> regions = getTestRegions(3);

        List<Scan> scanList = ScanDivider.divide(originalScan, regions);

        assertEquals(3, scanList.size());

        // divided scan 1
        assertEquals(15, toInt(scanList.get(0).getStartRow()));
        assertEquals(20, toInt(scanList.get(0).getStopRow()));
        assertSame(originalFilter, scanList.get(0).getFilter());

        // divided scan 2
        assertEquals(20, toInt(scanList.get(1).getStartRow()));
        assertEquals(30, toInt(scanList.get(1).getStopRow()));
        assertSame(originalFilter, scanList.get(1).getFilter());

        // divided scan 3
        assertEquals(30, toInt(scanList.get(2).getStartRow()));
        assertEquals(35, toInt(scanList.get(2).getStopRow()));
        assertSame(originalFilter, scanList.get(2).getFilter());
    }

    @Test
    public void divideForParallelScan_largeRegionCount() throws IOException {

        Filter originalFilter = new KeyOnlyFilter();

        Scan originalScan = new Scan();
        originalScan.setStartRow(toBytes(100));
        originalScan.setStopRow(toBytes(9500));
        originalScan.setFilter(originalFilter);

        // [10, 20], [20, 30], [30, 40] ... [9990, 10000]
        SortedSet<Pair<HRegionInfo,ServerName>> regions = getTestRegions(1000);  // one thousand regions!

        List<Scan> scanList = ScanDivider.divide(originalScan, regions);

        assertEquals(16, scanList.size());

        // divided scan 1
        assertEquals(100, toInt(scanList.get(0).getStartRow()));
        assertEquals(630, toInt(scanList.get(0).getStopRow()));

        // divided scan 2
        assertEquals(630, toInt(scanList.get(1).getStartRow()));
        assertEquals(1250, toInt(scanList.get(1).getStopRow()));

        // divided scan 3
        assertEquals(1250, toInt(scanList.get(2).getStartRow()));
        assertEquals(1870, toInt(scanList.get(2).getStopRow()));

        //
        // . . .
        //

        // divided scan 14
        assertEquals(8070, toInt(scanList.get(13).getStartRow()));
        assertEquals(8690, toInt(scanList.get(13).getStopRow()));

        // divided scan 15
        assertEquals(8690, toInt(scanList.get(14).getStartRow()));
        assertEquals(9310, toInt(scanList.get(14).getStopRow()));

        // divided scan 16
        assertEquals(9310, toInt(scanList.get(15).getStartRow()));
        assertEquals(9500, toInt(scanList.get(15).getStopRow()));
    }

    private static SortedSet<Pair<HRegionInfo,ServerName>> getTestRegions(int count) {
    	SortedSet<Pair<HRegionInfo,ServerName>> regions = new TreeSet<Pair<HRegionInfo,ServerName>>(new RegionCacheComparator());
        int startKey = 10;
        int endKey = 20;
        for (int i = 0; i < count; i++) {
            regions.add(testRegion(TABLE, toBytes(startKey), toBytes(endKey)));
            startKey += 10;
            endKey += 10;
        }
        return regions;
    }


    private static Pair<HRegionInfo,ServerName> testRegion(byte[] tableName, byte[] startKey, byte[] endKey) {
        return Pair.newPair(new HRegionInfo(tableName, startKey, endKey),new ServerName("example.org,1234,1212121212"));
    }


}