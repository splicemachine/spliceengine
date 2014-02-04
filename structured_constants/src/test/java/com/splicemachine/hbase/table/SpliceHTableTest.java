package com.splicemachine.hbase.table;

import com.google.common.collect.Sets;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 * Created on: 10/25/13
 */
public class SpliceHTableTest {

    @Test
    public void testGetStartAndEndKeysWorksWithSameStartAndEndKeyEmptyStart() throws Exception {
        RegionCache cache = mock(RegionCache.class);

        final SortedSet<HRegionInfo> regions = Sets.newTreeSet();
        byte[] tableName = Bytes.toBytes("1184");
        byte[] startKey = new byte[]{};
        for(int i=0;i<10;i++){
            byte[] endKey = Encoding.encode(i);
            HRegionInfo info = new HRegionInfo(tableName,startKey,endKey);
            startKey = endKey;
            regions.add(info);
        }
        byte[] endKey = HConstants.EMPTY_END_ROW;
        regions.add(new HRegionInfo(tableName,startKey,endKey));
        when(cache.getRegions(tableName)).thenReturn(regions);

        HConnection connection = mock(HConnection.class);
        Configuration config  = SpliceConstants.config;
        when(connection.getConfiguration()).thenReturn(config);

        when(connection.getRegionLocation(any(byte[].class),any(byte[].class),anyBoolean())).thenAnswer(new Answer<HRegionLocation>() {
            @Override
            public HRegionLocation answer(InvocationOnMock invocation) throws Throwable {
                byte[] startKey = (byte[]) invocation.getArguments()[1];
                for(HRegionInfo regionInfo:regions){
                    byte[] endKey = regionInfo.getEndKey();
                    if(endKey.length==0){
                        return new HRegionLocation(regionInfo,"localhost",8180);
                    }else if(Bytes.compareTo(endKey,startKey)>0){
                        return new HRegionLocation(regionInfo,"localhost",8180);
                    }
                }
                return null;
            }
        });

        ExecutorService executor = mock(ExecutorService.class);
        SpliceHTable table = new SpliceHTable(tableName,connection,executor,cache);

        byte[] testStart = HConstants.EMPTY_START_ROW;
        byte[] testEnd = testStart;

        List<Pair<byte[],byte[]>> pairs = table.getKeysDealWithSameStartStopKey(testStart, testEnd,0);
        Assert.assertEquals("Incorrect number of regions returned!",11,pairs.size());
    }


    @Test
    public void testGetStartAndEndKeysWorksWithSameStartAndEndKey() throws Exception {
        RegionCache cache = mock(RegionCache.class);

        final SortedSet<HRegionInfo> regions = Sets.newTreeSet();
        byte[] tableName = Bytes.toBytes("1184");
        byte[] startKey = new byte[]{};
        for(int i=0;i<10;i++){
            byte[] endKey = Encoding.encode(i);
            HRegionInfo info = new HRegionInfo(tableName,startKey,endKey);
            startKey = endKey;
            regions.add(info);
        }
        byte[] endKey = HConstants.EMPTY_END_ROW;
        regions.add(new HRegionInfo(tableName,startKey,endKey));
        when(cache.getRegions(tableName)).thenReturn(regions);

        HConnection connection = mock(HConnection.class);
        Configuration config  = SpliceConstants.config;
        when(connection.getConfiguration()).thenReturn(config);

        when(connection.getRegionLocation(any(byte[].class),any(byte[].class),anyBoolean())).thenAnswer(new Answer<HRegionLocation>() {
            @Override
            public HRegionLocation answer(InvocationOnMock invocation) throws Throwable {
                byte[] startKey = (byte[]) invocation.getArguments()[1];
                for(HRegionInfo regionInfo:regions){
                    byte[] endKey = regionInfo.getEndKey();
                    if(endKey.length==0){
                        return new HRegionLocation(regionInfo,"localhost",8180);
                    }else if(Bytes.compareTo(endKey,startKey)>0){
                        return new HRegionLocation(regionInfo,"localhost",8180);
                    }
                }
                return null;
            }
        });

        ExecutorService executor = mock(ExecutorService.class);
        SpliceHTable table = new SpliceHTable(tableName,connection,executor,cache);

        byte[] testStart = Encoding.encode(1);
        byte[] testEnd = testStart;

        List<Pair<byte[],byte[]>> pairs = table.getKeysDealWithSameStartStopKey(testStart, testEnd,0);
        Assert.assertEquals("Incorrect number of regions returned!",1,pairs.size());
    }

    @Test
    public void testGetStartEndKeysFromCacheWorksWithCorrectCache() throws Exception {
        RegionCache cache = mock(RegionCache.class);

        SortedSet<HRegionInfo> regions = Sets.newTreeSet();
        byte[] tableName = Bytes.toBytes("1184");
        byte[] startKey = new byte[]{};
        for(int i=0;i<10;i++){
            byte[] endKey = Encoding.encode(i);
            HRegionInfo info = new HRegionInfo(tableName,startKey,endKey);
            startKey = endKey;
            regions.add(info);
        }
        byte[] endKey = HConstants.EMPTY_END_ROW;
        regions.add(new HRegionInfo(tableName,startKey,endKey));
        when(cache.getRegions(tableName)).thenReturn(regions);

        HConnection connection = mock(HConnection.class);
        Configuration config  = SpliceConstants.config;
        when(connection.getConfiguration()).thenReturn(config);
        ExecutorService executor = mock(ExecutorService.class);
        SpliceHTable table = new SpliceHTable(tableName,connection,executor,cache);

        byte[] testStart = Encoding.encode(1);
        byte[] testEnd = Encoding.encode(3);

        List<Pair<byte[],byte[]>> pairs = table.getKeysDealWithSameStartStopKey(testStart, testEnd,0);
        Assert.assertEquals("Incorrect number of regions returned!",2,pairs.size());
    }
}
