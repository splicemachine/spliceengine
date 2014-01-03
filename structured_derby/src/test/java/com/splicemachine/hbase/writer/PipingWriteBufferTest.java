package com.splicemachine.hbase.writer;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.*;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;
import org.junit.Assert;
/**
 * @author Scott Fines
 *         Created on: 9/24/13
 */
public class PipingWriteBufferTest{

    private PipingWriteBuffer  buffer;
    private byte[] tableName;
    FakedHBaseRegionCache regionCache;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testRebuildingRegionToBufferMap() throws Exception
    {
        tableName = Bytes.toBytes("testTable");
        regionCache = new FakedHBaseRegionCache();
        Monitor monitor = new Monitor(SpliceConstants.writeBufferSize,SpliceConstants.maxBufferEntries,SpliceConstants.numRetries,SpliceConstants.pause,SpliceConstants.maxFlushesPerRegion);
        buffer = new PipingWriteBuffer(tableName, "100", null, null, regionCache, null, null, monitor);

        KVPair kv = new KVPair(Bytes.toBytes("aaaa"), Bytes.toBytes("1"));
        buffer.add(kv);

        kv = new KVPair(Bytes.toBytes("bbbc"), Bytes.toBytes("2"));
        buffer.add(kv);

        kv = new KVPair(Bytes.toBytes("dddd"), Bytes.toBytes("3"));
        buffer.add(kv);

        int [] r1 ={2, 1};
        TreeMap<Integer, Integer> map = buffer.getRegionToBufferCount();
        Set<Integer> set = map.keySet();
        Iterator<Integer> iterator = set.iterator();
        int i = 0;
        while(iterator.hasNext()) {
            int count = map.get(iterator.next()).intValue();
            Assert.assertEquals(count, r1[i]);
            ++i;
        }

        // force rebuilding region to buffer map
        buffer.setBuildBuffer();

        // fake table split
        regionCache.reload();

        //add another row and this should trigger a rebuild of region to buffer map
        kv = new KVPair(Bytes.toBytes("eeee"), Bytes.toBytes("4"));
        buffer.add(kv);

        int[] r2 = {1, 1, 2};
        map = buffer.getRegionToBufferCount();
        set = map.keySet();
        iterator = set.iterator();
        i = 0;
        while(iterator.hasNext()) {
            int count = map.get(iterator.next()).intValue();
            Assert.assertEquals(count, r2[i]);
            ++i;
        }
    }

    @After
    public void tearDown() throws Exception {

    }

    public class FakedHBaseRegionCache implements RegionCache {

        private final ConcurrentHashMap<Integer,SortedSet<HRegionInfo>> regionCache;

        public  FakedHBaseRegionCache () {
            this.regionCache = new  ConcurrentHashMap<Integer,SortedSet<HRegionInfo>>();
            //populate the cache with HRegionInfo
            populateCache();
        }

        private void populateCache() {

            // Populate the cache with two regions
            Integer key = Bytes.mapKey(tableName);
            SortedSet<HRegionInfo> infos =  new ConcurrentSkipListSet<HRegionInfo>();
            HRegionInfo info = new HRegionInfo(tableName, null, Bytes.toBytes("cccc"));
            infos.add(info);
            info = new HRegionInfo(tableName, Bytes.toBytes("cccd"), null);
            infos.add(info);
            regionCache.put(key, infos);
        }

        public void reload() {

            // Populate the cache with two regions
            Integer key = Bytes.mapKey(tableName);
            SortedSet<HRegionInfo> infos =  regionCache.get(key);
            infos.clear();
            HRegionInfo info = new HRegionInfo(tableName, null, Bytes.toBytes("bbba"));
            infos.add(info);

            info = new HRegionInfo(tableName, Bytes.toBytes("bbbb"), Bytes.toBytes("cccc"));
            infos.add(info);

            info = new HRegionInfo(tableName, Bytes.toBytes("cccd"), null);
            infos.add(info);

            regionCache.put(key, infos);
        }

        @Override
        public long getUpdateTimestamp() {
            return 0;
        }

        @Override
        public void start() {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public long size(){
            return regionCache.size();
        }

        @Override
        public void invalidate(byte[] tableName){
        }

        @Override
        public SortedSet<HRegionInfo> getRegions(byte[] tableName) throws ExecutionException {
            return regionCache.get(Bytes.mapKey(tableName));
        }

        @Override
        public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        }
    }

    private static class Monitor implements WriteCoordinatorStatus,BufferConfiguration{
        private volatile long maxHeapSize;
        private volatile int maxEntries;
        private volatile int maxRetries;
        private volatile int maxFlushesPerRegion;

        private AtomicInteger outstandingBuffers = new AtomicInteger(0);
        private volatile long pauseTime;
        private AtomicLong writesRejected = new AtomicLong(0l);

        private Monitor(long maxHeapSize, int maxEntries, int maxRetries,long pauseTime,int maxFlushesPerRegion) {
            this.maxHeapSize = maxHeapSize;
            this.maxEntries = maxEntries;
            this.maxRetries = maxRetries;
            this.pauseTime = pauseTime;
            this.maxFlushesPerRegion = maxFlushesPerRegion;
        }

        @Override public long getMaxBufferHeapSize() { return maxHeapSize; }
        @Override public void setMaxBufferHeapSize(long newMaxHeapSize) { this.maxHeapSize = newMaxHeapSize; }
        @Override public int getMaxBufferEntries() { return maxEntries; }
        @Override public void setMaxBufferEntries(int newMaxBufferEntries) { this.maxEntries = newMaxBufferEntries; }
        @Override public int getOutstandingCallBuffers() { return outstandingBuffers.get(); }
        @Override public int getMaximumRetries() { return maxRetries; }
        @Override public void setMaximumRetries(int newMaxRetries) { this.maxRetries = newMaxRetries; }
        @Override public long getPauseTime() { return pauseTime; }
        @Override public void setPauseTime(long newPauseTimeMs) { this.pauseTime = newPauseTimeMs; }
        @Override public long getMaxHeapSize() { return maxHeapSize; }
        @Override public int getMaxEntries() { return maxEntries; }
        @Override public int getMaxFlushesPerRegion() { return maxFlushesPerRegion; }
        @Override public void setMaxFlushesPerRegion(int newMaxFlushesPerRegion) { this.maxFlushesPerRegion = newMaxFlushesPerRegion; }

        @Override
        public long getSynchronousFlushCount() {
            return writesRejected.get();
        }

        @Override
        public void writeRejected() {
            this.writesRejected.incrementAndGet();
        }
    }
}



