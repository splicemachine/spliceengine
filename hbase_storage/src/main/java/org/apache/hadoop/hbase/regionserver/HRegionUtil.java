/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package org.apache.hadoop.hbase.regionserver;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.SpliceHFileUtil;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;

/**
 * Class for accessing protected methods in HBase.
 *
 */
public class HRegionUtil {
    private static final Logger LOG=Logger.getLogger(HRegionUtil.class);
    public static void lockStore(Store store){
        ((HStore)store).lock.readLock().lock();
    }

    public static void unlockStore(Store store){
        ((HStore)store).lock.readLock().unlock();
    }

    public static void updateWriteRequests(HRegion region,long numWrites){
        LongAdder writeRequestsCount = region.writeRequestsCount;
        if (writeRequestsCount != null)
            writeRequestsCount.add(numWrites);
    }

    public static void updateReadRequests(HRegion region,long numReads){
        LongAdder readRequestsCount = region.readRequestsCount;
        if (readRequestsCount != null)
            readRequestsCount.add(numReads);
    }

    public static List<byte[]> getCutpoints(Store store, byte[] start, byte[] end,
                                            int requestedSplits, long bytesPerSplit) throws IOException {
        assert Bytes.startComparator.compare(start, end) <= 0 || start.length == 0 || end.length == 0;
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getCutpoints");
        Collection<? extends StoreFile> storeFiles;
        storeFiles = store.getStorefiles();
        HFile.Reader fileReader = null;
        List<byte[]> cutPoints = new ArrayList<byte[]>();
        int carry = 0;

        byte[] regionStart = store.getRegionInfo().getStartKey();
        byte[] regionEnd = store.getRegionInfo().getEndKey();

        if (regionStart != null && regionStart.length > 0) {
            if (start == null || Bytes.startComparator.compare(start, regionStart) < 0) {
                start = regionStart;
            }
        }
        if (regionEnd != null && regionEnd.length > 0) {
            if (end == null || Bytes.endComparator.compare(end, regionEnd) > 0) {
                end = regionEnd;
            }
        }

        long numSplits = 0;
        int splitBlockSize = HConfiguration.getConfiguration().getSplitBlockSize();
        if (bytesPerSplit > 0) {
            long totalStoreFileInBytes = 0;
            for (StoreFile file : storeFiles) {
                if (file != null) {
                    totalStoreFileInBytes += ((HStoreFile) file).getFileInfo().getFileStatus().getLen();
                }
            }
            numSplits = totalStoreFileInBytes / bytesPerSplit;
            if (numSplits <= 1)
                numSplits = 1;
            long bytesPerSplitEvenDistribution = totalStoreFileInBytes / numSplits;
            if (bytesPerSplitEvenDistribution > Integer.MAX_VALUE) {
                splitBlockSize = Integer.MAX_VALUE;
            } else {
                splitBlockSize = (int) bytesPerSplitEvenDistribution;
            }
        }
        else if (requestedSplits > 0) {
            long totalStoreFileInBytes = 0;
            for (StoreFile file : storeFiles) {
                if (file != null) {
                    totalStoreFileInBytes += ((HStoreFile)file).getFileInfo().getFileStatus().getLen();
                }
            }
            long bytesPerSplitThisRegion = totalStoreFileInBytes / requestedSplits;
            if (bytesPerSplitThisRegion > Integer.MAX_VALUE) {
                splitBlockSize = Integer.MAX_VALUE;
            } else {
                splitBlockSize = (int) bytesPerSplitThisRegion;
            }
        }

        Pair<byte[], byte[]> range = new Pair<>(start, end);
        for (StoreFile file : storeFiles) {
            if (file != null) {
                long storeFileInBytes = ((HStoreFile) file).getFileInfo().getFileStatus().getLen();
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "getCutpoints with file=%s with size=%d", file.getPath(), storeFileInBytes);
                fileReader = ((HStoreFile)file).getReader().getHFileReader();
                carry = SpliceHFileUtil.addStoreFileCutpoints(cutPoints, fileReader, storeFileInBytes, carry, range, splitBlockSize);
            }
        }

        if (storeFiles.size() > 1) {  // have to sort, hopefully will not happen a lot if major compaction is working properly...
            Collections.sort(cutPoints, new Comparator<byte[]>() {
                @Override
                public int compare(byte[] left, byte[] right) {
                    return org.apache.hadoop.hbase.util.Bytes.compareTo(left, right);
                }
            });
        }

        // add region start key at beginning
        cutPoints.add(0, store.getRegionInfo().getStartKey());
        // add region end key at end
        cutPoints.add(store.getRegionInfo().getEndKey());

        if (LOG.isDebugEnabled()) {
            RegionInfo regionInfo = store.getRegionInfo();
            String startKey = "\"" + CellUtils.toHex(regionInfo.getStartKey()) + "\"";
            String endKey = "\"" + CellUtils.toHex(regionInfo.getEndKey()) + "\"";
            LOG.debug("Cutpoints for " + regionInfo.getRegionNameAsString() + " [" + startKey + "," + endKey + "]: ");
            for (byte[] cutpoint : cutPoints) {
                LOG.debug("\t" + CellUtils.toHex(cutpoint));
            }
        }
        return cutPoints;
    }


    public static BitSet keyExists(boolean hasConstraintChecker, Store store, Pair<KVPair, Lock>[] dataAndLocks) throws IOException {
        BitSet bitSet = new BitSet(dataAndLocks.length);
        if (! (store instanceof HStore)) {
            return null;
        }

        HStore hstore = (HStore)store;
        hstore.lock.readLock().lock();
        Collection<? extends StoreFile> storeFiles;
        try {
            storeFiles = store.getStorefiles();
                /*
                 * Apparently, there's an issue where, when you first start up an HBase instance, if you
                 * call this code directly, you can break. In essence, there are no storefiles, so it goes
                 * to the memstore, where SOMETHING (and I don't know what) causes it to mistakenly return
                 * false,
                 * which tells the writing code that it's safe to write, resulting in some missing Primary Key
                 * errors.
                 *
                 * And in practice, it doesn't do you much good to check the memstore if there are no store
                 * files,
                 * since you'll just have to turn around and check the memstore again when you go to perform
                 * your
                 * get/scan. So may as well save ourselves the extra effort and skip operation if there are no
                  * store
                 * files to check.
                 */
            if (storeFiles.size() <= 0) return null;
            StoreFileReader fileReader;

            // Check Store Files

            byte[][] keys = new byte[dataAndLocks.length][];
            int[] keyOffset = new int[dataAndLocks.length];
            int[] keyLength = new int[dataAndLocks.length];
            for (int i =0; i<dataAndLocks.length;i++) {
                if(dataAndLocks[i]==null) continue;
                if(hasConstraintChecker || !KVPair.Type.INSERT.equals(dataAndLocks[i].getFirst().getType())) {
                    keys[i] = dataAndLocks[i].getFirst().getRowKey(); // Remove Array Copy (Is this buffered?)...
                    keyOffset[i] = 0;
                    keyLength[i] = keys[i].length;
                }
            }

            for (StoreFile file : storeFiles) {
                if (file != null) {
                    HStoreFile hStoreFile = (HStoreFile)file;
                    hStoreFile.initReader();
                    fileReader = hStoreFile.getReader();
                    BloomFilter bloomFilter = fileReader.generalBloomFilter;
                    if (bloomFilter == null)
                        bitSet.set(0,dataAndLocks.length); // Low level race condition, need to go to scan
                    else {
                        for (int j = 0; j<keys.length; j++) {
                            if (bloomFilter.contains(keys[j],keyOffset[j],keyLength[j],(ByteBuff) null))
                                bitSet.set(j);
                        }
                    }
                }
            }
            Segment memstore = getKvset(hstore);
            Segment snapshot = getSnapshot(hstore);
            for (int i =0; i<dataAndLocks.length;i++) {
                if(dataAndLocks[i]==null) continue;
                byte[] key = dataAndLocks[i].getFirst().getRowKey();
                if(hasConstraintChecker || !KVPair.Type.INSERT.equals(dataAndLocks[i].getFirst().getType())) {
                    if (!bitSet.get(i)) {
                        Cell kv = new KeyValue(key,
                                SIConstants.DEFAULT_FAMILY_BYTES,
                                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                                HConstants.LATEST_TIMESTAMP,
                                HConstants.EMPTY_BYTE_ARRAY);
                        bitSet.set(i, checkMemstore(memstore, key, kv) || checkMemstore(snapshot, key, kv));
                    }
                }
            }
            return bitSet;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw ioe;
        } finally {
            hstore.lock.readLock().unlock();
        }
    }

    protected static boolean checkMemstore(Segment kvSet, byte[] key, Cell kv) {
        Cell placeHolder;
        try {
            SortedSet<Cell> cellSet = kvSet.tailSet(kv);
            placeHolder = cellSet.isEmpty() ? null : cellSet.first();
            if (placeHolder != null && CellUtil.matchingRow(placeHolder, key))
                return true;
        } catch (NoSuchElementException ignored) {
        } // This keeps us from constantly performing key value comparisons for empty set
        return false;
    }

    public static Segment getKvset(HStore store) {
        return ((DefaultMemStore) store.memstore).active;
    }

    public static Segment getSnapshot(HStore store) {
        return ((DefaultMemStore) store.memstore).snapshot;
    }

    public static RegionScanner getScanner(HRegion region, Scan scan, List<KeyValueScanner> keyValueScanners) throws IOException {
        return region.getScanner(scan, keyValueScanners);
    }
}
