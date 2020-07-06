/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
        // TODO Sample memstore writes to generate cutpoints on the memstore
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getCutpoints store: %s requestedSplits: %d bytesPerSplit: %d", store.getStorefiles(), requestedSplits, bytesPerSplit);
        Collection<? extends StoreFile> storeFiles;
        storeFiles = store.getStorefiles();
        HFile.Reader fileReader = null;
        List<byte[]> cutPoints = new ArrayList<byte[]>();
        List<byte[]> finalCutPoints = new ArrayList<byte[]>();
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

        int minSplits = 0;
        if (bytesPerSplit == 0) {
            bytesPerSplit = HConfiguration.getConfiguration().getSplitBlockSize();
            // if the user hasn't explicitly set the number of splits, force a minimum
            minSplits = HConfiguration.getConfiguration().getSplitsPerRegionMin();
        }

        long totalStoreFileInBytes = 0;
        for (StoreFile file : storeFiles) {
            if (file != null) {
                totalStoreFileInBytes += getFileSize(file);
            }
        }
        // We use the MemStore size to estimate the right number of splits because we take it into account when
        // computing bytesPerSplit in AbstractSMInputFormat, even though we don't scan the memstore for split points
        // We hope the data in the MemStore follows a similar distribution to that in the HFiles
        long numSplits;
        numSplits = (totalStoreFileInBytes + store.getMemStoreSize().getDataSize()) / bytesPerSplit;

        long finalNumSplits = numSplits = Math.max(numSplits, minSplits);

        // We'll generate more splits if we have multiple store files and reassemble them later to get more accurate splits
        numSplits *= storeFiles.size();

        if (finalNumSplits > 1 && numSplits > 0) {
            // Here we don't take into account the MemStore size because we are only scanning the HFiles
            long bytesPerSplitEvenDistribution = totalStoreFileInBytes / numSplits;
            int splitBlockSize;
            if (bytesPerSplitEvenDistribution > Integer.MAX_VALUE) {
                splitBlockSize = Integer.MAX_VALUE;
            } else {
                splitBlockSize = (int) bytesPerSplitEvenDistribution;
            }

            Pair<byte[], byte[]> range = new Pair<>(start, end);
            for (StoreFile file : storeFiles) {
                if (file != null) {
                    long storeFileInBytes = getFileSize(file);
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
                // If we had more than one HFiles we generated more cutpoints than needed, take
                // only the amount we wanted. We need (#splits - 1) cutpoints
                int step = (int) (cutPoints.size() / (finalNumSplits - 1));
                if (step <= 1) { // we want all generated cutpoints
                    finalCutPoints = cutPoints;
                } else {
                    for (int i = step - 1; i < cutPoints.size(); i += step) {
                        finalCutPoints.add(cutPoints.get(i));
                    }
                }
            } else {
                finalCutPoints = cutPoints;
            }
        }

        // add region start key at beginning
        finalCutPoints.add(0, store.getRegionInfo().getStartKey());
        // add region end key at end
        finalCutPoints.add(store.getRegionInfo().getEndKey());
        if (LOG.isDebugEnabled()) {
            RegionInfo regionInfo = store.getRegionInfo();
            String startKey = "\"" + CellUtils.toHex(regionInfo.getStartKey()) + "\"";
            String endKey = "\"" + CellUtils.toHex(regionInfo.getEndKey()) + "\"";
            LOG.debug("Cutpoints for " + regionInfo.getRegionNameAsString() + " [" + startKey + "," + endKey + "]: ");
            for (byte[] cutpoint : finalCutPoints) {
                LOG.debug("\t" + CellUtils.toHex(cutpoint));
            }
        }
        return finalCutPoints;
    }

    // If it's a reference file, we are only reading half the HFile
    private static long getFileSize(StoreFile file) throws IOException {
        long size = ((HStoreFile)file).getFileInfo().getFileStatus().getLen();
        return ((HStoreFile)file).getFileInfo().isReference() ? size / 2 : size;
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
                                                   SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES,
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

    public static void replaceStoreFiles(HStore store, Collection<HStoreFile> compactedFiles, Collection<HStoreFile> result)
            throws IOException {
        store.replaceStoreFiles(compactedFiles, result);
    }
}
