package org.apache.hadoop.hbase.regionserver;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.StorageConfiguration;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.util.BloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.parquet.bytes.BytesUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.BitSet;
import java.util.concurrent.locks.Lock;

/**
 * Class for accessing protected methods in HBase.
 *
 * @author johnleach
 */
public class HRegionUtil extends BaseHRegionUtil{
    private static final Logger LOG=Logger.getLogger(HRegionUtil.class);
    private static int splitBlockSize = HConfiguration.INSTANCE.getInt(StorageConfiguration.SPLIT_BLOCK_SIZE);
    public static void lockStore(Store store){
        ((HStore)store).lock.readLock().lock();
    }

    public static void unlockStore(Store store){
        ((HStore)store).lock.readLock().unlock();
    }

    public static void updateWriteRequests(HRegion region,long numWrites){
        HBasePlatformUtils.updateWriteRequests(region,numWrites);
    }

    public static void updateReadRequests(HRegion region,long numReads){
        HBasePlatformUtils.updateReadRequests(region,numReads);
    }

    public static List<byte[]> getCutpoints(Store store, byte[] start, byte[] end) throws IOException {
        assert Bytes.compareTo(start, end) <= 0 || start.length == 0 || end.length == 0;
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getCutpoints");
        Collection<StoreFile> storeFiles;
        storeFiles = store.getStorefiles();
        HFile.Reader fileReader;
        List<byte[]> cutPoints = new ArrayList<byte[]>();
        int carry = 0;
        byte[] regionStart = store.getRegionInfo().getStartKey();
        byte[] regionEnd = store.getRegionInfo().getEndKey();
        if (regionStart != null && regionStart.length > 0) {
            if (start == null || Bytes.compareTo(start, regionStart) < 0) {
                start = regionStart;
            }
        }
        if (regionEnd != null && regionEnd.length > 0) {
            if (end == null || Bytes.compareTo(end, regionEnd) > 0) {
                end = regionEnd;
            }
        }


        Pair<byte[], byte[]> range = new Pair<>(start, end);
//        double multiplier = Math.pow(1.05, storeFiles.size());
        for (StoreFile file : storeFiles) {
            if (file != null) {
                long storeFileInBytes = file.getFileInfo().getFileStatus().getLen();
                // If we have many store files, partitions will be harder to estimate and will tend to be bigger, apply a correction factor
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "getCutpoints with file=%s with size=%d", file.getPath(), storeFileInBytes);
                fileReader = file.createReader().getHFileReader();
                carry = addStoreFileCutpoints(cutPoints, fileReader, storeFileInBytes, carry, range);
            }
        }

        if (storeFiles.size() > 1) {              // have to sort, hopefully will not happen a lot if major compaction is working properly...
            Collections.sort(cutPoints, new Comparator<byte[]>() {
                @Override
                public int compare(byte[] left, byte[] right) {
                    return Bytes.compareTo(left, right);
                }
            });
        }
        if (LOG.isTraceEnabled()) {
            HRegionInfo regionInfo = store.getRegionInfo();
            String startKey = "\"" + Bytes.toStringBinary(regionInfo.getStartKey()) + "\"";
            String endKey = "\"" + Bytes.toStringBinary(regionInfo.getEndKey()) + "\"";
            LOG.trace("Cutpoints for " + regionInfo.getRegionNameAsString() + " [" + startKey + "," + endKey + "]: ");
            for (byte[] cutpoint : cutPoints) {
                LOG.trace("\t" + Bytes.toStringBinary(cutpoint));
            }
        }
        return cutPoints;
    }

    private static int addStoreFileCutpoints(List<byte[]> cutpoints, HFile.Reader fileReader, long storeFileInBytes, int carry, Pair<byte[],byte[]> range) throws IOException {
        HFileBlockIndex.BlockIndexReader indexReader = fileReader.getDataBlockIndexReader();
        int size = indexReader.getRootBlockCount();
        int levels = fileReader.getTrailer().getNumDataIndexLevels();
        if (levels == 1) {
            int incrementalSize = (int) (size > 0 ? storeFileInBytes / (float) size : storeFileInBytes);
            int sizeCounter = 0;
            for (int i = 0; i < size; ++i) {
                if (sizeCounter >= splitBlockSize) {
                    sizeCounter = 0;
                    KeyValue tentative = KeyValue.createKeyValueFromKey(indexReader.getRootBlockKey(i));
                    if (CellUtils.isKeyValueInRange(tentative, range)) {
                        cutpoints.add(tentative.getRow());
                    }
                }
                sizeCounter += incrementalSize;
            }
	        return sizeCounter;
        } else {
            for (int i = 0; i < size; ++i) {
                HFileBlock block = fileReader.readBlock(
                        indexReader.getRootBlockOffset(i),
                        indexReader.getRootBlockDataSize(i),
                        true, true, false, true,
                        levels == 2 ? BlockType.LEAF_INDEX : BlockType.INTERMEDIATE_INDEX,
                        fileReader.getDataBlockEncoding());
                carry = addIndexCutpoints(fileReader, block.getBufferWithoutHeader(), levels - 1,  cutpoints, storeFileInBytes / size, carry, range);
            }
	        return carry;
        }
    }

    /**
     * The number of bytes stored in each "secondary index" entry in addition to
     * key bytes in the non-root index block format. The first long is the file
     * offset of the deeper-level block the entry points to, and the int that
     * follows is that block's on-disk size without including header.
     */
    static final int SECONDARY_INDEX_ENTRY_OVERHEAD = Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG;

    private static int addIndexCutpoints(HFile.Reader fileReader, ByteBuffer nonRootIndex, int level, List<byte[]> cutpoints, long storeFileInBytes, int carriedSize, Pair<byte[], byte[]> range) throws IOException {
        int numEntries = nonRootIndex.getInt(0);
        // Entries start after the number of entries and the secondary index.
        // The secondary index takes numEntries + 1 ints.
        int entriesOffset = Bytes.SIZEOF_INT * (numEntries + 2);

        if (level > 1) {
            // Intermediate index
            for (int i = 0; i < numEntries; ++i) {
                // Targetkey's offset relative to the end of secondary index
                int targetEntryRelOffset = nonRootIndex.getInt(Bytes.SIZEOF_INT * (i + 1));

                // The offset of the target key in the blockIndex buffer
                int targetEntryOffset = entriesOffset     // Skip secondary index
                        + targetEntryRelOffset;           // Skip all entries until mid

                long offset = nonRootIndex.getLong(targetEntryOffset);
                int onDiskSize = nonRootIndex.getInt(targetEntryOffset + Bytes.SIZEOF_LONG);

                HFileBlock block = fileReader.readBlock(
                        offset,
                        onDiskSize,
                        true, true, false, true,
                        level == 2 ? BlockType.LEAF_INDEX : BlockType.INTERMEDIATE_INDEX,
                        fileReader.getDataBlockEncoding());
                carriedSize = addIndexCutpoints(fileReader, block.getBufferWithoutHeader(), level - 1, cutpoints, storeFileInBytes / numEntries, carriedSize, range);
            }
            return carriedSize;
        } else {
            // Leaf index
            int incrementalSize = storeFileInBytes > numEntries ? (int) (storeFileInBytes / numEntries) : 1;
            int step = splitBlockSize > incrementalSize ? splitBlockSize / incrementalSize : 1;
            int firstStep = (splitBlockSize - carriedSize) / incrementalSize;

            int previous = 0;
            for (int i = firstStep; i < numEntries; previous = i, i += step) {
                carriedSize = 0; // We add a cutpoint, reset carried size

                // Targetkey's offset relative to the end of secondary index
                int targetKeyRelOffset = nonRootIndex.getInt(
                        Bytes.SIZEOF_INT * (i + 1));

                // The offset of the target key in the blockIndex buffer
                int targetKeyOffset = entriesOffset     // Skip secondary index
                        + targetKeyRelOffset               // Skip all entries until mid
                        + SECONDARY_INDEX_ENTRY_OVERHEAD;  // Skip offset and on-disk-size

                // We subtract the two consecutive secondary index elements, which
                // gives us the size of the whole (offset, onDiskSize, key) tuple. We
                // then need to subtract the overhead of offset and onDiskSize.
                int targetKeyLength = nonRootIndex.getInt(Bytes.SIZEOF_INT * (i + 2)) -
                        targetKeyRelOffset - SECONDARY_INDEX_ENTRY_OVERHEAD;

                ByteBuffer dup = nonRootIndex.duplicate();
                dup.position(targetKeyOffset);
                dup.limit(targetKeyOffset + targetKeyLength);
                KeyValue tentative = KeyValue.createKeyValueFromKey(dup.slice());
                if (CellUtils.isKeyValueInRange(tentative, range)) {
                    cutpoints.add(tentative.getRow());
                }
            }
            return (numEntries - previous) * incrementalSize + carriedSize;
        }
    }


    public static BitSet keyExists(boolean hasConstraintChecker, Store store, Pair<KVPair, Lock>[] dataAndLocks) throws IOException {
        BitSet bitSet = new BitSet(dataAndLocks.length);
            if (! (store instanceof HStore)) {
                return null;
            }

            HStore hstore = (HStore)store;
            hstore.lock.readLock().lock();
            Collection<StoreFile> storeFiles;
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
                StoreFile.Reader fileReader;

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
                        fileReader = file.createReader();
                        BloomFilter bloomFilter = fileReader.generalBloomFilter;
                        if (bloomFilter == null)
                            bitSet.set(0,dataAndLocks.length); // Low level race condition, need to go to scan
                        else {
                            BitSet returnedBitSet = bloomFilter.contains(keys, keyOffset, keyLength, (ByteBuffer) null);
                            bitSet.or(returnedBitSet);
                        }
                    }
                }
                NavigableSet<Cell> memstore = getKvset(hstore);
                NavigableSet<Cell> snapshot = getSnapshot(hstore);
                for (int i =0; i<dataAndLocks.length;i++) {
                    byte[] key = dataAndLocks[i].getFirst().getRowKey();
                    if(dataAndLocks[i]==null) continue;
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

        protected static boolean checkMemstore(NavigableSet<Cell> kvSet, byte[] key, Cell kv) {
            Cell placeHolder;
            try {
                kvSet = kvSet.tailSet(kv,true);
                placeHolder = kvSet.isEmpty() ? null : (Cell)kvSet.first();
                if (placeHolder != null && CellUtil.matchingRow(placeHolder, key))
                    return true;
            } catch (NoSuchElementException ignored) {
            } // This keeps us from constantly performing key value comparisons for empty set
            return false;
        }

    public static NavigableSet<Cell> getKvset(HStore store) {
        return ((DefaultMemStore) store.memstore).cellSet;
    }

    public static NavigableSet<Cell> getSnapshot(HStore store) {
        return ((DefaultMemStore) store.memstore).snapshot;
    }



}
