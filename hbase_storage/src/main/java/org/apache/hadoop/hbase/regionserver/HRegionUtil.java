package org.apache.hadoop.hbase.regionserver;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.StorageConfiguration;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

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
        double multiplier = Math.pow(1.05, storeFiles.size());
        for (StoreFile file : storeFiles) {
            if (file != null) {
                long storeFileInBytes = file.getFileInfo().getFileStatus().getLen();
                // If we have many store files, partitions will be harder to estimate and will tend to be bigger, apply a correction factor
                long adjustedSize = (long) (storeFileInBytes * multiplier);
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "getCutpoints with file=%s with size=%d", file.getPath(), storeFileInBytes);
                fileReader = file.createReader().getHFileReader();
                carry = addStoreFileCutpoints(cutPoints, fileReader, adjustedSize, carry);
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
        return cutPoints;
    }


    private static int addStoreFileCutpoints(List<byte[]> cutpoints, HFile.Reader fileReader, long storeFileInBytes, int carry) throws IOException {
        HFileBlockIndex.BlockIndexReader indexReader = fileReader.getDataBlockIndexReader();
        int size = indexReader.getRootBlockCount();
        int levels = fileReader.getTrailer().getNumDataIndexLevels();
        if (levels == 1) {
            int incrementalSize = (int) (size > 0 ? storeFileInBytes / (float) size : storeFileInBytes);
            int sizeCounter = 0;
            for (int i = 0; i < size; ++i) {
                if (sizeCounter >= splitBlockSize) {
                    sizeCounter = 0;
                    cutpoints.add(KeyValue.createKeyValueFromKey(indexReader.getRootBlockKey(i)).getRow());
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
                carry = addIndexCutpoints(fileReader, block.getBufferWithoutHeader(), levels - 1,  cutpoints, storeFileInBytes / size, carry);
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

    private static int addIndexCutpoints(HFile.Reader fileReader, ByteBuffer nonRootIndex, int level, List<byte[]> cutpoints, long storeFileInBytes, int carriedSize) throws IOException {
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
                carriedSize = addIndexCutpoints(fileReader, block.getBufferWithoutHeader(), level - 1, cutpoints, storeFileInBytes / numEntries, carriedSize);
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
                cutpoints.add(KeyValue.createKeyValueFromKey(dup.slice()).getRow());
            }
            return (numEntries - previous) * incrementalSize + carriedSize;
        }
    }

}
