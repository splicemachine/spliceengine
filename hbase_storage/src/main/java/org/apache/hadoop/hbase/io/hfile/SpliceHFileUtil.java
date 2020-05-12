package org.apache.hadoop.hbase.io.hfile;

import com.splicemachine.hbase.CellUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by jyuan on 5/30/19.
 */
public class SpliceHFileUtil {
    private static final Logger LOG=Logger.getLogger(HFileUtil.class);

    public static int addStoreFileCutpoints(List<byte[]> cutpoints, HFile.Reader fileReader, long storeFileInBytes, int carry, Pair<byte[], byte[]> range, int splitBlockSize) throws IOException {
        HFileBlockIndex.BlockIndexReader indexReader = fileReader.getDataBlockIndexReader();
        int size = indexReader.getRootBlockCount();
        int levels = fileReader.getTrailer().getNumDataIndexLevels();
        if (LOG.isDebugEnabled())
            LOG.debug("addStoreFileCutpoints storeFileInBytes " + storeFileInBytes + " carry " + carry +
                    " splitBlockSize " + splitBlockSize + " size " + size + " levels " + levels);
        if (levels == 1) {
            int incrementalSize = (int) (size > 0 ? storeFileInBytes / (float) size : storeFileInBytes);
            int sizeCounter = carry;
            for (int i = 0; i < size; ++i) {
                if (sizeCounter >= splitBlockSize) {
                    sizeCounter -= splitBlockSize;
                    Cell blockKey = ((HFileBlockIndex.CellBasedKeyBlockIndexReader)indexReader).getRootBlockKey(i);
                    KeyValue tentative = (KeyValue)blockKey;
                    if (CellUtils.isKeyValueInRange(tentative, range)) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Adding cutpoint " + CellUtils.toHex(CellUtil.cloneRow(tentative)));
                        }
                        cutpoints.add(CellUtil.cloneRow(tentative));
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
                carry = addIndexCutpoints(fileReader, block.getBufferWithoutHeader(), levels - 1,  cutpoints, storeFileInBytes / size, carry, range, splitBlockSize, block.getHFileContext().getBlocksize());
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

    private static int addIndexCutpoints(HFile.Reader fileReader, ByteBuff nonRootIndex, int level, List<byte[]> cutpoints, long storeFileInBytes, int carriedSize, Pair<byte[], byte[]> range, int splitBlockSize, int blockSize) throws IOException {
        int numEntries = nonRootIndex.getInt(0);
        // Entries start after the number of entries and the secondary index.
        // The secondary index takes numEntries + 1 ints.
        int entriesOffset = Bytes.SIZEOF_INT * (numEntries + 2);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Block readable bytes: " + nonRootIndex.limit());
        }

        if (level > 1) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Processing intermediate index");
            }
            // Intermediate index
            for (int i = 0; i < numEntries; ++i) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Decoding element " + i + " of " + numEntries);
                }
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
                carriedSize = addIndexCutpoints(fileReader, block.getBufferWithoutHeader(), level - 1, cutpoints, storeFileInBytes / numEntries, carriedSize, range, splitBlockSize, block.getHFileContext().getBlocksize());
            }
            return carriedSize;
        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Processing leaf index");
            }

            // Leaf index
            int incrementalSize = blockSize;
            int step = splitBlockSize > incrementalSize ? splitBlockSize / incrementalSize : 1;
            int firstStep = carriedSize > splitBlockSize ? 0 : (splitBlockSize - carriedSize) / incrementalSize;

            int previous = 0;
            for (int i = firstStep; i < numEntries; previous = i, i += step) {
                carriedSize = 0; // We add a cutpoint, reset carried size

                if (LOG.isTraceEnabled()) {
                    LOG.trace("Decoding element " + i + " of " + numEntries);
                }

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

                ByteBuff dup = nonRootIndex.duplicate();
                dup.position(targetKeyOffset);
                dup.limit(targetKeyOffset + targetKeyLength);
                ByteBuff bb = dup.slice();
                KeyValue tentative = createKeyValueFromKey(bb.array(), bb.arrayOffset(), bb.limit());
                if (CellUtils.isKeyValueInRange(tentative, range)) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Adding cutpoint " + CellUtils.toHex(CellUtil.cloneRow(tentative)));
                    }
                    cutpoints.add(CellUtil.cloneRow(tentative));
                }
            }
            return (numEntries - previous) * incrementalSize + carriedSize;
        }
    }

    private static KeyValue createKeyValueFromKey(byte[] b, int o, int l) {
        byte[] newb = new byte[l + 8];
        System.arraycopy(b, o, newb, 8, l);
        org.apache.hadoop.hbase.util.Bytes.putInt(newb, 0, l);
        org.apache.hadoop.hbase.util.Bytes.putInt(newb, 4, 0);
        return new KeyValue(newb);
    }
}
