package com.splicemachine.io.hbase.encoding;

import com.google.common.collect.ImmutableMap;
import com.splicemachine.orc.DiskRange;
import com.splicemachine.orc.OrcDataSource;
import com.splicemachine.orc.OrcDataSourceId;
import com.splicemachine.utils.SpliceLogUtils;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slices;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 * ORC DataSource used to read the columnar portion of an HBase Block at Splice Machine.
 *
 */
public class BlockOrcDataSource implements OrcDataSource {
    private static Logger LOG = Logger.getLogger(BlockOrcDataSource.class);
    public static final OrcDataSourceId orcDataSourceId = new OrcDataSourceId("1");
    ByteBuffer byteBuffer;
    long offset;
    long readBytes;
    long size;
    long readTimeNanos;


    public BlockOrcDataSource(ByteBuffer byteBuffer) {
        this(byteBuffer,0l);
    }

    /**
     *
     * This instance method rewinds the buffer and uses the offset as an absolute offset for the ByteBuffer.
     *
     *
     * @param byteBuffer
     * @param offset Absolute Offset in the buffer
     */
    public BlockOrcDataSource(ByteBuffer byteBuffer, long offset) {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"BlockOrcDataSource with byteBuffer position=%d, remaining=%d, offset=%d, sourceOffset=%d",byteBuffer.position(),byteBuffer.remaining(),byteBuffer.arrayOffset(),offset);
        byteBuffer.rewind();
        this.byteBuffer= byteBuffer;
        this.offset = offset; // Total Offset in Buffer for the data source
        this.size = byteBuffer.remaining() - offset; // Total Size
    }

    /**
     * Bytes read
     *
     * @return
     */
    @Override
    public long getReadBytes() {
        return readBytes;
    }

    /**
     *
     * Read time in Nanos.
     *
     * @return
     */
    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    /**
     *
     * ByteBuffer Size - Offset
     *
     * @return
     */
    @Override
    public long getSize() {
        return size;
    }

    @Override
    public ByteBuffer readFully(long position, int bufferLength) throws IOException {
        byteBuffer.position((int)position);
        byteBuffer.limit((int)position+bufferLength);
        return byteBuffer.slice();
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        readFully(position,buffer,0,buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength) throws IOException {
        throw new UnsupportedOperationException("Byte Copy Not Allowed");
/**        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"ReadFully position=%d, offset=%d, bufferOffset=%d, bufferLength=%d ",position,offset, bufferOffset,bufferLength);
        System.out.println("Byte Copy?");
        Thread.dumpStack();
        byteBuffer.rewind();
        long start = System.nanoTime();
        byteBuffer.position((int) (position+offset) );
        byteBuffer.get(buffer,bufferOffset,bufferLength);
        readTimeNanos += System.nanoTime() - start;
        readBytes += bufferLength;
 */
    }

    /**
     *
     * Not Supported.  We should not have range merge in our reader.
     *
     * @param diskRanges
     * @param <K>
     * @return
     * @throws IOException
     */
    @Override
    public <K> Map<K, FixedLengthSliceInput> readFully(Map<K, DiskRange> diskRanges) throws IOException {
        ImmutableMap.Builder<K, FixedLengthSliceInput> builder = ImmutableMap.builder();
        byteBuffer.rewind();
        // Assumption here: all disk ranges are in the same region. Therefore, serving them in arbitrary order
        // will not result in eviction of cache that otherwise could have served any of the DiskRanges provided.
        for (Map.Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            DiskRange diskRange = entry.getValue();
            byteBuffer.rewind();
            if (diskRange.getLength() == 0)
                builder.put(entry.getKey(),Slices.wrappedBuffer(new byte[0]).getInput());
            else {
                byteBuffer.limit((int) diskRange.getOffset() + diskRange.getLength());
                byteBuffer.position((int) diskRange.getOffset());
                builder.put(entry.getKey(), Slices.wrappedBuffer(byteBuffer.slice()).getInput());
            }
        } // Cut it up?
        return builder.build();
    }

    /**
     *
     * Close...
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"close");
    }

    /**
     *
     * Get Datasource ID.  (Static)
     *
     * @return
     */
    @Override
    public OrcDataSourceId getId() {
        return orcDataSourceId;
    }
}
