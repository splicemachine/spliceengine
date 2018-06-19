package com.splicemachine.io.hbase.encoding;

import com.splicemachine.art.ReadOnlyArt;
import com.splicemachine.art.node.Base;
import com.splicemachine.orc.OrcRecordReader;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 *
 * Encoded Seeker Implementation for traversing the tree and the columnar buffer
 *
 */
public class PAXEncodedSeeker implements DataBlockEncoder.EncodedSeeker {
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.getDefault();
    private static Logger LOG = Logger.getLogger(PAXEncodedSeeker.class);
    public static final int HBASE_OFFSET = 4;
    protected boolean includeMvccVersion;
//    protected ByteBuffer buffer;
    private ReadOnlyArt readOnlyArt;
    private Iterator<ByteBuffer[]> rowIterator;
    private Cell currentCell;
    private LazyColumnarSeeker lazyColumnarSeeker;
    public PAXEncodedSeeker(boolean includeMvccVersion) {
        this.includeMvccVersion = includeMvccVersion;
    }

    static {
        OrcRecordReader.allowCaching = false; // Do not copy buffers, expensive.
    }

    @Override
    public void setCurrentBuffer(ByteBuffer buffer) {
        try {
                int passedInBufferPosition = buffer.position();
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"setCurrentBuffer with position=%d, arrayOffset=%d"
                        ,passedInBufferPosition,passedInBufferPosition);
            int sizeOfTree = buffer.getInt();
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"sizeOfTree=%d",sizeOfTree);
            readOnlyArt = new ReadOnlyArt(buffer.slice());
            buffer.position(sizeOfTree+passedInBufferPosition+HBASE_OFFSET);
            lazyColumnarSeeker = new LazyColumnarSeeker(buffer.slice());
            rewind();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public ByteBuffer getKeyDeepCopy() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(currentCell.getRowArray(),currentCell.getRowOffset(),currentCell.getRowLength());
        return byteBuffer.duplicate();
    }

    @Override
    public ByteBuffer getValueShallowCopy() {
        return ByteBuffer.wrap(currentCell.getRowArray(),currentCell.getRowOffset(),currentCell.getRowLength());
    }



    @Override
    public Cell getKeyValue() {
        return currentCell;
    }

    @Override
    public void rewind() {
        try {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "rewind seeker");
            rowIterator = readOnlyArt.getRootIterator();
            next();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean next() {
        try {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "next");
            if (!rowIterator.hasNext()) {
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "seeker#exhausted");
                currentCell = null;
                return false;
            }
            ByteBuffer[] value = rowIterator.next();
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "tree fetch key=%s, position=%d", Base.toHex(value[0].array()), Bytes.toInt(value[1].array()));
            int rowPosition = Base.U.getInt(value[1].array(),24L);
            currentCell = new PAXActiveCell(rowPosition,value[0].array(),lazyColumnarSeeker,Base.U.getLong(value[1].array(),16L));
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
        public int seekToKeyInBlock(byte[] key, int offset, int length, boolean seekBefore) {
        SpliceLogUtils.error(LOG,"seek to key in block seekBefore=%s, key=%s",seekBefore,Base.toHex(key,offset,length));
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"seek to key in block seekBefore=%s, key=%s",seekBefore,Base.toHex(key,offset,length));
        try {
            if (seekBefore) {
                Iterator<ByteBuffer[]> reverseIterator = readOnlyArt.getReverseIterator(key, offset, length, null, 0, 0);
                if (reverseIterator.next() == null) {


                }
            } else {
                rowIterator = readOnlyArt.getIterator(key, offset, length, null, 0, 0);
                next();
            }
            return 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int seekToKeyInBlock(Cell key, boolean seekBefore) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"seek to key in block cellKey=%s",Base.toHex(key.getRowArray(),key.getRowOffset(),key.getRowLength()));
        try {
            byte[] gen = PAXEncodingState.genRowKey(key);
            seekToKeyInBlock(key == null ? null : gen, key == null ? 0 : 0, key == null ? 0 : gen.length, seekBefore);
            if (currentCell != null && CellComparator.compareRows(key.getRowArray(),key.getRowOffset(),key.getRowLength(),
                    currentCell.getRowArray(),currentCell.getRowOffset(),currentCell.getRowLength()) == 0) {
                if (currentCell.getTimestamp() > key.getTimestamp())
                    next();
            }
            return 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareKey(KeyValue.KVComparator comparator, byte[] key, int offset, int length) {
        return comparator.compareFlatKey(key, offset, length,
                currentCell.getRowArray(), currentCell.getRowOffset(), currentCell.getRowLength());
    }

    @Override
    public int compareKey(KeyValue.KVComparator comparator, Cell key) {
        return comparator.compare(key, currentCell);
    }

}