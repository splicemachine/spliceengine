package com.splicemachine.io.hbase.encoding;

import com.splicemachine.access.impl.data.UnsafeRecord;
import com.splicemachine.access.impl.data.UnsafeRecordUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLTinyint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.hbase.MemstoreAwareObserver;
import com.splicemachine.orc.OrcReader;
import com.splicemachine.orc.OrcRecordReader;
import com.splicemachine.orc.input.ColumnarBatchRow;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.OrcMetadataReader;
import com.splicemachine.orc.predicate.SpliceORCPredicate;
import com.splicemachine.si.impl.SpliceQuery;
import com.splicemachine.utils.SpliceLogUtils;
import io.airlift.units.DataSize;
import org.apache.log4j.Logger;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTimeZone;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * Lazy Column Seeker that is wrapped into the cell returned from the seeker.  The goal
 * is to <b>not</b> decompress the columnar bits unless the scan needs to return an actual value.
 *
 *
 */
public class LazyColumnarSeeker {
    //public static byte[] EMPTY = new byte[]{};
    public byte[] EMPTY = new byte[] { (byte)0x00 };  // msirek-temp
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.getDefault();
    private static Logger LOG = Logger.getLogger(LazyColumnarSeeker.class);
    private ByteBuffer bb;
    private OrcRecordReader orcRecordReader;
    public ExecRow writtenExecRow;
    private StructType schema;
    private Iterator<ColumnarBatch.Row> currentIterator;
    private OrcReader reader;
    private Map<Integer,DataType> typeMap;
    private ColumnarBatchRow columnarBatchRow;
    private ColumnarBatch.Row row;
    private FormatableBitSet fbs;
    public static final DataSize maxMergeDistance = new DataSize(1, DataSize.Unit.BYTE);
    public static final DataSize maxReadSize = new DataSize(200, DataSize.Unit.MEGABYTE);
    public static final DataSize maxBlockSize = new DataSize(200, DataSize.Unit.MEGABYTE);
    private boolean splitSeeker;
    public static final String C1 = "C1";
    public static final String C2 = "C2";
    public static final String C3 = "C3";
    public static final String C4 = "C4";
    public static final String C5 = "C5";
    public static final String C6 = "C6";
    private UnsafeRecord unsafeRecord;
    private int columnarPosition;
    private boolean init;
    private int columnarBatchPosition;
    private ValueRow dataRow;

    public LazyColumnarSeeker(ByteBuffer bb) {
        this.bb = bb;
    }

    public void setByteBuffer(ByteBuffer bb) {
        this.bb = bb;
    }

    public void init() throws IOException, StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Lazy Seeker Init");
        BlockOrcDataSource orcDataSource = new BlockOrcDataSource(bb, 0l);
        reader = new OrcReader(orcDataSource, new OrcMetadataReader(),
                maxMergeDistance,
                maxReadSize,
                maxBlockSize
        );
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Lazy Seeker ORC Section: columnNames=%s",reader.getColumnNames());
        SpliceQuery spliceQuery = SpliceQuery.queryContext.get();
        ExecRow execRow = null;
        if (spliceQuery == null) {
            execRow = MemstoreAwareObserver.conglomerateThreadLocal.get(); // Are we splitting or compacting?
            if (execRow == null) {
                SpliceLogUtils.warn(LOG, "No Format for seeker...");
                splitSeeker = true;
                execRow = new ValueRow(0);
                fbs = new FormatableBitSet(0);
            } else {
                SpliceLogUtils.warn(LOG,"Seeker during split, move to debug once fully tested -> " + execRow);
                fbs = new FormatableBitSet(execRow.length());
                fbs.setAll();
            }
        } else {
            fbs = spliceQuery.getScanColumns();
            if (fbs == null) {
                throw new RuntimeException("fbs not passed into encodedSeaker");
            }
            execRow = spliceQuery.getTemplate();
            if (execRow == null) {
                throw new RuntimeException("execRow not passed into encodedSeaker");
            }
        }
        typeMap = new HashMap<>(execRow.nColumns()+6);
        writtenExecRow = new ValueRow(execRow.size() + 6);


        writtenExecRow.setColumn(1, new SQLLongint()); // Timestamp
        typeMap.put(0,writtenExecRow.getColumn(1).getStructField(C1).dataType());
        writtenExecRow.setColumn(2, new SQLTinyint()); // Type Byte
        typeMap.put(1,writtenExecRow.getColumn(2).getStructField(C2).dataType());
        writtenExecRow.setColumn(3, new SQLLongint()); // Sequence ID
        typeMap.put(2,writtenExecRow.getColumn(3).getStructField(C3).dataType());
        writtenExecRow.setColumn(4, new SQLBoolean()); // Tombstone
        typeMap.put(3,writtenExecRow.getColumn(4).getStructField(C4).dataType());
        writtenExecRow.setColumn(5, new SQLLongint()); // TxnID
        typeMap.put(4,writtenExecRow.getColumn(5).getStructField(C5).dataType());
        writtenExecRow.setColumn(6, new SQLLongint()); // Effective Timestamp
        typeMap.put(5,writtenExecRow.getColumn(6).getStructField(C6).dataType());

        for (int i = 1; i <= execRow.size(); i++) {
            writtenExecRow.setColumn(6 + i, execRow.getColumn(i));
        }
        int i = -1;
        int j = 0;
        while ( (i = fbs.anySetBit(i)) != -1) {
            typeMap.put(6+i,execRow.getColumn(j+1).getStructField("c"+j).dataType());
            j++;
        }
        schema = writtenExecRow.schema();
        unsafeRecord = new UnsafeRecord(
                EMPTY,
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(writtenExecRow.length())],
                0l, true);
    }

    public byte[] lazyFetchPosition(int seekPosition, byte[] rowKey, int offset, int length) throws IOException, StandardException {
        if (!init) {
            init();
            init = true;
        }
        if (orcRecordReader== null || seekPosition <= columnarPosition)
            orcRecordReader = reader.createRecordReader(typeMap, SpliceORCPredicate.TRUE,HIVE_STORAGE_TIME_ZONE,new AggregatedMemoryContext(), Collections.emptyList(),Collections.EMPTY_LIST);
        while (columnarBatchPosition - 1 + orcRecordReader.getCurrentBatchSize() < seekPosition) {
            // Batch Seek/Skip
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Batch Fetching");
            columnarPosition = columnarBatchPosition;
            columnarBatchPosition += orcRecordReader.nextBatch();
            currentIterator = null;
        }
        // Inter Scan Seek
        if (currentIterator == null) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "Current Iterator = null");
            ColumnarBatch columnarBatch = orcRecordReader.getColumnarBatch(schema);
            currentIterator = columnarBatch.rowIterator();
            currentIterator.hasNext();
            row = currentIterator.next();
        }
        while (columnarPosition < seekPosition) {
            try {
                currentIterator.hasNext();
                if (columnarPosition+1 == columnarBatchPosition) {
                    columnarBatchPosition +=orcRecordReader.nextBatch();
                    ColumnarBatch columnarBatch = orcRecordReader.getColumnarBatch(schema);
                    currentIterator = columnarBatch.rowIterator();
                    currentIterator.hasNext();
                    row = currentIterator.next();
                    columnarPosition++;
                } else {
                    row = currentIterator.next();
                    columnarPosition++;
                    if (LOG.isTraceEnabled())
                        SpliceLogUtils.trace(LOG, "nextColumnPosition: " + columnarPosition);
                }
            } catch (Exception e) {
                SpliceLogUtils.error(LOG,"columnarPosition=%d, columnarBatchPosition=%d, seekPosition=%d",columnarPosition,columnarBatchPosition,seekPosition);
                throw new RuntimeException(e);
            }
        }
        //columnarPosition++;
        // Write Data
        writtenExecRow = writtenExecRow.fromSparkRow(new ColumnarBatchRow(row, schema));
        long version = writtenExecRow.getColumn(1).getLong();
        unsafeRecord.setKey(rowKey,0,rowKey.length-1);
        unsafeRecord.setHasTombstone(writtenExecRow.getColumn(1).getBoolean());
        unsafeRecord.setTxnId1(writtenExecRow.getColumn(2).getLong());
        unsafeRecord.setEffectiveTimestamp(writtenExecRow.getColumn(3).getLong());
        unsafeRecord.setNumberOfColumns(writtenExecRow.length()-6);
        unsafeRecord.setVersion(version);
        if (dataRow == null)
            dataRow = new ValueRow(writtenExecRow.length()-6);
        // Do we need to do this multiple times?
        System.arraycopy(writtenExecRow.getRowArray(),6,dataRow.getRowArray(),0,writtenExecRow.length()-6);
        unsafeRecord.setData(fbs,dataRow.getRowArray());
        //System.out.println("writtenExecRow ->" + writtenExecRow);  msirek-temp
        return unsafeRecord.getValue();
    }
}