package com.splicemachine.derby.stream.temporary.delete;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.stream.temporary.AbstractTableWriter;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.TxnView;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by jleach on 5/5/15.
 */
public class DeleteTableWriter extends AbstractTableWriter {
    private static final FixedDataHash EMPTY_VALUES_ENCODER = new FixedDataHash(new byte[]{});
    protected static final KVPair.Type dataType = KVPair.Type.DELETE;
    protected WriteCoordinator writeCoordinator;
    protected RecordingCallBuffer<KVPair> writeBuffer;
    protected PairEncoder encoder;
    public int rowsDeleted = 0;

    public DeleteTableWriter(TxnView txn, long heapConglom) throws StandardException {
        super(txn,heapConglom);
    }
    public void open() throws StandardException {
        writeCoordinator = SpliceDriver.driver().getTableWriter();
        writeBuffer = writeCoordinator.writeBuffer(destinationTable,
                txn, Metrics.noOpMetricFactory());
        encoder = new PairEncoder(getKeyEncoder(), getRowHash(), dataType);

    }
    public KeyEncoder getKeyEncoder() throws StandardException {
        return new KeyEncoder(NoOpPrefix.INSTANCE,new DataHash<ExecRow>(){
            private ExecRow currentRow;

            @Override
            public void setRow(ExecRow rowToEncode) {
                this.currentRow = rowToEncode;
            }

            @Override
            public byte[] encode() throws StandardException, IOException {
                RowLocation location = (RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject();
                return location.getBytes();
            }

            @Override public void close() throws IOException {  }

            @Override public KeyHashDecoder getDecoder() {
                return NoOpKeyHashDecoder.INSTANCE;
            }
        },NoOpPostfix.INSTANCE);
    }

    public DataHash getRowHash() throws StandardException {
        return EMPTY_VALUES_ENCODER;
    }


    public void close() throws StandardException {
        try {
            writeBuffer.flushBuffer();
            writeBuffer.close();
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    };

    public void delete(ExecRow execRow) throws StandardException {
        try {
            KVPair encode = encoder.encode(execRow);
            rowsDeleted++;
            writeBuffer.add(encode);
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    public void delete(Iterator<ExecRow> execRows) throws StandardException {
        while (execRows.hasNext())
            delete(execRows.next());
    }

    public void write(ExecRow execRow) throws StandardException {
        delete(execRow);
    }

    public void write(Iterator<ExecRow> execRows) throws StandardException {
        delete(execRows);
    }

}