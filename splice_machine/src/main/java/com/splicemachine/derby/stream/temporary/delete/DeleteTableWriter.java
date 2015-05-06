package com.splicemachine.derby.stream.temporary.delete;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.TxnView;

import java.io.IOException;

/**
 * Created by jleach on 5/5/15.
 */
public class DeleteTableWriter {
    private static final FixedDataHash EMPTY_VALUES_ENCODER = new FixedDataHash(new byte[]{});
    protected static final KVPair.Type dataType = KVPair.Type.DELETE;
    protected TxnView txn;
    WriteCoordinator writeCoordinator;

    public DeleteTableWriter() {
        // This is not right for the
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



}