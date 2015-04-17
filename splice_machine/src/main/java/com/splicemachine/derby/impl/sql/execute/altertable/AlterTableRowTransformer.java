package com.splicemachine.derby.impl.sql.execute.altertable;

import java.io.IOException;

import com.google.common.io.Closeables;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RowTransformer;

/**
 * Used by alter table write interceptors to map rows written in src table to new
 * target table.
 * <p/>
 * This class is driven by its exec row definitions and its encoder/decoder.<br/>
 * These are created for a specific alter table action in specific TransformingDDLDescriptors
 * specializations.
 */
public class AlterTableRowTransformer implements RowTransformer {
    private final ExecRow srcRow;
    private final ExecRow targetRow;
    private final EntryDataDecoder rowDecoder;
    private final KeyHashDecoder keyDecoder;
    private final PairEncoder entryEncoder;

    public AlterTableRowTransformer(ExecRow srcRow,
                            ExecRow targetRow,
                            KeyHashDecoder keyDecoder,
                            EntryDataDecoder rowDecoder,
                            PairEncoder entryEncoder) {
        this.srcRow = srcRow;
        this.targetRow = targetRow;
        this.rowDecoder = rowDecoder;
        this.keyDecoder = keyDecoder;
        this.entryEncoder = entryEncoder;
    }

    public KVPair transform(KVPair kvPair) throws StandardException, IOException {
        // Decode a row
        srcRow.resetRowArray();
        if (srcRow.nColumns() > 0) {
            keyDecoder.set(kvPair.getRowKey(), 0, kvPair.getRowKey().length);
            keyDecoder.decode(srcRow);

            rowDecoder.set(kvPair.getValue(), 0, kvPair.getValue().length);
            rowDecoder.decode(srcRow);
        }

        // encode the result
        KVPair newPair = entryEncoder.encode(targetRow);
        return newPair;
    }

    @Override
    public void close() {
        Closeables.closeQuietly(keyDecoder);
        Closeables.closeQuietly(rowDecoder);
        Closeables.closeQuietly(entryEncoder);
    }
}
