package com.splicemachine.derby.impl.sql.execute.altertable;

import java.io.IOException;
import java.sql.SQLException;

import com.google.common.io.Closeables;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RowTransformer;

/**
 * Used by alter table write interceptors to map rows written to old table to new
 * table with default values for new column.
 */
public class AlterTableRowTransformer implements RowTransformer {
    private final ExecRow oldRow;
    private final ExecRow newRow;
    private final EntryDataDecoder rowDecoder;
    private final KeyHashDecoder keyDecoder;
    private final PairEncoder entryEncoder;

    public AlterTableRowTransformer(ExecRow oldRow,
                            ExecRow newRow,
                            KeyHashDecoder keyDecoder,
                            EntryDataDecoder rowDecoder,
                            PairEncoder entryEncoder) {
        this.oldRow = oldRow;
        this.newRow = newRow;
        this.rowDecoder = rowDecoder;
        this.keyDecoder = keyDecoder;
        this.entryEncoder = entryEncoder;
    }

    public KVPair transform(KVPair kvPair) throws StandardException, IOException {
        // Decode a row
        oldRow.resetRowArray();
        if (oldRow.nColumns() > 0) {
            keyDecoder.set(kvPair.getRowKey(), 0, kvPair.getRowKey().length);
            keyDecoder.decode(oldRow);

            rowDecoder.set(kvPair.getValue(), 0, kvPair.getValue().length);
            rowDecoder.decode(oldRow);
        }

        // encode the result
        KVPair newPair = entryEncoder.encode(newRow);

        // preserve the same row key
        newPair.setKey(kvPair.getRowKey());
        return newPair;
    }

    @Override
    public void close() {
        Closeables.closeQuietly(keyDecoder);
        Closeables.closeQuietly(rowDecoder);
        Closeables.closeQuietly(entryEncoder);
    }
}
