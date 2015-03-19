package com.splicemachine.derby.impl.sql.execute.altertable;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.io.Closeables;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.utils.Pair;

/**
 * Used by add column write interceptor to map rows written to old table to new table with default
 * values for new column.
 */
public class AddColumnRowTransformer<Data> implements Closeable {
    private static SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();

    private final ExecRow oldRow;
    private final ExecRow newRow;
    private final EntryDataDecoder rowDecoder;
    private final KeyHashDecoder keyDecoder;
    private final PairEncoder entryEncoder;

    public AddColumnRowTransformer(Pair<ExecRow,ExecRow> oldNew,
                                   KeyHashDecoder keyDecoder,
                                   EntryDataDecoder rowDecoder,
                                   PairEncoder entryEncoder) {
        this.oldRow = oldNew.getFirst();
        this.newRow = oldNew.getSecond();
        this.rowDecoder = rowDecoder;
        this.keyDecoder = keyDecoder;
        this.entryEncoder = entryEncoder;
    }

    public KVPair transform(KVPair kvPair) throws StandardException, IOException {
        // Decode a row
        oldRow.resetRowArray();
        DataValueDescriptor[] oldFields = oldRow.getRowArray();
        if (oldFields.length != 0) {
            keyDecoder.set(kvPair.getRowKey(), 0, kvPair.getRowKey().length);
            keyDecoder.decode(oldRow);

            rowDecoder.set(kvPair.getValue(), 0, kvPair.getValue().length);
            rowDecoder.decode(oldRow);
        }

        // encode the result
        KVPair newPair = entryEncoder.encode(newRow);

        // preserve the old row key
        newPair.setKey(kvPair.getRowKey());
        return newPair;
    }

    public KVPair transform(Data kv) throws StandardException, IOException {
        // Decode a row
        oldRow.resetRowArray();
        DataValueDescriptor[] oldFields = oldRow.getRowArray();
        if (oldFields.length != 0) {
            keyDecoder.set(dataLib.getDataRowBuffer(kv), dataLib.getDataRowOffset(kv), dataLib.getDataRowlength(kv));
            keyDecoder.decode(oldRow);

            rowDecoder.set(dataLib.getDataValueBuffer(kv), dataLib.getDataValueOffset(kv), dataLib.getDataValuelength(kv));
            rowDecoder.decode(oldRow);
        }

        // encode the result
        KVPair newPair = entryEncoder.encode(newRow);

        // preserve the old row key
        newPair.setKey(dataLib.getDataRow(kv));
        return newPair;
    }

    @Override
    public void close() {
        Closeables.closeQuietly(keyDecoder);
        Closeables.closeQuietly(rowDecoder);
        Closeables.closeQuietly(entryEncoder);
    }

}
