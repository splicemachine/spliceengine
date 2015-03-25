package com.splicemachine.derby.impl.sql.execute.altertable;

import java.io.IOException;

import com.google.common.io.Closeables;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.NoOpDataHash;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.derby.utils.marshall.SaltedPrefix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.exception.SpliceDoNotRetryIOException;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.uuid.UUIDGenerator;

/**
 * Used by add column write interceptor to map rows written to old table to new table with default
 * values for new column.
 */
public class AddColumnRowTransformer implements RowTransformer {

    private final ExecRow oldRow;
    private final ExecRow newRow;
    private final EntryDataDecoder rowDecoder;
    private final KeyHashDecoder keyDecoder;
    private final PairEncoder entryEncoder;

    private AddColumnRowTransformer(ExecRow oldRow,
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

    public static AddColumnRowTransformer create(String tableVersion, int[] columnOrdering, ColumnInfo[] columnInfos)
        throws SpliceDoNotRetryIOException {
        // template rows
        ExecRow oldRow = new ValueRow(columnInfos.length-1);
        ExecRow newRow = new ValueRow(columnInfos.length);

        try {
            for (int i=0; i<columnInfos.length-1; i++) {
                DataValueDescriptor dvd = columnInfos[i].dataType.getNull();
                oldRow.setColumn(i+1,dvd);
                newRow.setColumn(i+1,dvd);
            }
            // set default value if given
            DataValueDescriptor newColDefaultValue = columnInfos[columnInfos.length-1].defaultValue;
            newRow.setColumn(columnInfos.length,
                             (newColDefaultValue != null ? newColDefaultValue : columnInfos[columnInfos.length-1].dataType.getNull()));
        } catch (StandardException e) {
            throw Exceptions.getIOException(e);
        }

        // key decoder
        KeyHashDecoder keyDecoder;
        if(columnOrdering!=null && columnOrdering.length>0){
            DescriptorSerializer[] oldDenseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(oldRow);
            keyDecoder = BareKeyHash.decoder(columnOrdering, null, oldDenseSerializers);
        }else{
            keyDecoder = NoOpDataHash.instance().getDecoder();
        }

        // row decoder
        DescriptorSerializer[] oldSerializers =
            VersionedSerializers.forVersion(tableVersion, true).getSerializers(oldRow);
        EntryDataDecoder rowDecoder = new EntryDataDecoder(IntArrays.count(oldRow.nColumns()),null,oldSerializers);

        // Row encoder
        KeyEncoder encoder;
        DescriptorSerializer[] newSerializers =
            VersionedSerializers.forVersion(tableVersion, true).getSerializers(newRow);
        if(columnOrdering !=null&& columnOrdering.length>0){
            //must use dense encodings in the key
            DescriptorSerializer[] denseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(newRow);
            encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(columnOrdering, null,
                                                                              denseSerializers), NoOpPostfix.INSTANCE);
        } else {
            UUIDGenerator uuidGenerator = SpliceDriver.driver().getUUIDGenerator().newGenerator(100);
            encoder = new KeyEncoder(new SaltedPrefix(uuidGenerator), NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
        }
        int[] columns = IntArrays.count(newRow.nColumns());

        if (columnOrdering != null && columnOrdering.length > 0) {
            for (int col: columnOrdering) {
                columns[col] = -1;
            }
        }
        DataHash rowHash = new EntryDataHash(columns, null,newSerializers);
        PairEncoder rowEncoder = new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);

        return new AddColumnRowTransformer(oldRow, newRow, keyDecoder, rowDecoder, rowEncoder);
    }
}
