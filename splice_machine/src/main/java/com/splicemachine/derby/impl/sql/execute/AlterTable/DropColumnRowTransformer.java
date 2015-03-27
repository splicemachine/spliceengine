package com.splicemachine.derby.impl.sql.execute.altertable;

import com.google.common.io.Closeables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.exception.SpliceDoNotRetryIOException;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.uuid.UUIDGenerator;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

/**
 * User: jyuan
 * Date: 2/7/14
 */
public class DropColumnRowTransformer implements RowTransformer {

    private final ExecRow oldRow;
    private final ExecRow newRow;
    private final EntryDataDecoder rowDecoder;
    private final KeyHashDecoder keyDecoder;
    private final PairEncoder entryEncoder;

    private boolean initialized;


    private UUID tableId;
    private TxnView txn;
    private ColumnInfo[] columnInfos;
    private int droppedColumnPosition;
    private int[] oldColumnOrdering;

    private DropColumnRowTransformer(ExecRow oldRow,
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

    public static RowTransformer create(String tableVersion,
                                                 int[] oldColumnOrdering,
                                                 int[] newColumnOrdering,
                                                 ColumnInfo[] columnInfos,
                                                 int droppedColumnPosition) throws SpliceDoNotRetryIOException {
        // template rows
        ExecRow oldRow = new ValueRow(columnInfos.length);
        ExecRow newRow = new ValueRow(columnInfos.length-1);

        try {
            int i = 1;
            int j =1;
            for (ColumnInfo col:columnInfos){
                DataValueDescriptor dataValue = col.dataType.getNull();
                oldRow.setColumn(i, dataValue);
                if (i++ != droppedColumnPosition){
                    newRow.setColumn(j++, dataValue);
                }
            }
        } catch (StandardException e) {
            throw Exceptions.getIOException(e);
        }

        // key decoder
        KeyHashDecoder keyDecoder;
        if(oldColumnOrdering!=null && oldColumnOrdering.length>0){
            DescriptorSerializer[] oldDenseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(oldRow);
            keyDecoder = BareKeyHash.decoder(oldColumnOrdering, null, oldDenseSerializers);
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
        if(newColumnOrdering !=null&& newColumnOrdering.length>0){
            //must use dense encodings in the key
            DescriptorSerializer[] denseSerializers =
                VersionedSerializers.forVersion(tableVersion, false).getSerializers(newRow);
            encoder = new KeyEncoder(NoOpPrefix.INSTANCE, BareKeyHash.encoder(newColumnOrdering, null,
                                                                              denseSerializers), NoOpPostfix.INSTANCE);
        } else {
            UUIDGenerator uuidGenerator = SpliceDriver.driver().getUUIDGenerator().newGenerator(100);
            encoder = new KeyEncoder(new SaltedPrefix(uuidGenerator), NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
        }
        int[] columns = IntArrays.count(newRow.nColumns());

        if (oldColumnOrdering != null && oldColumnOrdering.length > 0) {
            for (int col: oldColumnOrdering) {
                columns[col] = -1;
            }
        }
        DataHash rowHash = new EntryDataHash(columns, null,newSerializers);
        PairEncoder rowEncoder = new PairEncoder(encoder,rowHash, KVPair.Type.INSERT);

        return new DropColumnRowTransformer(oldRow, newRow, keyDecoder, rowDecoder, rowEncoder);
    }

}
