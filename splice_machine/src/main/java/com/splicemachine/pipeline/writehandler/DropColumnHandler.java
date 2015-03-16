package com.splicemachine.pipeline.writehandler;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.altertable.ConglomerateLoader;
import com.splicemachine.derby.impl.sql.execute.altertable.RowTransformer;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.List;

/**
 * User: jyuan
 * Date: 3/12/14
 */
public class DropColumnHandler implements WriteHandler {

    private RowTransformer rowTransformer;
    private ConglomerateLoader loader;
    private boolean failed;

    public DropColumnHandler(UUID tableId,
                             long toConglomId,
                             TxnView txn,
                             ColumnInfo[] columnInfos,
                             int droppedColumnPosition) {
        rowTransformer = new RowTransformer(tableId, txn, columnInfos, droppedColumnPosition);
        loader = new ConglomerateLoader(toConglomId, txn, false);
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {

        try {
            KVPair newPair;
            if (mutation.getType() == KVPair.Type.DELETE) {
                newPair = mutation;
            } else {
                // create a KeyValue for the mutation
                KeyValue kv = new KeyValue(mutation.getRowKey(), SpliceConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES,mutation.getValue());
                newPair = rowTransformer.transform(kv);
            }
            loader.add(newPair);
        } catch (Exception e) {
            failed = true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }

        if (!failed) {
            ctx.sendUpstream(mutation);
        }

    }

    @Override
    public void next(List<KVPair> mutations, WriteContext ctx) {
        throw new RuntimeException("Not Supported");
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        try {
            loader.flush();
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            Closeables.closeQuietly(rowTransformer);
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        // No Op
    }
}
