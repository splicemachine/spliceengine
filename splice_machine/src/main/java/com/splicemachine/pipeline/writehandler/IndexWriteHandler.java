package com.splicemachine.pipeline.writehandler;

import java.io.IOException;
import java.util.List;

import com.carrotsearch.hppc.BitSet;
import org.apache.log4j.Logger;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Intercepts UPDATE/UPSERT/INSERT/DELETE mutations to a base table and sends corresponding mutations to the index table.
 *
 * @author Scott Fines
 *         Created on: 5/1/13
 */
public class IndexWriteHandler extends AbstractIndexWriteHandler {

    private static final Logger LOG = Logger.getLogger(IndexWriteHandler.class);

    private final IndexTransformer transformer;
    private CallBuffer<KVPair> indexBuffer;
    private final int expectedWrites;

    public IndexWriteHandler(BitSet indexedColumns,
                             byte[] indexConglomBytes,
                             BitSet descColumns,
                             boolean keepState,
                             int expectedWrites,
                             IndexTransformer transformer){
        super(indexedColumns, indexConglomBytes, descColumns, keepState);
        this.expectedWrites = expectedWrites;
        this.transformer = transformer;
    }

    @Override
    public void next(List<KVPair> mutations, WriteContext ctx) {
        throw new RuntimeException("Not Supported");
    }

    @Override
    protected void subFlush(WriteContext ctx) throws Exception {
        if (indexBuffer != null) {
            indexBuffer.flushBuffer();
            // indexBuffer.close(); // Do not block
        }
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "flush");
        super.flush(ctx);
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "close");
        super.close(ctx);
    }

    @Override
    public void subClose(WriteContext ctx) throws Exception {
        if (indexBuffer != null) {
            indexBuffer.close(); // Blocks
        }
    }

    @Override
    protected boolean isHandledMutationType(KVPair.Type type) {
        return type == KVPair.Type.DELETE || type == KVPair.Type.CANCEL ||
            type == KVPair.Type.UPDATE || type == KVPair.Type.INSERT ||
            type == KVPair.Type.UPSERT || type == KVPair.Type.UNIQUE_UPDATE;
    }

    @Override
    public boolean updateIndex(KVPair mutation, WriteContext ctx) {
        failed = false;

        if (transformer.isUniqueIndex() && mutation.getType() == KVPair.Type.CANCEL) {
            return true;
        }

        ensureBufferReader(mutation, ctx);

        if (mutation.getType() == KVPair.Type.DELETE) {
            // If it's a DELETE, we can just delete the index and move on
            deleteIndex(mutation, ctx);
            return !failed;
        }

        if (mutation.getType().isUpdateOrUpsert()) {
            // Is this an UPDATE, UNIQUE_UPDATE or an UPSERT and we have ONLY updated primary key columns?  If so,
            // send the mutation to the index as it is.  The old value will have been deleted by the UpdateOperation
            // which sends a delete mutation when it sees that primary keys have changed.
            //
            // Otherwise...
            if (! transformer.primaryKeyUpdateOnly(mutation, ctx, indexedColumns)) {
                if(mutation.getType().equals(KVPair.Type.UNIQUE_UPDATE)) {
                    // DB-3315: Primary Key constraint not enforced
                    // If this is a unique index update, we need to change the type so that it passes constraint checking.
                    // The mutation type is shared between main table update and the index update. Uniqueness constraint
                    // check must be made against the main table update but, if that passes, there's no need to do it
                    // against the unique index update.
                    mutation.setType(KVPair.Type.UPDATE);
                }
                //
                //  To update the index now, we must find the old index row and delete it, then
                //  insert a new index row with the correct values.
                //
                //  The order of ops goes like:
                //
                //  1. Execute a get with all the indexed columns that are currently present (before the update)
                //  2. Create a KVPair reflecting the old get (old KVPair)
                //  3. Delete the old index row with the old KVPair
                //  4. Create a KVPair reflecting the mutation as an INSERT get (new KVPair)
                //  5. Insert the new index KVPair
                //
                // If the index delete does not find a row to delete, we send the mutation to the index as it is,
                // that is, instead of creating an INSERT with the mutation, we create a new index row with the
                // mutation as it came to us.
                if (deleteIndex(mutation, ctx)) {
                    // If the index was successfully deleted, we need to create an INSERT index row
                    createIndex(mutation, ctx, true);
                    return !failed;
                }
            }
        }

        // If we arrive here we are an INSERT or an UPDATE/UPSERT that has not yet updated the index.
        // This is the catchall for the branches that don't return above...
        createIndex(mutation, ctx, false);
        return !failed;
    }

    private boolean createIndex(KVPair mutation, WriteContext ctx, boolean toInsert) {
        try {
            KVPair newIndex = transformer.translate(mutation);
            if(toInsert)
                newIndex.setType(KVPair.Type.INSERT);
            if(keepState) {
                this.indexToMainMutationMap.put(newIndex, mutation);
            }
            indexBuffer.add(newIndex);
        } catch (Exception e) {
            failed = true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }
        return !failed;
    }

    private boolean deleteIndex(KVPair mutation, WriteContext ctx) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "index delete with %s", mutation);

    	/*
         * To delete the correct index row, we do the following:
         *
         * 1. do a Get() on all the indexed columns of the main table
         * 2. transform the results into an index row (as if we were inserting it)
         * 3. issue a delete against the index table
         */
        try {
            KVPair indexDelete = transformer.createIndexDelete(mutation, ctx, indexedColumns);
            if (indexDelete == null) {
                // we can't find the old row, it may have been deleted already, but we'll have to update the
                // index anyway in the calling method
//                ctx.success(mutation);
                return false;
            }
            if(keepState)
                this.indexToMainMutationMap.put(indexDelete,mutation);
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "performing index delete on row %s", BytesUtil.toHex(indexDelete.getRowKey()));
            ensureBufferReader(indexDelete, ctx);
            indexBuffer.add(indexDelete);
        } catch (Exception e) {
            failed=true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName()+":"+e.getMessage()));
        }
        return !failed;
    }

    private void ensureBufferReader(KVPair mutation, WriteContext ctx) {
        if (indexBuffer == null) {
            try {
                indexBuffer = getWriteBuffer(ctx, expectedWrites);
            } catch (Exception e) {
                ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
                failed = true;
            }
        }
    }
}
