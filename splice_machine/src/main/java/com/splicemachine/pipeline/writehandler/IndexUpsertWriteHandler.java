package com.splicemachine.pipeline.writehandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Intercepts UPDATE/UPSERT/INSERT mutations to a base table and sends corresponding mutations to the index table.
 *
 * @author Scott Fines
 *         Created on: 5/1/13
 */
public class IndexUpsertWriteHandler extends AbstractIndexWriteHandler {
    static final Logger LOG = Logger.getLogger(IndexUpsertWriteHandler.class);
    protected CallBuffer<KVPair> indexBuffer;
    public IndexTransformer transformer;
    private EntryDecoder newPutDecoder;
    private EntryDecoder oldDataDecoder;
    private final int expectedWrites;
    private final BitSet nonUniqueIndexColumn;
    private int[] columnOrdering;
    private int[] formatIds;
    private BitSet pkColumns;
    private BitSet pkIndexColumns;
    private KeyData[] pkIndex;
    private DataValueDescriptor[] kdvds;
    private MultiFieldDecoder keyDecoder;
    private int[] reverseColumnOrdering;

    public IndexUpsertWriteHandler(BitSet indexedColumns,
                                   int[] mainColToIndexPos,
                                   byte[] indexConglomBytes,
                                   BitSet descColumns,
                                   boolean keepState,
                                   boolean unique,
                                   boolean uniqueWithDuplicateNulls,
                                   int expectedWrites,
                                   int[] columnOrdering,
                                   int[] formatIds) {
        super(indexedColumns, mainColToIndexPos, indexConglomBytes, descColumns, keepState);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "instance");

        this.expectedWrites = expectedWrites;
        nonUniqueIndexColumn = (BitSet) translatedIndexColumns.clone();
        nonUniqueIndexColumn.set(translatedIndexColumns.length());
        pkColumns = new BitSet();
        this.formatIds = formatIds;
        this.columnOrdering = columnOrdering;
        if (columnOrdering != null) {
            for (int col : columnOrdering) {
                pkColumns.set(col);
            }
        }
        pkIndexColumns = (BitSet) pkColumns.clone();
        pkIndexColumns.and(indexedColumns);

        boolean[] destKeySortOrder = new boolean[formatIds.length];
        Arrays.fill(destKeySortOrder, true);
        for (int i = descColumns.nextSetBit(0); i >= 0; i = descColumns.nextSetBit(i + 1)) {
            destKeySortOrder[i] = false;
        }
        int[] keyDecodingMap = new int[(int) indexedColumns.length()];
        Arrays.fill(keyDecodingMap, -1);
        for (int i = indexedColumns.nextSetBit(0); i >= 0; i = indexedColumns.nextSetBit(i + 1)) {
            keyDecodingMap[i] = mainColToIndexPos[i];
        }
        this.transformer = new IndexTransformer(unique, uniqueWithDuplicateNulls, null,
                columnOrdering, formatIds, null, keyDecodingMap, destKeySortOrder);
    }

    @Override
    public boolean updateIndex(KVPair mutation, WriteContext ctx) {
        ensureBufferReader(mutation, ctx);

        if (mutation.getType().isUpdateOrUpsert()) {
            if(mutation.getType().equals(KVPair.Type.UNIQUE_UPDATE)) {
                // If this is a unique index update, we need to change the type so that it passes constraint checking.
                // The mutation type is shared between main table update and the index update. Uniqueness constraint
                // check must be made against the main table update but, if that passes, there's no need to do it
                // against the unique index update.
                mutation.setType(KVPair.Type.UPDATE);
            }
            if (doUpdate(mutation, ctx)) {
                // The doUpdate() method either updated the index or determined that it doesn't need to be updated.
                return false;
            }
        }

        // If we arrive here we are an INSERT or an UPDATE/UPSERT that has not yet updated the index.

        try {
            KVPair indexPair = transformer.translate(mutation);
            if(keepState) {
                this.indexToMainMutationMap.put(indexPair, mutation);
            }
            indexBuffer.add(indexPair);
        } catch (Exception e) {
            failed = true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }
        return true;
    }

    @Override
    protected boolean isHandledMutationType(KVPair.Type type) {
        return type == KVPair.Type.UPDATE || type == KVPair.Type.INSERT || type == KVPair.Type.UPSERT || type == KVPair.Type.UNIQUE_UPDATE;
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

    private MultiFieldDecoder createKeyDecoder() {
        if (keyDecoder == null)
            keyDecoder = MultiFieldDecoder.create();
        return keyDecoder;
    }

    private void buildKeyMap(KVPair mutation) throws StandardException {

        // if index key column set and primary key column set do not intersect,
        // no need to build a key map
        pkIndexColumns = (BitSet) pkColumns.clone();
        pkIndexColumns.and(indexedColumns);

        if (pkIndexColumns.cardinality() > 0) {
            int len = columnOrdering.length;
            createKeyDecoder();
            if (kdvds == null) {
                kdvds = new DataValueDescriptor[len];
                for (int i = 0; i < len; ++i) {
                    kdvds[i] = LazyDataValueFactory.getLazyNull(formatIds[columnOrdering[i]]);
                }
            }
            if (pkIndex == null)
                pkIndex = new KeyData[len];
            keyDecoder.set(mutation.getRowKey());
            for (int i = 0; i < len; ++i) {
                int offset = keyDecoder.offset();
                DerbyBytesUtil.skip(keyDecoder, kdvds[i]);
                int size = keyDecoder.offset() - 1 - offset;
                pkIndex[i] = new KeyData(offset, size);
            }
        }
        if (columnOrdering != null && columnOrdering.length > 0) {
            reverseColumnOrdering = new int[formatIds.length];
            for (int i = 0; i < columnOrdering.length; ++i) {
                reverseColumnOrdering[columnOrdering[i]] = i;
            }
        }
    }

    /**
     * @return TRUE if we update the index or otherwise determine that no further mutation of the index is necessary.
     * If this returns FALSE then we are indicating that the original mutation should be transformed and written to the
     * index as it is.
     */
    private boolean doUpdate(KVPair mutation, WriteContext ctx) {
        // This gives us the non-primary-key columns that this mutation modifies.
        if (newPutDecoder == null) {
            newPutDecoder = new EntryDecoder();
        }
        newPutDecoder.set(mutation.getValue());
        BitIndex updateIndex = newPutDecoder.getCurrentIndex();

        boolean nonPrimaryKeyIndexColumnUpdated = false;
        for (int i = updateIndex.nextSetBit(0); i >= 0; i = updateIndex.nextSetBit(i + 1)) {
            if (indexedColumns.get(i)) {
                nonPrimaryKeyIndexColumnUpdated = true;
                break;
            }
        }

        if (!nonPrimaryKeyIndexColumnUpdated) {
            /*
             * This is an UPDATE or an UPSERT and we have ONLY updated primary key columns.  Return and send the mutation
             * to the index as it is.  The old value will have been deleted by the UpdateOperation which sends
             * a delete mutation when it sees that primary keys have changed.
             */
            ctx.sendUpstream(mutation);
            return false;
        }

        // If we get here:
        //
        // (1) We are an UPDATE or UPSERT
        // (2) We are modifying non-primary keys.
        // (3) WE MAY-BE modifying primary keys.

        /*
         * To update the index now, we must find the old index row and delete it, then
         * insert a new index row with the correct values.
         *
         * The order of ops goes like:
         *
         * 1. Execute a get with all the indexed columns that is currently present (before the update)
         * 2. Create a KVPair reflecting the old get
         * 3. Create a KVPair reflecting the updated get
         * 4. Delete the old index row
         * 5. Insert the new index row
         */

        //delete the old record
        try {
            Get get = SpliceUtils.createGet(ctx.getTxn(), mutation.getRowKey());
            EntryPredicateFilter predicateFilter = new EntryPredicateFilter(indexedColumns, new ObjectArrayList<Predicate>(),true);
            get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());
            Result result = ctx.getRegion().get(get);
            if(result==null||result.isEmpty()){
                // we can't find the old row, may have been deleted already, but we'll have to update the index anyway
                return false;
            }

            KeyValue resultValue = null;
            for(KeyValue value:result.raw()){
                 if(CellUtil.matchingFamily(value, SpliceConstants.DEFAULT_FAMILY_BYTES)
                    && CellUtil.matchingQualifier(value,SpliceConstants.PACKED_COLUMN_BYTES)){
                    resultValue = value;
                    break;
                }
            }
            KVPair toTransform = new KVPair(get.getRow(),resultValue.getValue(), KVPair.Type.DELETE);

            KVPair indexDelete = transformer.translate(toTransform);
            if (keepState)
                this.indexToMainMutationMap.put(indexDelete, mutation);
            doDelete(ctx, indexDelete);

            //create/add a new record
            toTransform = new KVPair(mutation.getRowKey(),mutation.getValue(), KVPair.Type.INSERT);
            KVPair newIndex = transformer.translate(toTransform);
            if (keepState)
                indexToMainMutationMap.put(newIndex, mutation);

            indexBuffer.add(newIndex);

            ctx.sendUpstream(mutation);
            return true;
        } catch (Exception e) {
            failed = true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }

        //if we get an exception, then return null so we don't try and insert anything
        return true;
    }

    protected void doDelete(final WriteContext ctx, final KVPair delete) throws Exception {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "do delete %s",BytesUtil.toHex(delete.getRowKey()));
        ensureBufferReader(delete,ctx);
        indexBuffer.add(delete);
    }

    @Override
    public void next(List<KVPair> mutations, WriteContext ctx) {
        throw new RuntimeException("Not Supported");
    }

    private static class KeyData {
        private int offset;
        private int size;

        public KeyData(int offset, int size) {
            this.offset = offset;
            this.size = size;
        }

        public int getOffset() {
            return offset;
        }

        public int getSize() {
            return size;
        }
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
}
