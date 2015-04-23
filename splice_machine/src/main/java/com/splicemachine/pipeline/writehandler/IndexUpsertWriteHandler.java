package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.storage.*;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "updateIndex with %s", mutation);

        ensureBufferReader(mutation, ctx);

        return upsert(mutation, ctx);
    }

    @Override
    protected boolean isHandledMutationType(KVPair.Type type) {
        return type == KVPair.Type.UPDATE || type == KVPair.Type.INSERT || type == KVPair.Type.UPSERT;
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

    private boolean upsert(KVPair mutation, WriteContext ctx) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "upsert %s", mutation);
        KVPair put = update(mutation, ctx);
        if (put == null) {
            return false; //we updated the table, and the index during the update process
        }

        try {
            KVPair indexPair = transformer.translate(mutation);
        	if (LOG.isTraceEnabled())
        		SpliceLogUtils.trace(LOG, "performing upsert on row %s", BytesUtil.toHex(indexPair.getRowKey()));

            if(keepState)
                this.indexToMainMutationMap.put(indexPair,mutation);
            indexBuffer.add(indexPair);
        } catch (Exception e) {
            failed = true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }
        return true;
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

    private KVPair update(KVPair mutation, WriteContext ctx) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "update mutation=%s", BytesUtil.toHex(mutation.getRowKey()));
        switch (mutation.getType()) {
            case UPDATE:
            case UPSERT:
                return doUpdate(mutation, ctx);
            default:
                return mutation;
        }
    }

    private KVPair doUpdate(KVPair mutation, WriteContext ctx) {
        if (newPutDecoder == null) {
            newPutDecoder = new EntryDecoder();
        }

        newPutDecoder.set(mutation.getValue());

        BitIndex updateIndex = newPutDecoder.getCurrentIndex();
        boolean needsIndexUpdating = false;
        for (int i = updateIndex.nextSetBit(0); i >= 0; i = updateIndex.nextSetBit(i + 1)) {
            if (indexedColumns.get(i)) {
                needsIndexUpdating = true;
                break;
            }
        }
        /*
         * We would normally love to say that the index hasn't changed--however,
         * we can't do that in the case of an upsert, because the record may not
         * exist. In that case, we'll do a local check--if the record already
         * exists locally, THEN we can say that the index hasn't changed.
         *
         * If we are an UPDATE, then we know that the record MUST exist, so
         * we can skip even the local fetch.
         */
        if (mutation.getType() != KVPair.Type.UPSERT && !needsIndexUpdating) {
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "update mutation, index does not need updating");
            //nothing in the index has changed, whoo!
            ctx.sendUpstream(mutation);
            return null;
        }

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
        try{
            byte[] row = mutation.getRowKey();
            Get oldGet = SpliceUtils.createGet(ctx.getTxn(), row);
            // TODO -sf- when it comes time to add additional (non-indexed data) to the index, you'll need to add more fields than just what's in indexedColumns
            EntryPredicateFilter filter = new EntryPredicateFilter(indexedColumns, new ObjectArrayList<Predicate>(), true);
            oldGet.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL, filter.toBytes());

            Result r = ctx.getRegion().get(oldGet);
            if (r == null || r.isEmpty()) {
                /*
                 * There is no entry in the main table, so this is an insert, regardless of what it THINKS it is
                 */
                return mutation;
            } else if (mutation.getType() == KVPair.Type.UPSERT && !needsIndexUpdating) {
                //we didn't change the index, and the row exists, we can skip!
                return null;
            }

            ByteEntryAccumulator newKeyAccumulator = transformer.getKeyAccumulator();
            newKeyAccumulator.reset();
            if (newPutDecoder == null)
                newPutDecoder = new EntryDecoder();

            newPutDecoder.set(mutation.getValue());

            MultiFieldDecoder newDecoder = null;
            MultiFieldDecoder oldDecoder = null;
            ByteEntryAccumulator oldKeyAccumulator;
            try {
                newDecoder = newPutDecoder.getEntryDecoder();

                if (oldDataDecoder == null)
                    oldDataDecoder = new EntryDecoder();

                oldDataDecoder.set(r.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES));
                BitIndex oldIndex = oldDataDecoder.getCurrentIndex();
                oldDecoder = oldDataDecoder.getEntryDecoder();
                oldKeyAccumulator = new ByteEntryAccumulator(null, transformer.isUnique() ? translatedIndexColumns : nonUniqueIndexColumn);

                buildKeyMap(mutation);
                // fill in index columns that are being changed by the mutation. We can assume here that no primary key column is being
                // changed, because otherwise, we won't be here
                BitSet remainingIndexColumns = (BitSet) indexedColumns.clone();
                for (int newPos = updateIndex.nextSetBit(0); newPos >= 0 && newPos <= indexedColumns.length(); newPos = updateIndex.nextSetBit(newPos + 1)) {
                    if (indexedColumns.get(newPos)) {
                        int mappedPosition = mainColToIndexPosMap[newPos];
                        ByteSlice keySlice = newKeyAccumulator.getField(mappedPosition, true);
                        newPutDecoder.nextField(newDecoder, newPos, keySlice);
                        if (this.descColumns.get(newPos)) {
                            keySlice.set(keySlice.getByteCopy());
                            keySlice.reverse();
                        }
                        occupy(newKeyAccumulator, updateIndex, newPos);
                        remainingIndexColumns.clear(newPos);
                    }
                }

                // Inspect each primary key column. If it is an index column, then set it value to the index row key
                if (columnOrdering != null) {
                    for (int i = 0; i < columnOrdering.length; ++i) {
                        int pos = reverseColumnOrdering[i];
                        if (remainingIndexColumns.get(pos)) {
                            int indexPos = mainColToIndexPosMap[i];
                            newKeyAccumulator.add(indexPos, keyDecoder.array(), pkIndex[i].getOffset(), pkIndex[i].getSize());
                            occupy(newKeyAccumulator, kdvds[pos], i);
                            oldKeyAccumulator.add(indexPos, keyDecoder.array(), pkIndex[i].getOffset(), pkIndex[i].getSize());
                            occupy(oldKeyAccumulator, kdvds[pos], i);
                            remainingIndexColumns.clear(pos);
                        }
                    }
                }

                // Inspect each column in the old value. If it is an index column, then set its value to the index row key
                for (int oldPos = oldIndex.nextSetBit(0); oldPos >= 0 && oldPos <= indexedColumns.length(); oldPos = oldIndex.nextSetBit(oldPos + 1)) {
                    if (indexedColumns.get(oldPos)) {
                        int mappedPosition = mainColToIndexPosMap[oldPos];
                        ByteSlice oldKeySlice = oldKeyAccumulator.getField(mappedPosition, true);
                        oldDataDecoder.nextField(oldDecoder, oldPos, oldKeySlice);
                        if (this.descColumns.get(oldPos)) {
                            oldKeySlice.set(oldKeySlice.getByteCopy());
                            oldKeySlice.reverse();
                        }
                        occupy(oldKeyAccumulator, oldIndex, oldPos);
                        if (remainingIndexColumns.get(oldPos)) {
                            ByteSlice keySlice = newKeyAccumulator.getField(mappedPosition, true);
                            keySlice.set(oldKeySlice, descColumns.get(oldPos));
                            occupy(newKeyAccumulator, oldIndex, oldPos);
                            remainingIndexColumns.clear(oldPos);
                        }
                    }
                }
            } finally {
                if (oldDecoder != null)
                    oldDecoder.close();
                if (newDecoder != null)
                    newDecoder.close();
            }

            /*
             * There is a free optimization in that we don't have to update the index if the new entry
             * would be the same as the old one.
             *
             * This can only happen if an update happens to set EVERY indexed field to the same value as it used to be.
             * E.g., if the index row was (a,b), then the new mutation must also generate an index row (a,b) and
             * NO OTHER.
             *
             * In practice, this means that we must iterate over the old data, and check it against the new data.
             * If they are the same, then needn't change anything.
             */
            if (newKeyAccumulator.fieldsMatch(oldKeyAccumulator)) {
                //our fields match exactly, nothing to update! don't insert into the index either
                return null;
            }

            //bummer, we have to update the index--specifically, we have to delete the old row, then
            //insert the new one

            //delete the old record
            byte[] encodedRow = Encoding.encodeBytesUnsorted(row);
            if (!transformer.isUnique()) {
                oldKeyAccumulator.add((int) translatedIndexColumns.length(), encodedRow, 0, encodedRow.length);
            }

            byte[] existingIndexRowKey = oldKeyAccumulator.finish();

            KVPair indexDelete = KVPair.delete(existingIndexRowKey);
            if (keepState)
                this.indexToMainMutationMap.put(indexDelete, mutation);
            doDelete(ctx, indexDelete);

            //insert the new
            byte[] newIndexRowKey = transformer.getIndexRowKey(encodedRow, !transformer.isUnique());

            EntryEncoder rowEncoder = transformer.getRowEncoder();
            MultiFieldEncoder entryEncoder = rowEncoder.getEntryEncoder();
            entryEncoder.reset();
            entryEncoder.setRawBytes(encodedRow);
            byte[] indexValue = rowEncoder.encode();
            KVPair newPut = new KVPair(newIndexRowKey, indexValue);
            if (keepState)
                indexToMainMutationMap.put(newPut, mutation);

            indexBuffer.add(newPut);

            ctx.sendUpstream(mutation);
            return null;
        } catch (Exception e) {
            failed = true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }

        //if we get an exception, then return null so we don't try and insert anything
        return null;
    }

    private void occupy(EntryAccumulator accumulator, DataValueDescriptor dvd, int position) {
        int mappedPosition = mainColToIndexPosMap[position];
        if (DerbyBytesUtil.isScalarType(dvd, null))
            accumulator.markOccupiedScalar(mappedPosition);
        else if (DerbyBytesUtil.isDoubleType(dvd))
            accumulator.markOccupiedDouble(mappedPosition);
        else if (DerbyBytesUtil.isFloatType(dvd))
            accumulator.markOccupiedFloat(mappedPosition);
        else
            accumulator.markOccupiedUntyped(mappedPosition);
    }

    private void occupy(EntryAccumulator accumulator, BitIndex index, int position) {
        int mappedPosition = mainColToIndexPosMap[position];
        if (index.isScalarType(position))
            accumulator.markOccupiedScalar(mappedPosition);
        else if (index.isDoubleType(position))
            accumulator.markOccupiedDouble(mappedPosition);
        else if (index.isFloatType(position))
            accumulator.markOccupiedFloat(mappedPosition);
        else
            accumulator.markOccupiedUntyped(mappedPosition);
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
