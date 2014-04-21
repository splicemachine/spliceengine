package com.splicemachine.derby.impl.sql.execute.index;

import java.io.IOException;
import java.util.List;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.storage.ByteEntryAccumulator;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.kryo.KryoPool;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class IndexUpsertWriteHandler extends AbstractIndexWriteHandler {

    protected CallBuffer<KVPair> indexBuffer;
    protected IndexTransformer transformer;
    private EntryDecoder newPutDecoder;
    private EntryDecoder oldDataDecoder;
    private final int expectedWrites;
    private final BitSet nonUniqueIndexColumn;
    private int[] columnOrdering;
    private int[] formatIds;
    private BitSet pkColumns;
    private BitSet pkIndexColumns;
    private IndexTransformer.KeyData[] pkIndex;
    private DataValueDescriptor[] kdvds;
    private MultiFieldDecoder keyDecoder;
    private int[] reverseColumnOrdering;

    public IndexUpsertWriteHandler(BitSet indexedColumns,
                                   int[] mainColToIndexPos,
                                   TableName indexConglomBytes,
                                   BitSet descColumns,
                                   boolean keepState,
                                   boolean unique,
                                   boolean uniqueWithDuplicateNulls,
                                   int expectedWrites,
                                   int[] columnOrdering,
                                   int[] formatIds){
        super(indexedColumns,mainColToIndexPos,indexConglomBytes,descColumns,keepState);

        this.expectedWrites = expectedWrites;
        nonUniqueIndexColumn = (BitSet)translatedIndexColumns.clone();
        nonUniqueIndexColumn.set(translatedIndexColumns.length());
        pkColumns = new BitSet();
        this.formatIds = formatIds;
        this.columnOrdering = columnOrdering;
        if (columnOrdering != null) {
            for (int col:columnOrdering) {
                pkColumns.set(col);
            }
        }
        pkIndexColumns = (BitSet)pkColumns.clone();
        pkIndexColumns.and(indexedColumns);
        this.transformer = new IndexTransformer(indexedColumns,
                translatedIndexColumns,nonUniqueIndexColumn,descColumns,mainColToIndexPosMap,
                unique,uniqueWithDuplicateNulls, KryoPool.defaultPool(), columnOrdering, formatIds);
    }

    @Override
    protected void finish(WriteContext ctx) throws Exception {
        if(indexBuffer!=null){
            indexBuffer.flushBuffer();
            indexBuffer.close();
        }
    }

    @Override
    protected boolean updateIndex(KVPair mutation, WriteContext ctx) {
        ensureBufferReader(mutation, ctx);

        if(mutation.getType()== KVPair.Type.DELETE) return true; //send upstream without acting on it

        upsert(mutation,ctx);
        return !failed;
    }

    private void ensureBufferReader(KVPair mutation, WriteContext ctx) {
        if(indexBuffer==null){
            try{
                indexBuffer = getWriteBuffer(ctx,expectedWrites);
            }catch(Exception e){
                ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
                failed=true;
            }
        }
    }

    private void upsert(KVPair mutation, WriteContext ctx) {
       KVPair put = update(mutation, ctx);
        if(put==null) return; //we updated the table, and the index during the update process

        try{
            KVPair indexPair = transformer.translate(mutation);
            if(keepState)
                this.indexToMainMutationMap.put(indexPair,mutation);
            indexBuffer.add(indexPair);
        } catch (IOException e) {
            failed=true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        } catch (Exception e) {
            failed=true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }
    }

    private MultiFieldDecoder createKeyDecoder() {
        if (keyDecoder == null)
            keyDecoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());
        return keyDecoder;
    }

    private void buildKeyMap (KVPair mutation) throws StandardException {

        // if index key column set and primary key column set do not intersect,
        // no need to build a key map
        pkIndexColumns = (BitSet)pkColumns.clone();
        pkIndexColumns.and(indexedColumns);

        if(pkIndexColumns.cardinality() > 0)
        {
            int len = columnOrdering.length;
            createKeyDecoder();
            if (kdvds == null) {
                kdvds = new DataValueDescriptor[len];
                for(int i = 0; i < len; ++i) {
                    kdvds[i] = LazyDataValueFactory.getLazyNull(formatIds[columnOrdering[i]]);
                }
            }
            if(pkIndex == null)
                pkIndex = new IndexTransformer.KeyData[len];
            keyDecoder.set(mutation.getRow());
            for (int i = 0; i < len; ++i) {
                int offset = keyDecoder.offset();
                DerbyBytesUtil.skip(keyDecoder, kdvds[i]);
                int size = keyDecoder.offset()-1-offset;
                pkIndex[i] = new IndexTransformer.KeyData (offset, size);
            }
        }
        if (columnOrdering != null && columnOrdering.length > 0) {
            reverseColumnOrdering = new int[formatIds.length];
            for (int i = 0; i < columnOrdering.length; ++i){
                reverseColumnOrdering[columnOrdering[i]] = i;
            }
        }
    }
    private KVPair update(KVPair mutation, WriteContext ctx) {
        //TODO -sf- move this logic into IndexTransformer
        if(mutation.getType()!= KVPair.Type.UPDATE)
            return mutation; //not an update, nothing to do here

        if(newPutDecoder==null)
            newPutDecoder = new EntryDecoder(SpliceDriver.getKryoPool());

        newPutDecoder.set(mutation.getValue());

        BitIndex updateIndex = newPutDecoder.getCurrentIndex();
        boolean needsIndexUpdating = false;
        for(int i=updateIndex.nextSetBit(0);i>=0;i=updateIndex.nextSetBit(i+1)){
            if(indexedColumns.get(i)){
                needsIndexUpdating=true;
                break;
            }
        }
        if(!needsIndexUpdating){
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
            byte[] row = mutation.getRow();
            Get oldGet = SpliceUtils.createGet(ctx.getTransactionId(), row);
            //TODO -sf- when it comes time to add additional (non-indexed data) to the index, you'll need to add more fields than just what's in indexedColumns
            EntryPredicateFilter filter = new EntryPredicateFilter(indexedColumns, new ObjectArrayList<Predicate>(),true);
            oldGet.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,filter.toBytes());

            Result r = ctx.getRegion().get(oldGet);
            if(r==null||r.isEmpty()){
                /*
                 * There is no entry in the main table, so this is an insert, regardless of what it THINKS it is
                 */
                return mutation;
            }

            ByteEntryAccumulator newKeyAccumulator = transformer.getKeyAccumulator();
            ByteEntryAccumulator newRowAccumulator = transformer.getRowAccumulator();

            newKeyAccumulator.reset();
            newRowAccumulator.reset();

            if(newPutDecoder==null)
                newPutDecoder = new EntryDecoder(SpliceDriver.getKryoPool());

            newPutDecoder.set(mutation.getValue());

            MultiFieldDecoder newDecoder = null;
            MultiFieldDecoder oldDecoder = null;
            ByteEntryAccumulator oldKeyAccumulator;
            try {
                newDecoder = newPutDecoder.getEntryDecoder();

                if(oldDataDecoder==null)
                    oldDataDecoder = new EntryDecoder(SpliceDriver.getKryoPool());

                oldDataDecoder.set(r.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY));
                BitIndex oldIndex = oldDataDecoder.getCurrentIndex();
                oldDecoder = oldDataDecoder.getEntryDecoder();
                oldKeyAccumulator = new ByteEntryAccumulator(null,transformer.isUnique()?translatedIndexColumns:nonUniqueIndexColumn);

                buildKeyMap(mutation);
                // fill in index columns that are being changed by the mutation. We can assume here that no primary key column is being
                // changed, because otherwise, we won't be here
                BitSet remainingIndexColumns = (BitSet)indexedColumns.clone();
                for (int newPos=updateIndex.nextSetBit(0);newPos>=0 && newPos<=indexedColumns.length();newPos=updateIndex.nextSetBit(newPos+1)){
                    if(indexedColumns.get(newPos)){
                        int mappedPosition = mainColToIndexPosMap[newPos];
                        ByteSlice keySlice = newKeyAccumulator.getField(mappedPosition,true);
                        newPutDecoder.nextField(newDecoder,newPos,keySlice);
                        occupy(newKeyAccumulator,updateIndex,newPos);
                        remainingIndexColumns.clear(newPos);
                    }
                }

                // Inspect each primary key column. If it is an index column, then set it value to the index row key
                if (columnOrdering != null) {
                    for(int i = 0; i < columnOrdering.length; ++i) {
                        int pos = reverseColumnOrdering[i];
                        if (remainingIndexColumns.get(pos)) {
                            int indexPos = mainColToIndexPosMap[i];
                            newKeyAccumulator.add(indexPos, keyDecoder.array(), pkIndex[i].getOffset(), pkIndex[i].getSize());
                            occupy(newKeyAccumulator,kdvds[pos],i);
                            oldKeyAccumulator.add(indexPos, keyDecoder.array(), pkIndex[i].getOffset(), pkIndex[i].getSize());
                            occupy(oldKeyAccumulator,kdvds[pos],i);
                            remainingIndexColumns.clear(pos);
                        }
                    }
                }

                // Inspect each column in the old value. If it is an index column, then set its value to the index row key
                for(int oldPos=oldIndex.nextSetBit(0);oldPos>=0&&oldPos<=indexedColumns.length();oldPos=oldIndex.nextSetBit(oldPos+1)){
                    if (indexedColumns.get(oldPos)) {
                        int mappedPosition = mainColToIndexPosMap[oldPos];
                        ByteSlice oldKeySlice = oldKeyAccumulator.getField(mappedPosition,true);
                        oldDataDecoder.nextField(oldDecoder,oldPos,oldKeySlice);
                        occupy(oldKeyAccumulator,oldIndex,oldPos);
                        if (remainingIndexColumns.get(oldPos)) {
                            ByteSlice keySlice = newKeyAccumulator.getField(mappedPosition,true);
                            keySlice.set(oldKeySlice,descColumns.get(oldPos));
                            occupy(newKeyAccumulator,oldIndex,oldPos);
                            remainingIndexColumns.clear(oldPos);
                        }
                    }
                }
            } finally {
                if(oldDecoder!=null)
                    oldDecoder.close();
                if(newDecoder!=null)
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
            if(newKeyAccumulator.fieldsMatch(oldKeyAccumulator)){
                //our fields match exactly, nothing to update! don't insert into the index either
                return null;
            }

            //bummer, we have to update the index--specifically, we have to delete the old row, then
            //insert the new one

            //delete the old record
            byte[] encodedRow = Encoding.encodeBytesUnsorted(row);
            if(!transformer.isUnique()){
                oldKeyAccumulator.add((int)translatedIndexColumns.length(),encodedRow,0,encodedRow.length);
            }

            byte[] existingIndexRowKey = oldKeyAccumulator.finish();

            KVPair indexDelete = KVPair.delete(existingIndexRowKey);
            doDelete(ctx,indexDelete);

            //insert the new
            byte[] newIndexRowKey = transformer.getIndexRowKey(encodedRow, !transformer.isUnique());

            //if(!transformer.isUnique()) {
                newRowAccumulator.add((int)translatedIndexColumns.length(),encodedRow,0,encodedRow.length);
            //}
            byte[] indexValue = newRowAccumulator.finish();
            KVPair newPut = new KVPair(newIndexRowKey,indexValue);
            if(keepState)
                indexToMainMutationMap.put(mutation, newPut);
            indexBuffer.add(newPut);

            ctx.sendUpstream(mutation);
            return null;
        } catch (IOException e) {
            failed=true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        } catch (Exception e) {
            failed=true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName()+":"+e.getMessage()));
        }

        //if we get an exception, then return null so we don't try and insert anything
        return null;
    }

    private void occupy(EntryAccumulator accumulator, DataValueDescriptor dvd, int position) {
        int mappedPosition = mainColToIndexPosMap[position];
        if(DerbyBytesUtil.isScalarType(dvd))
            accumulator.markOccupiedScalar(mappedPosition);
        else if(DerbyBytesUtil.isDoubleType(dvd))
            accumulator.markOccupiedDouble(mappedPosition);
        else if(DerbyBytesUtil.isFloatType(dvd))
            accumulator.markOccupiedFloat(mappedPosition);
        else
            accumulator.markOccupiedUntyped(mappedPosition);
    }

		private void occupy(EntryAccumulator accumulator, BitIndex index, int position) {
				int mappedPosition = mainColToIndexPosMap[position];
				if(index.isScalarType(position))
						accumulator.markOccupiedScalar(mappedPosition);
				else if(index.isDoubleType(position))
						accumulator.markOccupiedDouble(mappedPosition);
				else if(index.isFloatType(position))
						accumulator.markOccupiedFloat(mappedPosition);
				else
						accumulator.markOccupiedUntyped(mappedPosition);
		}

    protected void doDelete(final WriteContext ctx, final KVPair delete) throws Exception {
        ensureBufferReader(delete,ctx);
        indexBuffer.add(delete);
    }

	@Override
	public void next(List<KVPair> mutations, WriteContext ctx) {
		// XXX JLEACH TODO
		throw new RuntimeException("Not Supported");
	}
}
