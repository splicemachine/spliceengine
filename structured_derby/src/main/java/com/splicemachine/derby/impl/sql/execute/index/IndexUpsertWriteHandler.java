package com.splicemachine.derby.impl.sql.execute.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.storage.SparseEntryAccumulator;
import com.splicemachine.storage.index.BitIndex;

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


    public IndexUpsertWriteHandler(BitSet indexedColumns,
                                   int[] mainColToIndexPos,
                                   byte[] indexConglomBytes,
                                   BitSet descColumns,
                                   boolean keepState,
                                   boolean unique,
                                   boolean uniqueWithDuplicateNulls,
                                   int expectedWrites) {
        super(indexedColumns,mainColToIndexPos,indexConglomBytes,descColumns,keepState);

        this.expectedWrites = expectedWrites;
        nonUniqueIndexColumn = (BitSet)translatedIndexColumns.clone();
        nonUniqueIndexColumn.set(translatedIndexColumns.length());
        this.transformer = new IndexTransformer(indexedColumns,
                translatedIndexColumns,
                nonUniqueIndexColumn,
                descColumns,mainColToIndexPosMap,unique,uniqueWithDuplicateNulls);
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
            Get oldGet = SpliceUtils.createGet(ctx.getTransactionId(), mutation.getRow());
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

            EntryAccumulator newKeyAccumulator = transformer.getKeyAccumulator();
            EntryAccumulator newRowAccumulator = transformer.getRowAccumulator();

            newKeyAccumulator.reset();
            newRowAccumulator.reset();

            if(newPutDecoder==null)
                newPutDecoder = new EntryDecoder(SpliceDriver.getKryoPool());

            newPutDecoder.set(mutation.getValue());

            MultiFieldDecoder newDecoder = null;
            MultiFieldDecoder oldDecoder = null;
            EntryAccumulator oldKeyAccumulator;
            try {
                newDecoder = newPutDecoder.getEntryDecoder();

                if(oldDataDecoder==null)
                    oldDataDecoder = new EntryDecoder(SpliceDriver.getKryoPool());

                oldDataDecoder.set(r.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY));
                BitIndex oldIndex = oldDataDecoder.getCurrentIndex();
                oldDecoder = oldDataDecoder.getEntryDecoder();
                oldKeyAccumulator = new SparseEntryAccumulator(null,transformer.isUnique()?translatedIndexColumns:nonUniqueIndexColumn);

                //fill in all the index fields that have changed
                for(int newPos=updateIndex.nextSetBit(0);newPos>=0 && newPos<=indexedColumns.length();newPos=updateIndex.nextSetBit(newPos+1)){
                    if(indexedColumns.get(newPos)){
                        ByteBuffer newBuffer = newPutDecoder.nextAsBuffer(newDecoder, newPos); // next indexed key
                        if(descColumns.get(mainColToIndexPosMap[newPos]))
                            accumulate(newKeyAccumulator,updateIndex,getDescendingBuffer(newBuffer),newPos);
                        else
                            accumulate(newKeyAccumulator,updateIndex,newBuffer,newPos);
                        accumulate(newRowAccumulator,updateIndex,newBuffer,newPos);
                    }
                }

                BitSet newRemainingCols = newKeyAccumulator.getRemainingFields();
                for(int oldPos=oldIndex.nextSetBit(0);oldPos>=0&&oldPos<=indexedColumns.length();oldPos=updateIndex.nextSetBit(oldPos+1)){
                    if(indexedColumns.get(oldPos)){
                        ByteBuffer oldBuffer = oldDataDecoder.nextAsBuffer(oldDecoder, oldPos);
                        //fill in the old key for checking
                        if(descColumns.get(mainColToIndexPosMap[oldPos]))
                            accumulate(oldKeyAccumulator,oldIndex,getDescendingBuffer(oldBuffer),oldPos);
                        else
                            accumulate(oldKeyAccumulator,oldIndex,oldBuffer,oldPos);

                        if(oldPos!=indexedColumns.length()&&newRemainingCols.get(oldPos)){
                            //we are missing this field from the new update, so add in the field from the old position
                            if(descColumns.get(oldPos))
                                accumulate(newKeyAccumulator,updateIndex,getDescendingBuffer(oldBuffer),oldPos);
                            else
                                accumulate(newKeyAccumulator,updateIndex,oldBuffer,oldPos);
                            accumulate(newKeyAccumulator,oldIndex,oldBuffer,oldPos);
                            accumulate(newRowAccumulator,oldIndex,oldBuffer,oldPos);
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
            if(!transformer.isUnique()){
                oldKeyAccumulator.add((int)translatedIndexColumns.length(),ByteBuffer.wrap(mutation.getRow()));
            }

            byte[] existingIndexRowKey = oldKeyAccumulator.finish();
            KVPair indexDelete = KVPair.delete(existingIndexRowKey);
            doDelete(ctx,indexDelete);

            //insert the new
            byte[] newIndexRowKey = transformer.getIndexRowKey(mutation.getRow());

            newRowAccumulator.add((int)translatedIndexColumns.length(),ByteBuffer.wrap(Encoding.encodeBytesUnsorted(mutation.getRow())));
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
