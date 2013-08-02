package com.splicemachine.derby.impl.sql.execute.index;

import com.google.common.base.Throwables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.storage.*;
import com.splicemachine.storage.index.BitIndex;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class IndexUpsertWriteHandler extends AbstractIndexWriteHandler {

    protected CallBuffer<Mutation> indexBuffer;
    private EntryAccumulator newKeyAccumulator;
    private EntryAccumulator newRowAccumulator;
    private EntryDecoder newPutDecoder;
    private EntryDecoder oldDataDecoder;

    private BitSet nonUniqueIndexedColumns;

    public IndexUpsertWriteHandler(BitSet indexedColumns, int[] mainColToIndexPos,byte[] indexConglomBytes,BitSet descColumns) {
        super(indexedColumns,mainColToIndexPos,indexConglomBytes,descColumns);

        nonUniqueIndexedColumns = (BitSet)translatedIndexColumns.clone();
        nonUniqueIndexedColumns.set(translatedIndexColumns.length());
    }

    @Override
    protected void finish(WriteContext ctx) throws Exception {
        if(indexBuffer!=null){
            indexBuffer.flushBuffer();
            indexBuffer.close();
        }
    }

    @Override
    protected boolean updateIndex(Mutation mutation, WriteContext ctx) {
        if(indexBuffer==null)
            indexBuffer = getWriteBuffer(ctx);

        if(Mutations.isDelete(mutation)) return true; //send upstream without acting on it

        upsert((Put)mutation,ctx);
        return !failed;
    }

    private void upsert(Put mutation, WriteContext ctx) {
        Put put = update(mutation, ctx);
        if(put==null) return; //we updated the table, and the index during the update process

        if(newKeyAccumulator==null)
            newKeyAccumulator = getKeyAccumulator();
        if(newRowAccumulator==null)
            newRowAccumulator = new SparseEntryAccumulator(null,nonUniqueIndexedColumns,true);

        newKeyAccumulator.reset();
        newRowAccumulator.reset();

        if(newPutDecoder==null)
            newPutDecoder = new EntryDecoder();

        newPutDecoder.set(mutation.get(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY).get(0).getValue());

        BitIndex mutationIndex = newPutDecoder.getCurrentIndex();
        try{
            MultiFieldDecoder mutationDecoder = newPutDecoder.getEntryDecoder();
            for(int i=mutationIndex.nextSetBit(0);i>=0&&i<=indexedColumns.length();i=mutationIndex.nextSetBit(i+1)){
                if(indexedColumns.get(i)){
                    ByteBuffer entry = newPutDecoder.nextAsBuffer(mutationDecoder, i);
                    if(descColumns.get(i))
                        accumulate(newKeyAccumulator,mutationIndex,getDescendingBuffer(entry),i);
                    else
                        accumulate(newKeyAccumulator,mutationIndex,entry,i);
                    accumulate(newRowAccumulator,mutationIndex,entry,i);
                }else{
                    newPutDecoder.seekForward(mutationDecoder,i);
                }
            }

            //add the row to the end of the index rob
            newRowAccumulator.add(translatedIndexColumns.length(), ByteBuffer.wrap(Encoding.encodeBytesUnsorted(mutation.getRow())));
            byte[] indexRowKey = getIndexRowKey(newKeyAccumulator);
            Put indexPut = Mutations.translateToPut(mutation,indexRowKey);
            indexPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY,newRowAccumulator.finish());

            indexToMainMutationMap.put(indexPut,mutation);
            indexBuffer.add(indexPut);
            ctx.sendUpstream(mutation);
        }catch (IOException e) {
            failed=true;
            ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, e.getClass().getSimpleName()+":"+e.getMessage()));
        } catch (Exception e) {
            failed=true;
            ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, e.getClass().getSimpleName()+":"+e.getMessage()));
        }
    }



    protected SparseEntryAccumulator getKeyAccumulator() {
        return new SparseEntryAccumulator(null,nonUniqueIndexedColumns,false);
    }

    protected byte[] getIndexRowKey(EntryAccumulator accumulator) {
        byte[] postfix = SpliceUtils.getUniqueKey();
        accumulator.add(translatedIndexColumns.length(), ByteBuffer.wrap(postfix));
        return accumulator.finish();
    }

    private Put update(Put mutation, WriteContext ctx) {
        if(!Bytes.equals(mutation.getAttribute(Puts.PUT_TYPE), Puts.FOR_UPDATE))
            return mutation; //not an update, nothing to do here

        if(newPutDecoder==null)
            newPutDecoder = new EntryDecoder();

        newPutDecoder.set(mutation.get(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY).get(0).getValue());

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
         */
        try{
            Get oldGet = SpliceUtils.createGet(mutation,mutation.getRow());
            //TODO -sf- when it comes time to add additional (non-indexed data) to the index, you'll need to add more fields than just what's in indexedColumns
            EntryPredicateFilter filter = new EntryPredicateFilter(indexedColumns, Collections.<Predicate>emptyList(),true);
            oldGet.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,filter.toBytes());

            Result r = ctx.getRegion().get(oldGet);
            if(r==null||r.isEmpty()){
                /*
                 * There is no entry in the main table, so this is an insert, regardless of what it THINKS it is
                 */
                return mutation;
            }

            //build the new row key and row entry
            if(newKeyAccumulator==null)
                newKeyAccumulator = getKeyAccumulator();
            if(newRowAccumulator==null)
                newRowAccumulator = new SparseEntryAccumulator(null,nonUniqueIndexedColumns,true);

            newKeyAccumulator.reset();
            newRowAccumulator.reset();

            if(newPutDecoder==null)
                newPutDecoder = new EntryDecoder();

            newPutDecoder.set(mutation.get(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY).get(0).getValue());

            MultiFieldDecoder newDecoder = newPutDecoder.getEntryDecoder();

            if(oldDataDecoder==null)
                oldDataDecoder = new EntryDecoder();

            oldDataDecoder.set(r.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY));
            BitIndex oldIndex = oldDataDecoder.getCurrentIndex();
            MultiFieldDecoder oldDecoder = oldDataDecoder.getEntryDecoder();
            EntryAccumulator oldKeyAccumulator = new SparseEntryAccumulator(null,translatedIndexColumns);

            //fill in all the index fields that have changed
            for(int newPos=updateIndex.nextSetBit(0);newPos>=0 && newPos<=indexedColumns.length();newPos=updateIndex.nextSetBit(newPos+1)){
                if(indexedColumns.get(newPos)){
                    ByteBuffer newBuffer = newPutDecoder.nextAsBuffer(newDecoder, newPos); // next indexed key
                    if(descColumns.get(newPos))
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
                    if(descColumns.get(oldPos))
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
            byte[] existingIndexRowKey = oldKeyAccumulator.finish();
            Mutation indexDelete = Mutations.translateToDelete(mutation,existingIndexRowKey);
            doDelete(ctx,indexDelete);

            //insert the new
            byte[] newIndexRowKey = getIndexRowKey(newKeyAccumulator);
            Put newPut = Mutations.translateToPut(mutation,newIndexRowKey);
            newRowAccumulator.add(translatedIndexColumns.length(),ByteBuffer.wrap(Encoding.encodeBytesUnsorted(mutation.getRow())));
            newPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY,newRowAccumulator.finish());
            indexToMainMutationMap.put(mutation,newPut);
            indexBuffer.add(newPut);

            ctx.sendUpstream(mutation);
            return null;
        } catch (IOException e) {
            failed=true;
            ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, e.getClass().getSimpleName()+":"+e.getMessage()));
        } catch (Exception e) {
            failed=true;
            ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, e.getClass().getSimpleName()+":"+e.getMessage()));
        }

        //if we get an exception, then return null so we don't try and insert anything
        return null;
    }



    protected void doDelete(WriteContext ctx, final Mutation delete) throws Exception {
        HTableInterface hTable = ctx.getHTable(indexConglomBytes);
        try{
            final byte[] indexStop = BytesUtil.unsignedCopyAndIncrement(delete.getRow());
            hTable.coprocessorExec(BatchProtocol.class,
                    delete.getRow(),indexStop, new Batch.Call<BatchProtocol, Object>() {
                @Override
                public MutationResult call(BatchProtocol instance) throws IOException {
                    return instance.deleteFirstAfter(
                            SpliceUtils.getTransactionId(delete),delete.getRow(),indexStop);
                }
            });
        } catch (Throwable throwable) {
            @SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable t = Throwables.getRootCause(throwable);
            ctx.failed(delete,new MutationResult(MutationResult.Code.FAILED,t.getClass().getSimpleName()+":"+t.getMessage()));
            failed=true;
        }
    }
}
