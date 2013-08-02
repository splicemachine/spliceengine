package com.splicemachine.derby.impl.sql.execute.index;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.storage.*;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.ByteDataOutput;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class IndexDeleteWriteHandler extends AbstractIndexWriteHandler {

    private final List<Mutation> deletes = Lists.newArrayListWithExpectedSize(0);

    public IndexDeleteWriteHandler(BitSet indexedColumns,int[] mainColToIndexPosMap,byte[] indexConglomBytes,BitSet descColumns){
        super(indexedColumns,mainColToIndexPosMap,indexConglomBytes,descColumns);


    }

    @Override
    protected boolean updateIndex(Mutation mutation, WriteContext ctx) {
        if(!Mutations.isDelete(mutation)) return true; //allow super class to send it along

        delete(mutation,ctx);
        return !failed;
    }

    @Override
    protected void finish(WriteContext ctx) throws Exception {
        for(final Mutation delete:deletes){
            if(failed){
                ctx.notRun(delete);
            }else{
                final byte[] indexStop = BytesUtil.unsignedCopyAndIncrement(delete.getRow());
                HTableInterface table = ctx.getHTable(indexConglomBytes);
                try {
                    table.coprocessorExec(BatchProtocol.class,
                            delete.getRow(),indexStop,new Batch.Call<BatchProtocol, MutationResult>() {
                        @Override
                        public MutationResult call(BatchProtocol instance) throws IOException {
                            return instance.deleteFirstAfter(
                                    SpliceUtils.getTransactionId(delete),delete.getRow(),indexStop);
                        }
                    });
                } catch (Throwable throwable) {
                    //noinspection ThrowableResultOfMethodCallIgnored
                    Throwable t = Throwables.getRootCause(throwable);
                    ctx.failed(delete, new MutationResult(MutationResult.Code.FAILED, t.getClass().getSimpleName() + ":" + t.getMessage()));
                    failed=true;
                }
            }
        }
    }

    private void delete(Mutation mutation, WriteContext ctx) {
        /*
         * Deleting a row involves first getting the row, then
         * constructing an index row key from the row, then issuing a
         * delete request against the index.
         */
        try {

            Get get = SpliceUtils.createGet(mutation, mutation.getRow());
            EntryPredicateFilter predicateFilter = new EntryPredicateFilter(indexedColumns, Collections.<Predicate>emptyList(),true);
            get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());
            Result result = ctx.getRegion().get(get);
            if(result==null||result.isEmpty()){
                //already deleted? Weird, but okay, we can deal with that
                ctx.success(mutation);
                return;
            }

            EntryDecoder getDecoder = new EntryDecoder();
            getDecoder.set(result.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY));

            EntryAccumulator indexKeyAccumulator = new SparseEntryAccumulator(null,indexedColumns,false);
            BitIndex getIndex = getDecoder.getCurrentIndex();
            MultiFieldDecoder fieldDecoder = getDecoder.getEntryDecoder();
            for(int i=indexedColumns.nextSetBit(0);i>=0;i=indexedColumns.nextSetBit(i+1)){
                if(descColumns.get(i))
                    accumulate(indexKeyAccumulator,getIndex,getDescendingBuffer(getDecoder.nextAsBuffer(fieldDecoder,i)),i);
                else
                    accumulate(indexKeyAccumulator,getIndex,getDecoder.nextAsBuffer(fieldDecoder,i),i);
            }

            byte[] indexRowKey = indexKeyAccumulator.finish();

            Mutation delete = Mutations.getDeleteOp(mutation,indexRowKey);
            indexToMainMutationMap.put(delete,mutation);
            performDelete(delete,ctx);

        } catch (IOException e) {
            failed=true;
            ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, e.getClass().getSimpleName()+":"+e.getMessage()));
        } catch (Exception e) {
            failed=true;
            ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, e.getClass().getSimpleName()+":"+e.getMessage()));
        }
    }

    protected void performDelete(final Mutation deleteOp, WriteContext ctx) throws Exception {
        deletes.add(deleteOp);
    }
}
