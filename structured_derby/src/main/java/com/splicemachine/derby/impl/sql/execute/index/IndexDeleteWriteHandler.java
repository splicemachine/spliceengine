package com.splicemachine.derby.impl.sql.execute.index;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.hbase.batch.WriteContext;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class IndexDeleteWriteHandler extends AbstractIndexWriteHandler {

    private final List<Mutation> deletes = Lists.newArrayListWithExpectedSize(0);
    public IndexDeleteWriteHandler(int[] indexColsToMainColMap,
                                   byte[][] mainColPos,
                                   byte[] indexConglomBytes){
        super(indexColsToMainColMap,mainColPos,indexConglomBytes);


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
                final byte[] indexStop = BytesUtil.copyAndIncrement(delete.getRow());
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

            for(byte[] mainColumn:mainColPos){
                get.addColumn(DEFAULT_FAMILY_BYTES,mainColumn);
            }

            Result result = ctx.getRegion().get(get,null);
            if(result==null||result.isEmpty()){
                //already deleted? Weird, but okay, we can deal with that
                ctx.success(mutation);
                return;
            }

            NavigableMap<byte[],byte[]> familyMap = result.getFamilyMap(DEFAULT_FAMILY_BYTES);
            final byte[] indexRowKey = getDeleteKey(familyMap);

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

    private byte[] getDeleteKey(NavigableMap<byte[], byte[]> familyMap) {
        byte[][] rowKeyBuilder = getDataArray();
        int size =0;
        for(int indexPos=0;indexPos < indexColsToMainColMap.length;indexPos++){
            byte[] mainPutPos = mainColPos[indexPos];
            byte[] data = familyMap.get(mainPutPos);
            rowKeyBuilder[indexPos] = data;
            size+=data.length;
        }

        return BytesUtil.concatenate(rowKeyBuilder, size);
    }

    protected void performDelete(final Mutation deleteOp, WriteContext ctx) throws Exception {
        deletes.add(deleteOp);
    }
}
