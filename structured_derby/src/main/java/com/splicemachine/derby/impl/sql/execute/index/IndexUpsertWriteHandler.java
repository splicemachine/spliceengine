package com.splicemachine.derby.impl.sql.execute.index;

import com.google.common.base.Throwables;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.hbase.batch.WriteContext;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class IndexUpsertWriteHandler extends AbstractIndexWriteHandler {

    protected CallBuffer<Mutation> indexBuffer;
    protected MultiFieldEncoder encoder;

    public IndexUpsertWriteHandler(int[] indexColsToMainColMap, byte[][] mainColPos, byte[] indexConglomBytes) {
        super(indexColsToMainColMap, mainColPos, indexConglomBytes);
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
        Put put = update(mutation,ctx);
        if(put==null) return; //we updated the table, but it didn't change the index any!
        if(encoder==null)
            encoder = getEncoder();

        encoder.reset();

        for(int indexPos=0;indexPos<indexColsToMainColMap.length;indexPos++){
            byte[] putPos = mainColPos[indexPos];
            byte[] data = put.get(DEFAULT_FAMILY_BYTES,putPos).get(0).getValue();
            encoder.setRawBytes(data);
        }

        byte[] indexRowKey = getIndexRowKey(encoder);

        try {
            Put indexPut = SpliceUtils.createPut(indexRowKey, put);
            for(int i=0;i<indexColsToMainColMap.length;i++){
                byte[] indexPos = Encoding.encode(i);
                indexPut.add(DEFAULT_FAMILY_BYTES,indexPos,encoder.getEncodedBytes(i));
            }

            byte[] locPos = Encoding.encode(indexColsToMainColMap.length);
            indexPut.add(DEFAULT_FAMILY_BYTES, locPos, put.getRow());

            indexToMainMutationMap.put(indexPut,put);
            indexBuffer.add(indexPut);
            ctx.sendUpstream(mutation);
        } catch (IOException e) {
            failed=true;
            ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, e.getClass().getSimpleName()+":"+e.getMessage()));
        } catch (Exception e) {
            failed=true;
            ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, e.getClass().getSimpleName()+":"+e.getMessage()));
        }

    }

    protected byte[] getIndexRowKey(MultiFieldEncoder encoder) {
        byte[] postfix = SpliceUtils.getUniqueKey();
        encoder.setRawBytes(postfix);
        return encoder.build();
    }

    private Put update(Put mutation, WriteContext ctx) {
        if(!Bytes.equals(mutation.getAttribute(Puts.PUT_TYPE), Puts.FOR_UPDATE))
            return mutation; //not an update, nothing to do here

        boolean indexNeedsUpdating = false;
        for(byte[] indexColPos:mainColPos){
            if(mutation.has(DEFAULT_FAMILY_BYTES,indexColPos)){
                indexNeedsUpdating=true;
                break;
            }
        }
        if(!indexNeedsUpdating){
            //nothing in the index has changed! Whoo!
            ctx.sendUpstream(mutation);
            return null;
        }

        //bummer, we have to update the index
        try {
            Get oldGet = SpliceUtils.createGet(mutation, mutation.getRow());
            for(byte[] indexColPos:mainColPos){
                oldGet.addColumn(DEFAULT_FAMILY_BYTES,indexColPos);
            }
            Result r = ctx.getRegion().get(oldGet,null);
            if(r ==null|| r.isEmpty()){
                //no row exists in the main table, so this is actually an insert, no matter what
                //the attribute says.
                return mutation;
            }

            if(encoder==null)
                encoder = getEncoder();

            encoder.reset();
            /*
             * We get a free optimization here in that we don't have to do any update if the row already
             * looks like what we want.
             *
             * This can only occur when the row to delete looks like the row that we're putting on the index.
             * Since we might potentially be adding a UUID to the end of the key, we check the already-present
             * row key with the new row key BEFORE adding a UUID to it.
             */
            for (byte[] mainColPo : mainColPos) {
                byte[] data = r.getValue(DEFAULT_FAMILY_BYTES, mainColPo);
                encoder.setRawBytes(data);
            }

            byte[] currentIndexRowKey = encoder.build();

            //TODO -sf- don't waste this work, reuse it to build the index update somwhow!
            encoder.reset();

            for(int indexPos=0;indexPos<indexColsToMainColMap.length;indexPos++){
                byte[] putPos = mainColPos[indexPos];
                byte[] data = mutation.get(DEFAULT_FAMILY_BYTES,putPos).get(0).getValue();
                encoder.setRawBytes(data);
            }

            byte[] newRowKey = encoder.build();
            if(Bytes.compareTo(newRowKey, currentIndexRowKey)==0){
                //whoo! we don't need to update the index after all!
                return null;
            }

            Mutation delete = Mutations.translateToDelete(mutation,currentIndexRowKey);
            doDelete(ctx, delete);

            Put newPut = Mutations.translateToPut(mutation,null);
            for(byte[] indexPos:mainColPos){
                byte[] data;
                if(mutation.has(DEFAULT_FAMILY_BYTES,indexPos))
                    data = mutation.get(DEFAULT_FAMILY_BYTES,indexPos).get(0).getValue();
                else
                    data = r.getValue(DEFAULT_FAMILY_BYTES,indexPos);

                newPut.add(DEFAULT_FAMILY_BYTES,indexPos,data);
            }
            return newPut;
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
            final byte[] indexStop = BytesUtil.copyAndIncrement(delete.getRow());
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
