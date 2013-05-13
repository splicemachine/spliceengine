package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.batch.WriteContext;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class IndexUpsertWriteHandler extends AbstractIndexWriteHandler {

    protected CallBuffer<Mutation> indexBuffer;
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
        byte[][] rowKeyBuilder = getDataArray();
        int size =0;

        for(int indexPos=0;indexPos<indexColsToMainColMap.length;indexPos++){
            byte[] putPos = mainColPos[indexPos];
            byte[] data = put.get(DEFAULT_FAMILY_BYTES,putPos).get(0).getValue();
            rowKeyBuilder[indexPos] = data;
            size+=data.length+1;
        }

        byte[] indexRowKey = getIndexRowKey(rowKeyBuilder,size);

        try {
            Put indexPut = SpliceUtils.createPut(indexRowKey, put);
            for(int i=0;i<indexColsToMainColMap.length;i++){
                byte[] indexPos = Bytes.toBytes(i);
                indexPut.add(DEFAULT_FAMILY_BYTES,indexPos,rowKeyBuilder[i]);
            }

            byte[] locPos = Bytes.toBytes(indexColsToMainColMap.length);
            indexPut.add(DEFAULT_FAMILY_BYTES, locPos, put.getRow());

            indexToMainMutationMap.put(indexPut,put);
            indexBuffer.add(indexPut);
            ctx.sendUpstream(mutation);
        } catch (IOException e) {
            failed=true;
            ctx.failed(mutation,e.getClass().getSimpleName()+":"+e.getMessage());
        } catch (Exception e) {
            failed=true;
            ctx.failed(mutation,e.getClass().getSimpleName()+":"+e.getMessage());
        }

    }

    protected byte[] getIndexRowKey(byte[][] rowKeyBuilder, int size) {
        byte[] postfix = SpliceUtils.getUniqueKey();
        rowKeyBuilder[rowKeyBuilder.length-1] = postfix;
        size+=postfix.length;
        return BytesUtil.concatenate(rowKeyBuilder,size);
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

            byte[][] rowToDelete = getDataArray();
            int size =0;
            for(int indexPos=0;indexPos<mainColPos.length;indexPos++){
                byte[] data = r.getValue(DEFAULT_FAMILY_BYTES,mainColPos[indexPos]);
                rowToDelete[indexPos] = data;
                size+=data.length+1;
            }

            byte[] indexRowKey = BytesUtil.concatenate(rowToDelete, size);
            Mutation delete = Mutations.translateToDelete(mutation,indexRowKey);
            indexBuffer.add(delete);

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
            ctx.failed(mutation,e.getClass().getSimpleName()+":"+e.getMessage());
        } catch (Exception e) {
            failed=true;
            ctx.failed(mutation,e.getClass().getSimpleName()+":"+e.getMessage());
        }
        //if we get an exception, then return null so we don't try and insert anything
        return null;
    }
}
