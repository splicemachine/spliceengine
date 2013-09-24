package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.writer.WriteResult;

import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class UniqueIndexDeleteWriteHandler extends IndexDeleteWriteHandler{

    private CallBuffer<KVPair> indexBuffer;
    public UniqueIndexDeleteWriteHandler(BitSet indexedColumns,int[] mainColToIndexPosMap,byte[] indexConglomBytes,
                                         BitSet descColumns,
                                         boolean keepState) {
        super(indexedColumns,mainColToIndexPosMap, indexConglomBytes,descColumns,keepState);
    }

    @Override
    protected boolean updateIndex(KVPair mutation, WriteContext ctx) {
        if(indexBuffer==null){
            try {
                indexBuffer = getWriteBuffer(ctx);
            } catch (Exception e) {
                ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
                failed=true;
            }
        }
        return super.updateIndex(mutation, ctx);
    }

    @Override
    protected void performDelete(KVPair deleteOp, WriteContext ctx) throws Exception {
        indexBuffer.add(deleteOp);
    }

    @Override
    protected void finish(WriteContext ctx) throws Exception {
        if(indexBuffer!=null){
            indexBuffer.flushBuffer();
            indexBuffer.close();
        }
    }
}
