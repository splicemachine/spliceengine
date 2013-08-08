package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.MutationResult;
import com.splicemachine.hbase.batch.WriteContext;
import org.apache.hadoop.hbase.client.Mutation;

import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class UniqueIndexDeleteWriteHandler extends IndexDeleteWriteHandler{

    private CallBuffer<Mutation> indexBuffer;
    public UniqueIndexDeleteWriteHandler(BitSet indexedColumns,int[] mainColToIndexPosMap,byte[] indexConglomBytes,BitSet descColumns) {
        super(indexedColumns,mainColToIndexPosMap, indexConglomBytes,descColumns);
    }

    @Override
    protected boolean updateIndex(Mutation mutation, WriteContext ctx) {
        if(indexBuffer==null){
            try {
                indexBuffer = getWriteBuffer(ctx);
            } catch (Exception e) {
                ctx.failed(mutation,new MutationResult(MutationResult.Code.FAILED,e.getClass().getSimpleName()+":"+e.getMessage()));
                failed=true;
            }
        }
        return super.updateIndex(mutation, ctx);
    }

    @Override
    protected void performDelete(Mutation deleteOp, WriteContext ctx) throws Exception {
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
