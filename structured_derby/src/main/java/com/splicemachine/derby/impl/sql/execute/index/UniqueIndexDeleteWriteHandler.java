package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.batch.WriteContext;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class UniqueIndexDeleteWriteHandler extends IndexDeleteWriteHandler{

    private CallBuffer<Mutation> indexBuffer;
    public UniqueIndexDeleteWriteHandler(int[] indexColsToMainColMap,
                                         byte[][] mainColPos,
                                         byte[] indexConglomBytes) {
        super(indexColsToMainColMap, mainColPos, indexConglomBytes);

    }

    @Override
    protected boolean updateIndex(Mutation mutation, WriteContext ctx) {
        if(indexBuffer==null)
            indexBuffer = getWriteBuffer(ctx);
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

    @Override
    protected byte[][] getDataArray() {
        return new byte[indexColsToMainColMap.length][];
    }
}
