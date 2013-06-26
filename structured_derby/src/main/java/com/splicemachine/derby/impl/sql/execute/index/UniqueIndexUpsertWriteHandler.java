package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.batch.WriteContext;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class UniqueIndexUpsertWriteHandler extends IndexUpsertWriteHandler{
    public UniqueIndexUpsertWriteHandler(int[] indexColsToMainColMap,
                                            byte[][] mainColPos,
                                            byte[] indexConglomBytes) {
        super(indexColsToMainColMap, mainColPos, indexConglomBytes);
    }


    @Override
    protected MultiFieldEncoder getEncoder() {
        return MultiFieldEncoder.create(indexColsToMainColMap.length);
    }

    @Override
    protected byte[] getIndexRowKey(MultiFieldEncoder encoder) {
        return encoder.build();
    }

    @Override
    protected void doDelete(WriteContext ctx,Mutation delete) throws Exception {
        indexBuffer.add(delete);
    }
}
