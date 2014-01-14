package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.writer.KVPair;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class UniqueIndexUpsertWriteHandler extends IndexUpsertWriteHandler{
    public UniqueIndexUpsertWriteHandler(BitSet indexedColumns, int[] mainColToIndexPosMap,byte[] indexConglomBytes,BitSet descColumns,boolean keepState, int expectedWrites) {
        super(indexedColumns,mainColToIndexPosMap, indexConglomBytes,descColumns,keepState,true,expectedWrites);
    }

    @Override
    protected void doDelete(WriteContext ctx,KVPair delete) throws Exception {
        indexBuffer.add(delete);
    }
}
