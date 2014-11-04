package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.hbase.KVPair;

/**
 * @author Scott Fines
 *         Created on: 5/1/13
 */
public class UniqueIndexUpsertWriteHandler extends IndexUpsertWriteHandler {
    public UniqueIndexUpsertWriteHandler(BitSet indexedColumns, int[] mainColToIndexPosMap, byte[] indexConglomBytes,
                                         BitSet descColumns, boolean keepState, boolean isUniqueWithDuplicateNulls,
                                         int expectedWrites, int[] columnOrdering, int[] formatIds) {
        super(indexedColumns, mainColToIndexPosMap, indexConglomBytes, descColumns, keepState, true,
              isUniqueWithDuplicateNulls, expectedWrites, columnOrdering, formatIds);
    }

    @Override
    protected void doDelete(WriteContext ctx, KVPair delete) throws Exception {
        indexBuffer.add(delete);
    }
}
