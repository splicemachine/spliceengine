package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import org.apache.hadoop.hbase.TableName;

import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.KVPair;

/**
 * @author Scott Fines
 *         Created on: 5/1/13
 */
public class UniqueIndexUpsertWriteHandler extends IndexUpsertWriteHandler {
    public UniqueIndexUpsertWriteHandler(BitSet indexedColumns, int[] mainColToIndexPosMap, TableName indexConglomBytes,
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
