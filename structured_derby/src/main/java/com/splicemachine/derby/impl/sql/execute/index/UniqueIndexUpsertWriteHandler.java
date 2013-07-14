package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.SparseEntryAccumulator;
import org.apache.hadoop.hbase.client.Mutation;

import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 5/1/13
 */
public class UniqueIndexUpsertWriteHandler extends IndexUpsertWriteHandler{
    public UniqueIndexUpsertWriteHandler(BitSet indexedColumns, int[] mainColToIndexPosMap,byte[] indexConglomBytes) {
        super(indexedColumns,mainColToIndexPosMap, indexConglomBytes);
    }

    @Override
    protected SparseEntryAccumulator getKeyAccumulator() {
        return new SparseEntryAccumulator(indexedColumns,false);
    }

    @Override
    protected byte[] getIndexRowKey(EntryAccumulator keyAccumulator) {
        return keyAccumulator.finish();
    }

    @Override
    protected void doDelete(WriteContext ctx,Mutation delete) throws Exception {
        indexBuffer.add(delete);
    }
}
