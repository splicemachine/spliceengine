package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.constants.bytes.BytesUtil;

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
    protected byte[][] getDataArray() {
        return new byte[indexColsToMainColMap.length][];
    }

    @Override
    protected byte[] getIndexRowKey(byte[][] rowKeyBuilder, int size) {
        return BytesUtil.concatenate(rowKeyBuilder,size);
    }
}
