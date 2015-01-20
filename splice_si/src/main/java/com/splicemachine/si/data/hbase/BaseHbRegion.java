package com.splicemachine.si.data.hbase;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.IHTable;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

public abstract class BaseHbRegion<HbRowLock> implements IHTable<HbRowLock> {

    protected Mutation getMutation(KVPair kvPair, TxnView txn) throws IOException {
        assert kvPair.getType() == KVPair.Type.INSERT : "Performing an update/delete on a non-transactional table";
        ByteSlice rowKey = kvPair.rowKeySlice();
        ByteSlice value = kvPair.valueSlice();
        Put put = newPut(rowKey);
        byte[] family = SpliceConstants.DEFAULT_FAMILY_BYTES;
        byte[] column = SpliceConstants.PACKED_COLUMN_BYTES;
        KeyValue kv = new KeyValue(rowKey.array(),rowKey.offset(),rowKey.length(),
                family,0,family.length,
                column,0,column.length,
                txn.getTxnId(),
                KeyValue.Type.Put,value.array(),value.offset(),value.length());
        try {
            put.add(kv);
        } catch (IOException ignored) {
						/*
						 * This exception only appears to occur if the row in the Cell does not match
						 * the row that's set in the Put. This is definitionally not the case for the above
						 * code block, so we shouldn't have to worry about this error. As a result, throwing
						 * a RuntimeException here is legitimate
						 */
            throw new RuntimeException(ignored);
        }

        put.setAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME, SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        return put;
    }

    protected abstract Put newPut(ByteSlice rowKey);

}
