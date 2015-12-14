package com.splicemachine.si.data.hbase;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.IHTable;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import java.io.IOException;
import static com.splicemachine.si.constants.SIConstants.DEFAULT_FAMILY_BYTES;
import static com.splicemachine.si.constants.SIConstants.PACKED_COLUMN_BYTES;
import static com.splicemachine.si.constants.SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;
import static com.splicemachine.si.constants.SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE;


public abstract class BaseHbRegion implements IHTable<OperationWithAttributes,Delete,
        Get,Mutation,OperationStatus, Pair<Mutation, HRowLock>,
        Put,Result,Scan> {

    protected Mutation getMutation(KVPair kvPair, TxnView txn) throws IOException {
        assert kvPair.getType() == KVPair.Type.INSERT : "Performing an update/delete on a non-transactional table";
        ByteSlice rowKey = kvPair.rowKeySlice();
        ByteSlice value = kvPair.valueSlice();
        Put put = newPut(rowKey);
        byte[] family = DEFAULT_FAMILY_BYTES;
        byte[] column = PACKED_COLUMN_BYTES;
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

        put.setAttribute(SUPPRESS_INDEXING_ATTRIBUTE_NAME, SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        return put;
    }

    protected abstract Put newPut(ByteSlice rowKey);

}
