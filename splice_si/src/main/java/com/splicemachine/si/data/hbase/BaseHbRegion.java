package com.splicemachine.si.data.hbase;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.IHTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

public abstract class BaseHbRegion<HbRowLock> implements IHTable<HbRowLock> {

    protected static Mutation getMutation(KVPair kvPair, TxnView txn) throws IOException {
        assert kvPair.getType() == KVPair.Type.INSERT : "Performing an update/delete on a non-transactional table";
        Put put = new Put(kvPair.getRow());
        put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES, txn.getTxnId(), kvPair.getValue());
        put.setAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME, SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        return put;
    }

}
