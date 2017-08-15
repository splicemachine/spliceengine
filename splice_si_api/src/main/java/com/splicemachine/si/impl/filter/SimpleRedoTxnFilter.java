package com.splicemachine.si.impl.filter;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataResult;

import java.io.IOException;

/**
 * Created by jleach on 8/15/17.
 *
 * Not yet...
 *
 */
public class SimpleRedoTxnFilter extends SimpleActiveTxnFilter {
    private DataResult[] lookups;
    private int position;

    public SimpleRedoTxnFilter(TxnView myTxn, ReadResolver readResolver, TxnSupplier baseSupplier,DataResult[] lookups, int position) {
        super(myTxn, readResolver, baseSupplier);
        this.lookups = lookups;
        this.position = position;
    }

    private ReturnCode checkVisibility(DataCell data) throws IOException {
        assert Bytes.equals(data.key(),lookups[position].key()):"Bytes are not equivalent";
        if(!isVisible(unsafeRecord.getTxnId1()))
            return unsafeRecord.getVersion() == 1?ReturnCode.NEXT_ROW:ReturnCode.SKIP;
        return unsafeRecord.hasTombstone()?ReturnCode.NEXT_ROW:ReturnCode.INCLUDE;
    }



}
