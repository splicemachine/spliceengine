package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.TxnLocationFactory;

/**
 * Created by jleach on 1/18/17.
 */
public class SimpleTxnLocationFactory implements TxnLocationFactory {

    @Override
    public int getNodeId() {
        return 1;
    }

    @Override
    public int getRegionId() {
        return 1;
    }
}
