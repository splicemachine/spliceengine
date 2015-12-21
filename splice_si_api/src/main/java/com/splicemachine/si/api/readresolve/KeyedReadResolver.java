package com.splicemachine.si.api.readresolve;

import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.TrafficControl;

/**
 * @author Scott Fines
 *         Date: 12/21/15
 */
public interface KeyedReadResolver{
    boolean resolve(Partition region,
                    ByteSlice rowKey,
                    long txnId,
                    TxnSupplier txnSupplier,
                    RollForwardStatus status,
                    boolean failOnError,
                    TrafficControl trafficControl);
}
