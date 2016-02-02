package com.splicemachine.si.impl.region;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.txn.Txn;
import org.apache.hadoop.hbase.Cell;

/**
 * @author Scott Fines
 *         Date: 2/2/16
 */
public class TxnStoreUtils{

    public static Txn.State adjustStateForTimeout(Txn.State currentState,
                                                  Cell keepAliveCell,
                                                  Clock clock,
                                                  long keepAliveTimeoutMs){
        long lastKATime = V2TxnDecoder.decodeKeepAlive(keepAliveCell,false);

        if((clock.currentTimeMillis()-lastKATime)>keepAliveTimeoutMs)
            return Txn.State.ROLLEDBACK;
        return currentState;
    }
}
