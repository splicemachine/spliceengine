package com.splicemachine.si.impl.functions;

import com.google.common.base.Function;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.storage.Record;

import javax.annotation.Nullable;

/**
 *
 * Utilize Global Cache to resolve transaction and place transactions seen into global cache
 *
 */
public class CreateLookupSet implements Function<Record[],long[]> {
    public TxnSupplier globalTxnCache;
    public TxnSupplier scanTxnCache;
    public CreateLookupSet(TxnSupplier globalTxnCache, TxnSupplier scanTxnCache) {
        this.globalTxnCache = globalTxnCache;
        this.scanTxnCache = scanTxnCache;
    }


    @Nullable
    @Override
    public long[] apply(Record[] iterator) {
        return null;
    }
}
