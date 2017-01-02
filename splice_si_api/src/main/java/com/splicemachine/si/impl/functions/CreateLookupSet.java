package com.splicemachine.si.impl.functions;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import com.google.common.base.Function;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.storage.Record;

import javax.annotation.Nullable;

/**
 *
 * Utilize Global Cache to resolve transaction and place transactions seen into global cache
 *
 */
public class CreateLookupSet implements Function<Record[],LongSet> {
    public TxnSupplier globalTxnCache;
    public TxnSupplier scanTxnCache;
    public CreateLookupSet(TxnSupplier globalTxnCache, TxnSupplier scanTxnCache) {
        this.globalTxnCache = globalTxnCache;
        this.scanTxnCache = scanTxnCache;
    }


    @Nullable
    @Override
    public LongSet apply(Record[] records) {
        if (records != null && records.length > 0) {
            LongSet setToReturn = new LongOpenHashSet(records.length);
            for (Record activeConglomerate : array) {
                if (activeConglomerate == null || activeConglomerate.getEffectiveTimestamp() != 0) // Empty Array Element or Txn Resolved
                    break;
                if (activeConglomerate.getTxnId2() < 0) { // Collapsable Txn
                    Txn txn = globableCache.getTransaction(activeConglomerate.getTxnId1());
                }
                if (activeConglomerate.getTxnId1() > activeConglomerate.getTxnId2()) { // Hierarchical Txn

                }
            }
        }
        return null;
    }
}
