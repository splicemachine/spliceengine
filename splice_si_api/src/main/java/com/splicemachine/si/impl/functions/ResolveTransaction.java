package com.splicemachine.si.impl.functions;

import com.splicemachine.si.api.data.Record;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import org.spark_project.guava.base.Function;
import javax.annotation.Nullable;

/**
 *
 * Utilize Global Cache to resolve transaction and place transactions seen into global cache
 * Modify record inline
 */
public class ResolveTransaction implements Function<Record[],Record[]> {
    private TxnSupplier globableCache;
    private TxnSupplier scanCache;



    public ResolveTransaction(TxnSupplier globalCache, TxnSupplier scanCache) {
        this.globableCache = globalCache;
        this.scanCache = scanCache;
    }


    @Nullable
    @Override
    public Record[] apply(Record[] array) {
        try {
            for (Record activeConglomerate : array) {
                if (activeConglomerate == null || activeConglomerate.getEffectiveTimestamp() != 0) // Empty Array Element or Txn Resolved
                    break;
                if (activeConglomerate.getTxnId2() < 0) { // Collapsable Txn
                    Txn txn = globableCache.getTransaction(activeConglomerate.getTxnId1());
                }
                if (activeConglomerate.getTxnId1() > activeConglomerate.getTxnId2()) { // Hierarchical Txn

                }
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
