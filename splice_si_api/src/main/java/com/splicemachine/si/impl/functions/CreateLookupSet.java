package com.splicemachine.si.impl.functions;

import com.splicemachine.si.api.data.Record;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.spark_project.guava.base.Function;

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
