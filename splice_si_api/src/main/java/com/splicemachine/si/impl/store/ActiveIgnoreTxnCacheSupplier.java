package com.splicemachine.si.impl.store;

import com.splicemachine.utils.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jyuan on 4/20/15.
 */
public class ActiveIgnoreTxnCacheSupplier {

    private final Map<String, List<Pair<Long,Long>>> cache;

    IgnoreTxnCacheSupplier delegate;

    public ActiveIgnoreTxnCacheSupplier (IgnoreTxnCacheSupplier delegate) {
        cache = new HashMap();
        this.delegate = delegate;
    }

    public boolean shouldIgnore(String tableName, Long txnId) {

        boolean result = false;
        try {
            if (tableName != null) {
                List<Pair<Long, Long>> ignoreTxnList = cache.get(tableName);
                if (ignoreTxnList == null) {
                    ignoreTxnList = delegate.getIgnoreTxnList(tableName);
                    cache.put(tableName, ignoreTxnList);
                }

                for (Pair<Long, Long> range : ignoreTxnList) {
                    if (txnId > range.getFirst() && txnId < range.getSecond()) {
                        result = true;
                        break;
                    }
                }
            }
        } catch (Exception e) {
            return false;
        }
        return result;
    }
}
