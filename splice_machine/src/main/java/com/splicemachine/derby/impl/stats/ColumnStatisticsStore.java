package com.splicemachine.derby.impl.stats;

import com.splicemachine.si.api.TxnView;
import com.splicemachine.stats.ColumnStatistics;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public interface ColumnStatisticsStore {

    public Map<String,List<ColumnStatistics>> fetchColumnStats(TxnView txn, long conglomerateId, Collection<String> partitions) throws ExecutionException;

}
