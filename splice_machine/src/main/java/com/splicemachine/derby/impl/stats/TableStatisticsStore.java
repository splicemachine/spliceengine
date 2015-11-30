package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.sql.dictionary.TableStatisticsDescriptor;
import com.splicemachine.si.api.TxnView;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public interface TableStatisticsStore {

    void start() throws ExecutionException;

    TableStatisticsDescriptor[] fetchTableStatistics(TxnView txn,long conglomerateId,List<String> partitionsToFetch) throws ExecutionException;

    void invalidate(long conglomerateId,Collection<String> partitionsToInvalidate);
}
