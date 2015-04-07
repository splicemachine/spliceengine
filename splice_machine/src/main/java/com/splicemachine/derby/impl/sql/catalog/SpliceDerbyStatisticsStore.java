package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.db.catalog.Statistics;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.StatisticsStore;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.stats.PartitionStatsStore;
import com.splicemachine.derby.impl.stats.StatisticsStorage;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.stats.TableStatistics;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 4/7/15
 */
public class SpliceDerbyStatisticsStore implements StatisticsStore{
    private final PartitionStatsStore tableStats;

    public SpliceDerbyStatisticsStore(){
        this.tableStats =StatisticsStorage.getPartitionStore();
    }

    public SpliceDerbyStatisticsStore(PartitionStatsStore tableStats){
        this.tableStats=tableStats;
    }

    @Override
    public List<Statistics> getAllStatistics(TransactionController tc,ConglomerateDescriptor td) throws StandardException{
        TxnView txn=((SpliceTransactionManager)tc).getRawTransaction().getActiveStateTxn();
        try{
            TableStatistics statistics=tableStats.getStatistics(txn,td.getConglomerateNumber());
            return Collections.<Statistics>singletonList(new SpliceDerbyStatistics(statistics));
        }catch(ExecutionException e){
            throw Exceptions.parseException(e.getCause());
        }
    }
}
