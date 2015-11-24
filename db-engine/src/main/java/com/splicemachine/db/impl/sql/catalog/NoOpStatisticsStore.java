package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.catalog.Statistics;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.StatisticsStore;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.Collections;
import java.util.List;

/**
 * A no-op implementation of a Statistics store
 * @author Scott Fines
 *         Date: 4/7/15
 */
public class NoOpStatisticsStore implements StatisticsStore{
    public static final StatisticsStore INSTANCE= new NoOpStatisticsStore();

    private NoOpStatisticsStore(){}

    @Override
    public List<Statistics> getAllStatistics(TransactionController tc,ConglomerateDescriptor td){
        return Collections.emptyList();
    }
}
