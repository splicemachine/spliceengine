package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.catalog.Statistics;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 4/7/15
 */
public interface StatisticsStore{

    List<Statistics> getAllStatistics(TransactionController tc,ConglomerateDescriptor td) throws StandardException;
}
