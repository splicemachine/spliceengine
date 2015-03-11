package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;

/**
 * Default implementation class for {@link SystemAggregateGenerator}.
 * 
 * @author Jun Yuan
 */
public class DefaultSystemAggregateGenerator implements SystemAggregateGenerator{

    protected DataDictionary dictionary;

    public DefaultSystemAggregateGenerator(DataDictionary dictionary) {
        this.dictionary = dictionary;
    }
    
    public void createAggregates(TransactionController tc) throws StandardException {
        /*
         * Derby does not add any custom aggregates by default.
         */
//
//        UUID sysFunUUID = dictionary.getSysFunSchemaDescriptor().getUUID();
//
//        {
//        Aggregate aggregate = new Aggregate(
//                "STDDEV_POP",
//                TypeDescriptor.DOUBLE,
//                TypeDescriptor.DOUBLE,
//                "com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevPop");
//
//        aggregate.createSystemAggregate(dictionary, tc, sysFunUUID);
//        }
//
//        {
//        Aggregate aggregate = new Aggregate(
//                "STDDEV_SAMP",
//                TypeDescriptor.DOUBLE,
//                TypeDescriptor.DOUBLE,
//                "com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevSamp");
//
//        aggregate.createSystemAggregate(dictionary, tc, sysFunUUID);
//        }
    }
}
