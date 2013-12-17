package org.apache.derby.impl.sql.catalog;

import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 12/16/13
 * Time: 1:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class DefaultSystemAggregateGenerator implements SystemAggregateGenerator{

    private DataDictionary dictionary;

    public DefaultSystemAggregateGenerator(DataDictionary dictionary) {
        this.dictionary = dictionary;
    }
    public void createAggregates(TransactionController tc) throws StandardException {

        {
        Aggregate aggregate = new Aggregate(
                "STDDEV_POP",
                TypeDescriptor.DOUBLE,
                TypeDescriptor.DOUBLE,
                "com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevPop");

        aggregate.createSystemAggregate(dictionary, tc);
        }

        {
        Aggregate aggregate = new Aggregate(
                "STDDEV_SAMP",
                TypeDescriptor.DOUBLE,
                TypeDescriptor.DOUBLE,
                "com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevSamp");

        aggregate.createSystemAggregate(dictionary, tc);
        }
    }
}
