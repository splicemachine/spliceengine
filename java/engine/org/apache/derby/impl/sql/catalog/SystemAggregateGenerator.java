package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 12/16/13
 * Time: 1:38 PM
 * To change this template use File | Settings | File Templates.
 */
public interface SystemAggregateGenerator {
    void createAggregates(TransactionController tc) throws StandardException;
}
