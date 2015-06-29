package com.splicemachine.si.api;

import com.splicemachine.si.impl.SimpleOperationFactory;

/**
 * Utility class for constructing a TxnOperationFactory.
 *
 * @author Scott Fines
 * Date: 7/8/14
 */
public class TransactionOperations {

    private static volatile TxnOperationFactory operationFactory;
    private static final Object lock = new Object();

    public static TxnOperationFactory getOperationFactory(){
        TxnOperationFactory factory = operationFactory;
        if(factory==null)
            factory = initialize();
        return factory;
    }

    private static TxnOperationFactory initialize() {
        synchronized (lock){
            TxnOperationFactory factory = operationFactory;
            if(factory!=null) return factory;

            factory = new SimpleOperationFactory();
            operationFactory = factory;
            return factory;
        }
    }
}
