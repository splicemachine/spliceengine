package com.splicemachine.si2.txn;

import com.splicemachine.constants.ITransactionManager;
import com.splicemachine.constants.ITransactionManagerFactory;

import java.io.IOException;

public class TransactionManagerFactory implements ITransactionManagerFactory {
    @Override
    public void init() {
    }

    @Override
    public ITransactionManager newTransactionManager() throws IOException {
        return new TransactionManager();
    }
}
