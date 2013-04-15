package com.splicemachine.si2.api;

import java.io.IOException;

public interface TransactorFactory {
    void init();
    Transactor newTransactionManager(HbaseConfigurationSource configSource) throws IOException;
}
