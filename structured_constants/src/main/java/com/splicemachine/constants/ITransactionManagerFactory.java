package com.splicemachine.constants;

import java.io.IOException;

public interface ITransactionManagerFactory {
    void init();
    ITransactionManager newTransactionManager(IHbaseConfigurationSource configSource) throws IOException;
}
