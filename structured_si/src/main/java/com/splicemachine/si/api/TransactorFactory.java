package com.splicemachine.si.api;

import java.io.IOException;

public interface TransactorFactory {
    Transactor newTransactor(HbaseConfigurationSource configSource) throws IOException;
}
