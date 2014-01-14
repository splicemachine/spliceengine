package com.splicemachine.si.impl;

import java.io.IOException;

public interface TransactionStoreCallback<T, Table> {
    T withTable(Table table) throws IOException;
}
