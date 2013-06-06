package com.splicemachine.si.impl;

import java.io.IOException;

public interface TransactionSource {
    Transaction getTransaction(long timestamp) throws IOException;
}
