package com.splicemachine.si.api;

import java.util.Set;

public interface PutLog {
    Set getRows(long transactionId);
    void setRows(long transactionId, Set rows);
    void removeRows(long transactionId);
}
