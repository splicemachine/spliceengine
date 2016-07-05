package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.si.api.txn.TxnView;
import java.util.Iterator;

/**
 * Created by jleach on 5/20/15.
 */
public interface TableWriter <T> {
    enum Type {INSERT,UPDATE,DELETE,INDEX}

    void open() throws StandardException;
    void open(TriggerHandler triggerHandler,SpliceOperation dmlWriteOperation) throws StandardException;
    void close() throws StandardException;
    void write(T row) throws StandardException;
    void write(Iterator<T> rows) throws StandardException;
    void setTxn(TxnView txn);
    TxnView getTxn();
    byte[] getDestinationTable();
    OperationContext getOperationContext();
}
