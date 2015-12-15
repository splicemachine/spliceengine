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
    public enum Type {INSERT,UPDATE,DELETE,INDEX};
    public void open() throws StandardException;
    public void open(TriggerHandler triggerHandler, SpliceOperation dmlWriteOperation) throws StandardException;
    public void close() throws StandardException;
    public void write(T row) throws StandardException;
    public void write(Iterator<T> rows) throws StandardException;
    public void setTxn(TxnView txn);
    public TxnView getTxn();
    public byte[] getDestinationTable();
}
