package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.si.api.TxnView;
import java.util.Iterator;

/**
 * Created by jleach on 5/20/15.
 */
public interface TableWriter {
    public enum Type {INSERT,UPDATE,DELETE};
    public void open() throws StandardException;
    public void open(TriggerHandler triggerHandler, SpliceOperation dmlWriteOperation) throws StandardException;
    public void close() throws StandardException;
    public void write(ExecRow execRow) throws StandardException;
    public void write(Iterator<ExecRow> execRows) throws StandardException;
    public void setTxn(TxnView txn);
    public TxnView getTxn();
    public byte[] getDestinationTable();
}
