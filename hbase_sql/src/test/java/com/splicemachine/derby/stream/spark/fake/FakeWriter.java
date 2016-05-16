package com.splicemachine.derby.stream.spark.fake;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.si.api.txn.TxnView;

import java.util.Iterator;

/**
 * Created by dgomezferro on 4/26/16.
 */
public class FakeWriter implements TableWriter {
    private final boolean fail;

    public FakeWriter(boolean fail) {
        this.fail = fail;
    }

    @Override
    public void open() throws StandardException {
    }

    @Override
    public void open(TriggerHandler triggerHandler, SpliceOperation dmlWriteOperation) throws StandardException {
    }

    @Override
    public void close() throws StandardException {
    }

    @Override
    public void write(Object row) throws StandardException {
        if (fail) {
            throw new NullPointerException("failed");
        }
    }

    @Override
    public void write(Iterator rows) throws StandardException {
        if (fail) {
            throw new NullPointerException("failed");
        }
    }

    @Override
    public void setTxn(TxnView childTxn) {
    }

    @Override
    public TxnView getTxn() {
        return null;
    }

    @Override
    public byte[] getDestinationTable() {
        return new byte[0];
    }
}
