package com.splicemachine.derby.stream.temporary;

import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.si.api.TxnView;

/**
 * Created by jleach on 5/20/15.
 */
public abstract class AbstractTableWriter implements AutoCloseable, TableWriter {
    protected TxnView txn;
    protected byte[] destinationTable;
    protected long heapConglom;

    public AbstractTableWriter() {

    }

    public AbstractTableWriter (TxnView txn, long heapConglom) {
        this.txn = txn;
        this.heapConglom = heapConglom;
        destinationTable = Long.toString(heapConglom).getBytes();
    }

    @Override
    public void setTxn(TxnView txn) {
        this.txn = txn;
    }

    @Override
    public TxnView getTxn() {
        return txn;
    }

    @Override
    public byte[] getDestinationTable() {
        return destinationTable;
    }

}
