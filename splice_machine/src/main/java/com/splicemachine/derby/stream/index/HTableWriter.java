package com.splicemachine.derby.stream.index;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.temporary.AbstractTableWriter;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;

import java.util.Iterator;
/**
 * Created by jyuan on 10/17/15.
 */
public class HTableWriter extends AbstractTableWriter<KVPair> {

    private long rowsWritten;

    public HTableWriter (TxnView txn, long heapConglom) {
        super(txn, heapConglom);
    }


    @Override
    public void open() throws StandardException {
        try {
            writeBuffer = writeCoordinator.writeBuffer(destinationTable,
                    txn, Metrics.noOpMetricFactory());
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void write(KVPair kvPair) throws StandardException {
        try {
            writeBuffer.add(kvPair);
            rowsWritten++;
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void write(Iterator<KVPair> iterator) throws StandardException {
        while (iterator.hasNext()) {
            write(iterator.next());
        }
    }

    public long getRowsWritten() {
        return rowsWritten;
    }
}
