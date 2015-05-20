package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.TxnView;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/17/15.
 */
public class ControlOperationContext<Op extends SpliceOperation> implements OperationContext<Op> {
        protected Op spliceOperation;
        long rowsRead;
        long rowsFiltered;
        long rowsWritten;

        public ControlOperationContext() {
        }

        protected ControlOperationContext(Op spliceOperation) {
            this.spliceOperation = spliceOperation;
            rowsRead = 0;
            rowsFiltered=0;
            rowsWritten = 0;
        }

        public void readExternalInContext(ObjectInput in) throws IOException, ClassNotFoundException
        {}

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            throw new RuntimeException("Control Side Should Never Be Serialized");
       }

        @Override
        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException {
            throw new RuntimeException("Control Side Should Never Be Serialized");
        }

    @Override
    public void prepare() {

    }

    @Override
    public void reset() {

    }

    @Override
    public Op getOperation() {
        return spliceOperation;
    }

    @Override
    public Activation getActivation() {
        return spliceOperation.getActivation();
    }

    @Override
    public TxnView getTxn() {
        try {
            return spliceOperation.getCurrentTransaction();
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }


    @Override
    public void recordRead() {
        rowsRead++;
    }

    @Override
    public void recordFilter() {
        rowsFiltered++;
    }

    @Override
    public void recordWrite() {
        rowsWritten++;
    }

    @Override
    public long getRecordsRead() {
        return rowsRead;
    }

    @Override
    public long getRecordsFiltered() {
        return rowsFiltered;
    }

    @Override
    public long getRecordsWritten() {
        return rowsWritten;
    }
}