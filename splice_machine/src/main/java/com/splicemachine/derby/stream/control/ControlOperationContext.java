package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jleach on 4/17/15.
 */
public class ControlOperationContext<Op extends SpliceOperation> implements OperationContext<Op> {
        long rowsRead;
        long rowsFiltered;
        long rowsWritten;
        long rowsJoinedLeft;
        long rowsJoinedRight;
        long rowsProduced;
        List<String> badRecords;
        public SpliceObserverInstructions soi;
        public SpliceTransactionResourceImpl impl;
        public Activation activation;
        public SpliceOperationContext context;
        public Op op;
        public TxnView txn;
        protected static Logger LOG = Logger.getLogger(ControlOperationContext.class);
        private int failBadRecordCount = -1;
        private boolean permissive;
        private boolean failed;
        private int numberBadRecords = 0;
        private byte[] operationUUID;


    public ControlOperationContext() {
        }

        protected ControlOperationContext(Op spliceOperation) {
            this.op = spliceOperation;
            this.activation = op.getActivation();
            try {
                this.txn = spliceOperation.getCurrentTransaction();
            } catch (StandardException se) {
                throw new RuntimeException(se);
            }
            rowsRead = 0;
            rowsFiltered=0;
            rowsWritten = 0;
            badRecords = new ArrayList<String>();
            operationUUID = spliceOperation.getUniqueSequenceID();
        }

        public void readExternalInContext(ObjectInput in) throws IOException, ClassNotFoundException
        {}

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            if (soi == null)
                soi = SpliceObserverInstructions.create(op.getActivation(), op);
            out.writeObject(soi);
            out.writeObject(op);
            TransactionOperations.getOperationFactory().writeTxn(txn, out);
       }

        @Override
        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException {
            SpliceSpark.setupSpliceStaticComponents();
            soi = (SpliceObserverInstructions) in.readObject();
            op = (Op) in.readObject();
            txn = TransactionOperations.getOperationFactory().readTxn(in);
            boolean prepared = false;
            try {
                impl = new SpliceTransactionResourceImpl();
                impl.prepareContextManager();
                prepared = true;
                impl.marshallTransaction(txn);
                activation = soi.getActivation(impl.getLcc());
                context = SpliceOperationContext.newContext(activation);
                op.init(context);
                readExternalInContext(in);
            } catch (Exception e) {
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
            } finally {
                if (prepared) {
                    impl.resetContextManager();
                }
            }
        }

    @Override
    public void prepare() {

    }

    @Override
    public void reset() {

    }

    @Override
    public Op getOperation() {
        return op;
    }

    @Override
    public Activation getActivation() {
        return op.getActivation();
    }

    @Override
    public TxnView getTxn() {
        try {
            return op.getCurrentTransaction();
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
    public void recordJoinedLeft() {
        rowsJoinedLeft++;
    }

    @Override
    public void recordJoinedRight() {
        rowsJoinedRight++;
    }

    @Override
    public void recordProduced() {
        rowsProduced++;
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

    @Override
    public void pushScope(String display) {
        // no op
    }

    @Override
    public void pushScope() {
        // no op
    }

    @Override
    public void popScope() {
        // no op
    }

    @Override
    public void recordBadRecord(String badRecord) {
        numberBadRecords++;
        badRecords.add(badRecord);
        if (numberBadRecords>= this.failBadRecordCount)
            failed=true;
    }

    @Override
    public List<String> getBadRecords() {
        return badRecords;
    }

    @Override
    public byte[] getOperationUUID() {
        return operationUUID;
    }

    @Override
    public boolean isPermissive() {
        return permissive;
    }

    @Override
    public boolean isFailed() {
        return failed;
    }

    @Override
    public void setPermissive() {
        this.permissive=true;
    }

    @Override
    public void setFailBadRecordCount(int failBadRecordCount) {
        this.failBadRecordCount = failBadRecordCount;
    }
}