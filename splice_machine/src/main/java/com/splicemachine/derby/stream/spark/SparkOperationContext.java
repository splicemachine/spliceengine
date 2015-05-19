package com.splicemachine.derby.stream.spark;

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
import org.apache.spark.Accumulator;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/17/15.
 */
public class SparkOperationContext<Op extends SpliceOperation> implements OperationContext<Op> {
        protected static Logger LOG = Logger.getLogger(SparkOperationContext.class);
        public SpliceObserverInstructions soi;
        public SpliceTransactionResourceImpl impl;
        public Activation activation;
        public SpliceOperationContext context;
        public Op op;
        public TxnView txn;
        public Accumulator<Integer> rowsRead;
        public Accumulator<Integer> rowsFiltered;
        public Accumulator<Integer> rowsWritten;

        public SparkOperationContext() {
        }

        protected SparkOperationContext(Op spliceOperation)  {
            this.op = spliceOperation;
            this.activation = op.getActivation();
            try {
                this.txn = spliceOperation.getCurrentTransaction();
            } catch (StandardException se) {
                throw new RuntimeException(se);
            }
            this.rowsRead = SpliceSpark.getContext().accumulator(0, op.getName()+" rows read");
            this.rowsFiltered = SpliceSpark.getContext().accumulator(0, op.getName()+" rows filtered");
            this.rowsWritten = SpliceSpark.getContext().accumulator(0, op.getName()+" rows written");
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
            out.writeObject(rowsRead);
            out.writeObject(rowsFiltered);
            out.writeObject(rowsWritten);
        }

        @Override
        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException {
            SpliceSpark.setupSpliceStaticComponents();
            soi = (SpliceObserverInstructions) in.readObject();
            op = (Op) in.readObject();
            txn = TransactionOperations.getOperationFactory().readTxn(in);
            rowsRead = (Accumulator) in.readObject();
            rowsFiltered = (Accumulator) in.readObject();
            rowsWritten = (Accumulator) in.readObject();
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
        impl.prepareContextManager();
    }

    @Override
    public void reset() {
        impl.resetContextManager();
    }

    @Override
    public Op getOperation() {
        return op;
    }

    @Override
    public Activation getActivation() {
        return activation;
    }

    @Override
    public TxnView getTxn() {
        return txn;
    }

    @Override
    public void recordRead() {
        rowsRead.add(1);
    }

    @Override
    public void recordFilter() {
        rowsFiltered.add(1);
    }

    @Override
    public void recordWrite() {
        rowsWritten.add(1);
    }
}