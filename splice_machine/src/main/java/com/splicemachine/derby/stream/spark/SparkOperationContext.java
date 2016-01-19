package com.splicemachine.derby.stream.spark;

import com.google.common.collect.Maps;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stream.accumulator.BadRecordsAccumulator;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulable;
import org.apache.spark.Accumulator;
import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jleach on 4/17/15.
 */
public class SparkOperationContext<Op extends SpliceOperation> implements OperationContext<Op> {
        protected static Logger LOG = Logger.getLogger(SparkOperationContext.class);
        private BroadcastedActivation broadcastedActivation;

        public SpliceTransactionResourceImpl impl;
        public Activation activation;
        public SpliceOperationContext context;
        public Op op;
        public Accumulator<Integer> rowsRead;
        public Accumulator<Integer> rowsJoinedLeft;
        public Accumulator<Integer> rowsJoinedRight;
        public Accumulator<Integer> rowsProduced;
        public Accumulator<Integer> rowsFiltered;
        public Accumulator<Integer> rowsWritten;
        public Accumulable<List<String>,String> badRecordsAccumulable;
        public boolean permissive;
        public boolean failed;
        public int numberBadRecords = 0;
        private int failBadRecordCount = -1;
        private byte[] operationUUID;

        public SparkOperationContext() {

        }

        protected SparkOperationContext(Op spliceOperation, BroadcastedActivation broadcastedActivation)  {
            this.broadcastedActivation = broadcastedActivation;
            this.op = spliceOperation;
            this.activation = op.getActivation();
            String baseName = "(" + op.resultSetNumber() + ") " + op.getName();
            this.rowsRead = SpliceSpark.getContext().accumulator(0, baseName + " rows read");
            this.rowsFiltered = SpliceSpark.getContext().accumulator(0, baseName + " rows filtered");
            this.rowsWritten = SpliceSpark.getContext().accumulator(0, baseName + " rows written");
            this.rowsJoinedLeft = SpliceSpark.getContext().accumulator(0, baseName + " rows joined left");
            this.rowsJoinedRight = SpliceSpark.getContext().accumulator(0, baseName + " rows joined right");
            this.rowsProduced= SpliceSpark.getContext().accumulator(0, baseName + " rows produced");
            this.badRecordsAccumulable = SpliceSpark.getContext().accumulable(new ArrayList<String>(), baseName+" BadRecords",new BadRecordsAccumulator());
            this.operationUUID = spliceOperation.getUniqueSequenceID();
        }

        protected SparkOperationContext(Activation activation)  {
            this.op = null;
            this.activation = activation;
            this.rowsRead = SpliceSpark.getContext().accumulator(0, "rows read");
            this.rowsFiltered = SpliceSpark.getContext().accumulator(0, "rows filtered");
            this.rowsWritten = SpliceSpark.getContext().accumulator(0, "rows written");
            this.rowsJoinedLeft = SpliceSpark.getContext().accumulator(0, "rows joined left");
            this.rowsJoinedRight = SpliceSpark.getContext().accumulator(0, "rows joined right");
            this.rowsProduced= SpliceSpark.getContext().accumulator(0, "rows produced");
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(failBadRecordCount);
            out.writeBoolean(permissive);
            out.writeBoolean(op!=null);
            if (op!=null) {
                out.writeObject(broadcastedActivation);
                out.writeInt(op.resultSetNumber());
            }
            out.writeObject(rowsRead);
            out.writeObject(rowsFiltered);
            out.writeObject(rowsWritten);
            out.writeObject(rowsJoinedLeft);
            out.writeObject(rowsJoinedRight);
            out.writeObject(rowsProduced);
            out.writeObject(badRecordsAccumulable);
            if (operationUUID != null) {
                out.writeInt(operationUUID.length);
                out.write(operationUUID);
            } else {
                out.writeInt(-1);
            }
        }

        @Override
        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException {
            failBadRecordCount = in.readInt();
            permissive = in.readBoolean();
            SpliceSpark.setupSpliceStaticComponents();
            boolean isOp = in.readBoolean();
            if (isOp) {
                broadcastedActivation = (BroadcastedActivation) in.readObject();
                op = (Op) broadcastedActivation.getActivationHolder().getOperationsMap().get(in.readInt());
                activation = broadcastedActivation.getActivationHolder().getActivation();
            }
            rowsRead = (Accumulator) in.readObject();
            rowsFiltered = (Accumulator) in.readObject();
            rowsWritten = (Accumulator) in.readObject();
            rowsJoinedLeft = (Accumulator<Integer>) in.readObject();
            rowsJoinedRight = (Accumulator<Integer>) in .readObject();
            rowsProduced = (Accumulator<Integer>) in.readObject();
            badRecordsAccumulable = (Accumulable<List<String>,String>) in.readObject();
            int len = in.readInt();
            if (len > 0) {
                operationUUID = new byte[len];
                in.readFully(operationUUID);
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

    @Override
    public void recordJoinedLeft() {
        rowsJoinedLeft.add(1);
    }

    @Override
    public void recordJoinedRight() {
        rowsJoinedRight.add(1);
    }

    @Override
    public void recordProduced() {
        rowsProduced.add(1);
    }

    @Override
    public long getRecordsRead() {
        return rowsRead.value();
    }

    @Override
    public long getRecordsFiltered() {
        return rowsFiltered.value();
    }

    @Override
    public long getRecordsWritten() {
        return rowsWritten.value();
    }


    @Override
    public void pushScope(String display) {
        SpliceSpark.pushScope(display);
    }

    @Override
    public void pushScope() {
        SpliceSpark.pushScope(getOperation().getScopeName());
    }

    @Override
    public void pushScopeForOp(String step) {
        SpliceSpark.pushScope(getOperation().getScopeName() + (step != null ? ": " + step : ""));
    }

    @Override
    public void popScope() {
        SpliceSpark.popScope();
    }

    @Override
    public void recordBadRecord(String badRecord) {
        numberBadRecords++;
        badRecordsAccumulable.add(badRecord);
        if (numberBadRecords>= this.failBadRecordCount)
            failed=true;
    }


    @Override
    public List<String> getBadRecords() {
        List<SpliceOperation> operations = getOperation().getSubOperations();
        List<String> badRecords = badRecordsAccumulable.value();
        if (operations!=null) {
            for (SpliceOperation operation : operations) {
                if (operation.getOperationContext()!=null)
                    badRecords.addAll(operation.getOperationContext().getBadRecords());
            }
        }
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
        this.permissive = true;
    }

    @Override
    public void setFailBadRecordCount(int failBadRecordCount) {
        this.failBadRecordCount = failBadRecordCount;
    }
}