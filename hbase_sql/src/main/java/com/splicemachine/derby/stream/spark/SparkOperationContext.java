package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.derby.stream.control.BadRecordsRecorder;
import com.splicemachine.stream.accumulator.BadRecordsAccumulator;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulable;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import java.sql.SQLException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Created by jleach on 4/17/15.
 */
public class SparkOperationContext<Op extends SpliceOperation> implements OperationContext<Op>{
    protected static Logger LOG=Logger.getLogger(SparkOperationContext.class);
    private static final String LINE_SEP = System.lineSeparator();
    private BroadcastedActivation broadcastedActivation;
    private Accumulable<BadRecordsRecorder,String> badRecordsAccumulator;

    public SpliceTransactionResourceImpl impl;
    public Activation activation;
    public SpliceOperationContext context;
    public Op op;
    public Accumulator<Long> rowsRead;
    public Accumulator<Long> rowsJoinedLeft;
    public Accumulator<Long> rowsJoinedRight;
    public Accumulator<Long> rowsProduced;
    public Accumulator<Long> rowsFiltered;
    public Accumulator<Long> rowsWritten;
    public boolean permissive;
    public long badRecordsSeen;
    public long badRecordThreshold;
    public boolean failed;

    public SparkOperationContext(){

    }

    @SuppressWarnings("unchecked")
    protected SparkOperationContext(Op spliceOperation,BroadcastedActivation broadcastedActivation){
        this.broadcastedActivation = broadcastedActivation;
        this.op=spliceOperation;
        this.activation=op.getActivation();
        String baseName="("+op.resultSetNumber()+") "+op.getName();
        AccumulatorParam param=AccumulatorParam.LongAccumulatorParam$.MODULE$;

        this.rowsRead=SpliceSpark.getContext().accumulator(0L,baseName+" rows read",param);
        this.rowsFiltered=SpliceSpark.getContext().accumulator(0l,baseName+" rows filtered",param);
        this.rowsWritten=SpliceSpark.getContext().accumulator(0l,baseName+" rows written",param);
        this.rowsJoinedLeft=SpliceSpark.getContext().accumulator(0l,baseName+" rows joined left",param);
        this.rowsJoinedRight=SpliceSpark.getContext().accumulator(0l,baseName+" rows joined right",param);
        this.rowsProduced=SpliceSpark.getContext().accumulator(0l,baseName+" rows produced",param);
    }

    @SuppressWarnings("unchecked")
    protected SparkOperationContext(Activation activation){
        this.op=null;
        this.activation=activation;
        AccumulatorParam param=AccumulatorParam.LongAccumulatorParam$.MODULE$;

        this.rowsRead=SpliceSpark.getContext().accumulator(0L,"rows read",param);
        this.rowsFiltered=SpliceSpark.getContext().accumulator(0l,"rows filtered",param);
        this.rowsWritten=SpliceSpark.getContext().accumulator(0l,"rows written",param);
        this.rowsJoinedLeft=SpliceSpark.getContext().accumulator(0l,"rows joined left",param);
        this.rowsJoinedRight=SpliceSpark.getContext().accumulator(0l,"rows joined right",param);
        this.rowsProduced=SpliceSpark.getContext().accumulator(0l, "rows produced", param);
    }

    public void readExternalInContext(ObjectInput in) throws IOException, ClassNotFoundException{
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeLong(badRecordsSeen);
        out.writeLong(badRecordThreshold);
        out.writeBoolean(permissive);
        out.writeBoolean(op!=null);
        if(op!=null){
            out.writeObject(broadcastedActivation);
            out.writeInt(op.resultSetNumber());
        }
        out.writeObject(rowsRead);
        out.writeObject(rowsFiltered);
        out.writeObject(rowsWritten);
        out.writeObject(rowsJoinedLeft);
        out.writeObject(rowsJoinedRight);
        out.writeObject(rowsProduced);
        out.writeObject(badRecordsAccumulator);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException{
        badRecordsSeen = in.readLong();
        badRecordThreshold = in.readLong();
        permissive=in.readBoolean();
        SpliceSpark.setupSpliceStaticComponents();
        boolean isOp=in.readBoolean();
        if(isOp){
            broadcastedActivation = (BroadcastedActivation)in.readObject();
            op=(Op)broadcastedActivation.getActivationHolder().getOperationsMap().get(in.readInt());
            activation=broadcastedActivation.getActivationHolder().getActivation();
        }
        rowsRead=(Accumulator)in.readObject();
        rowsFiltered=(Accumulator)in.readObject();
        rowsWritten=(Accumulator)in.readObject();
        rowsJoinedLeft=(Accumulator<Long>)in.readObject();
        rowsJoinedRight=(Accumulator<Long>)in.readObject();
        rowsProduced=(Accumulator<Long>)in.readObject();
        badRecordsAccumulator = (Accumulable<BadRecordsRecorder,String>) in.readObject();
    }

    @Override
    public void prepare(){
//        impl.prepareContextManager();
//        impl.marshallTransaction(txn);
    }

    @Override
    public void reset(){
//        impl.resetContextManager();
    }

    @Override
    public Op getOperation(){
        return op;
    }

    @Override
    public Activation getActivation(){
        return activation;
    }

    @Override
    public void recordRead(){
        rowsRead.add(1l);
    }

    @Override
    public void recordFilter(){
        rowsFiltered.add(1l);
    }

    @Override
    public void recordWrite(){
        rowsWritten.add(1l);
    }

    @Override
    public void recordJoinedLeft(){
        rowsJoinedLeft.add(1l);
    }

    @Override
    public void recordJoinedRight(){
        rowsJoinedRight.add(1l);
    }

    @Override
    public void recordProduced(){
        rowsProduced.add(1l);
    }

    @Override
    public long getRecordsRead(){
        return rowsRead.value();
    }

    @Override
    public long getRecordsFiltered(){
        return rowsFiltered.value();
    }

    @Override
    public long getRecordsWritten(){
        return rowsWritten.value();
    }


    @Override
    public void pushScope(String displayName){
        SpliceSpark.pushScope(displayName);
    }

    @Override
    public void pushScope(){
        SpliceSpark.pushScope(getOperation().getScopeName());
    }

    @Override
    public void pushScopeForOp(Scope step){
        SpliceSpark.pushScope(getOperation().getScopeName()+": "+step.displayName());
    }

    @Override
    public void pushScopeForOp(String step){
        SpliceSpark.pushScope(getOperation().getScopeName() + (step != null ? ": " + step : ""));
    }

    @Override
    public void popScope(){
        SpliceSpark.popScope();
    }

    @Override
    public TxnView getTxn(){
        return broadcastedActivation.getActivationHolder().getTxn();
    }

    @Override
    public void recordBadRecord(String badRecord, Exception e) throws StandardException {
        if (! failed) {
            ++badRecordsSeen;
            String errorState = "";
            if (e != null) {
                if (e instanceof SQLException) {
                    errorState = ((SQLException)e).getSQLState();
                } else if (e instanceof StandardException) {
                    errorState = ((StandardException)e).getSQLState();
                }
            }
            badRecordsAccumulator.add(errorState + " " + badRecord+LINE_SEP);
            if (badRecordThreshold >= 0 && badRecordsSeen > badRecordThreshold) {
                // If tolerance threshold is < 0, we're accepting all bad records
                // We can't dereference the accumulator's value (BadRecordRecorder) here on server side
                // to check failure, so we have to track it manually
                failed = true;
            }
        }
    }

    @Override
    public long getBadRecords() {
        // can only be called after we're back on the client side since we need to reference accumulator value
        long nBadRecords = (getBadRecordsRecorder() != null ? getBadRecordsRecorder().getNumberOfBadRecords() : 0);
        List<SpliceOperation> operations=getOperation().getSubOperations();
        if(operations!=null){
            for(SpliceOperation operation : operations){
                if(operation.getOperationContext()!=null)
                    nBadRecords += operation.getOperationContext().getBadRecords();
            }
        }
        return nBadRecords;
    }

    @Override
    public String getBadRecordFileName() {
        // can only be called after we're back on the client side since we need to reference accumulator value
        return (getBadRecordsRecorder() != null ? getBadRecordsRecorder().getBadRecordFileName() : "");
    }

    @Override
    public BadRecordsRecorder getBadRecordsRecorder() {
        // can only be called after we're back on the client side since we need to reference accumulator value
        if (this.badRecordsAccumulator != null) {
            return this.badRecordsAccumulator.value();
        }
        return null;
    }

    @Override
    public boolean isPermissive(){
        return permissive;
    }

    @Override
    public boolean isFailed(){
        return failed;
    }

    @Override
    public void setPermissive(String statusDirectory, String importFileName, long badRecordThreshold){
        this.permissive=true;
        this.badRecordThreshold = badRecordThreshold;
        BadRecordsRecorder badRecordsRecorder = new BadRecordsRecorder(statusDirectory, importFileName, badRecordThreshold);
        this.badRecordsAccumulator=SpliceSpark.getContext().accumulable(badRecordsRecorder,badRecordsRecorder.getUniqueName(), new BadRecordsAccumulator());
    }
}