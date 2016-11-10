/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.spark;

import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.derby.stream.control.BadRecordsRecorder;
import com.splicemachine.stream.accumulator.BadRecordsAccumulator;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulable;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;

import java.io.*;
import java.sql.SQLException;
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
    public Accumulator<Long> retryAttempts;
    public Accumulator<Long> regionTooBusyExceptions;

    public Accumulator<Long> pipelineRowsWritten;
    public Accumulator<Long> thrownErrorsRows;
    public Accumulator<Long> retriedRows;
    public Accumulator<Long> partialRows;
    public Accumulator<Long> partialThrownErrorRows;
    public Accumulator<Long> partialRetriedRows;
    public Accumulator<Long> partialIgnoredRows;
    public Accumulator<Long> partialWrite;
    public Accumulator<Long> ignoredRows;
    public Accumulator<Long> catchThrownRows;
    public Accumulator<Long> catchRetriedRows;

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

        this.retryAttempts =SpliceSpark.getContext().accumulator(0L, "(WritePipeline) retry attempts", param);
        this.regionTooBusyExceptions =SpliceSpark.getContext().accumulator(0L, "(WritePipeline) region too busy exceptions", param);
        this.pipelineRowsWritten=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows written",param);
        this.thrownErrorsRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows thrown errors",param);
        this.retriedRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows retried",param);
        this.partialRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) total rows partial error",param);
        this.partialThrownErrorRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows partial thrown error",param);
        this.partialRetriedRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows partial retried",param);
        this.partialIgnoredRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows partial ignored",param);
        this.partialWrite=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) partial write",param);
        this.ignoredRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows ignored error",param);
        this.catchThrownRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows catch error rethrown",param);
        this.catchRetriedRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows catch error retried",param);
    }

    @SuppressWarnings("unchecked")
    protected SparkOperationContext(Activation activation, BroadcastedActivation broadcastedActivation){
        this.op=null;
        this.activation=activation;
        this.broadcastedActivation = broadcastedActivation;
        AccumulatorParam param=AccumulatorParam.LongAccumulatorParam$.MODULE$;

        this.rowsRead=SpliceSpark.getContext().accumulator(0L,"rows read",param);
        this.rowsFiltered=SpliceSpark.getContext().accumulator(0l,"rows filtered",param);
        this.rowsWritten=SpliceSpark.getContext().accumulator(0l,"rows written",param);
        this.rowsJoinedLeft=SpliceSpark.getContext().accumulator(0l,"rows joined left",param);
        this.rowsJoinedRight=SpliceSpark.getContext().accumulator(0l,"rows joined right",param);
        this.rowsProduced=SpliceSpark.getContext().accumulator(0l,"rows produced",param);

        this.retryAttempts =SpliceSpark.getContext().accumulator(0L, "(WritePipeline) retry attempts", param);
        this.regionTooBusyExceptions =SpliceSpark.getContext().accumulator(0L, "(WritePipeline) region too busy exceptions", param);
        this.pipelineRowsWritten=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows written",param);;
        this.thrownErrorsRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows thrown errors",param);
        this.retriedRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows retried",param);
        this.partialRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) total rows partial error",param);
        this.partialThrownErrorRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows partial thrown error",param);
        this.partialRetriedRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows partial retried",param);
        this.partialIgnoredRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows partial ignored",param);
        this.partialWrite=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) partial write",param);
        this.ignoredRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows ignored error",param);
        this.catchThrownRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows catch error rethrown",param);
        this.catchRetriedRows=SpliceSpark.getContext().accumulator(0l,"(WritePipeline) rows catch error retried",param);
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
        out.writeObject(retryAttempts);
        out.writeObject(regionTooBusyExceptions);
        out.writeObject(rowsJoinedLeft);
        out.writeObject(rowsJoinedRight);
        out.writeObject(rowsProduced);
        out.writeObject(badRecordsAccumulator);
        out.writeObject(thrownErrorsRows);
        out.writeObject(retriedRows);
        out.writeObject(partialRows);
        out.writeObject(partialThrownErrorRows);
        out.writeObject(partialRetriedRows);
        out.writeObject(partialIgnoredRows);
        out.writeObject(partialWrite);
        out.writeObject(ignoredRows);
        out.writeObject(catchThrownRows);
        out.writeObject(catchRetriedRows);
        out.writeObject(pipelineRowsWritten);
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
        retryAttempts =(Accumulator)in.readObject();
        regionTooBusyExceptions =(Accumulator)in.readObject();
        rowsJoinedLeft=(Accumulator<Long>)in.readObject();
        rowsJoinedRight=(Accumulator<Long>)in.readObject();
        rowsProduced=(Accumulator<Long>)in.readObject();
        badRecordsAccumulator = (Accumulable<BadRecordsRecorder,String>) in.readObject();

        thrownErrorsRows=(Accumulator<Long>)in.readObject();
        retriedRows=(Accumulator<Long>)in.readObject();
        partialRows=(Accumulator<Long>)in.readObject();
        partialThrownErrorRows=(Accumulator<Long>)in.readObject();
        partialRetriedRows=(Accumulator<Long>)in.readObject();
        partialIgnoredRows=(Accumulator<Long>)in.readObject();
        partialWrite=(Accumulator<Long>)in.readObject();
        ignoredRows=(Accumulator<Long>)in.readObject();
        catchThrownRows=(Accumulator<Long>)in.readObject();
        catchRetriedRows=(Accumulator<Long>)in.readObject();
        pipelineRowsWritten=(Accumulator<Long>)in.readObject();
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
    public void recordPipelineWrites(long w) {
        pipelineRowsWritten.add(w);
    }

    @Override
    public void recordThrownErrorRows(long w) {
        thrownErrorsRows.add(w);
    }

    @Override
    public void recordRetriedRows(long w) {

        retriedRows.add(w);
    }

    @Override
    public void recordPartialRows(long w) {

        partialRows.add(w);
    }

    @Override
    public void recordPartialThrownErrorRows(long w) {

        partialThrownErrorRows.add(w);
    }

    @Override
    public void recordPartialRetriedRows(long w) {

        partialRetriedRows.add(w);
    }

    @Override
    public void recordPartialIgnoredRows(long w) {

        partialIgnoredRows.add(w);
    }

    @Override
    public void recordPartialWrite(long w) {

        partialWrite.add(w);
    }

    @Override
    public void recordIgnoredRows(long w) {

        ignoredRows.add(w);
    }

    @Override
    public void recordCatchThrownRows(long w) {

        catchThrownRows.add(w);
    }

    @Override
    public void recordCatchRetriedRows(long w) {

        catchRetriedRows.add(w);
    }

    @Override
    public void recordRetry(long w){
        retryAttempts.add(w);
    }

    @Override
    public void recordRegionTooBusy(long w){
        regionTooBusyExceptions.add(w);
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
    public long getRetryAttempts(){
        return retryAttempts.value();
    }

    public long getRegionTooBusyExceptions(){
        return regionTooBusyExceptions.value();
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
    public void recordBadRecord(String badRecord, Exception e) {
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
	if(importFileName == null)importFileName="unspecified_" + System.currentTimeMillis();
        BadRecordsRecorder badRecordsRecorder = new BadRecordsRecorder(statusDirectory,
                                                                       importFileName,
                                                                       badRecordThreshold);
        this.badRecordsAccumulator=SpliceSpark.getContext().accumulable(badRecordsRecorder,
                                                                        badRecordsRecorder.getUniqueName(),
                                                                        new BadRecordsAccumulator());
    }

    public ActivationHolder getActivationHolder() {
        return broadcastedActivation!= null ? broadcastedActivation.getActivationHolder() : null;
    }

    @Override
    public OperationContext getClone() throws IOException, ClassNotFoundException{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(this);
        oos.flush();
        oos.close();
        ByteString bs = ZeroCopyLiteralByteString.wrap(baos.toByteArray());

        // Deserialize activation to clone it
        InputStream is = bs.newInput();
        ObjectInputStream ois = new ObjectInputStream(is);
        SparkOperationContext operationContext = (SparkOperationContext) ois.readObject();
        BroadcastedActivation  broadcastedActivation = operationContext.broadcastedActivation;
        BroadcastedActivation.ActivationHolderAndBytes activationHolderAndBytes = broadcastedActivation.readActivationHolder();
        broadcastedActivation.setActivationHolder(activationHolderAndBytes.getActivationHolder());
        operationContext.op = broadcastedActivation.getActivationHolder().getOperationsMap().get(op.resultSetNumber());
        operationContext.activation = operationContext.broadcastedActivation.getActivationHolder().getActivation();
        return operationContext;
    }
}
