/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.spark;

import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.client.SpliceClient;
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
import org.apache.spark.TaskContext;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.TaskCompletionListener;

import java.io.*;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * Operation Context for recording Operation activities and state.
 *
 */
public class SparkOperationContext<Op extends SpliceOperation> implements OperationContext<Op>{
    protected static Logger LOG=Logger.getLogger(SparkOperationContext.class);
    private static final String LINE_SEP = System.lineSeparator();
    private static final String BAD_FILENAME = "unspecified_";
    private BroadcastedActivation broadcastedActivation;
    private Accumulable<BadRecordsRecorder,String> badRecordsAccumulator;

    public SpliceTransactionResourceImpl impl;
    public Activation activation;
    public SpliceOperationContext context;
    public Op op;
    public LongAccumulator rowsRead;
    public LongAccumulator rowsJoinedLeft;
    public LongAccumulator rowsJoinedRight;
    public LongAccumulator rowsProduced;
    public LongAccumulator rowsFiltered;
    public LongAccumulator rowsWritten;
    public LongAccumulator retryAttempts;
    public LongAccumulator regionTooBusyExceptions;

    public LongAccumulator thrownErrorsRows;
    public LongAccumulator retriedRows;
    public LongAccumulator partialRows;
    public LongAccumulator partialThrownErrorRows;
    public LongAccumulator partialRetriedRows;
    public LongAccumulator partialIgnoredRows;
    public LongAccumulator partialWrite;
    public LongAccumulator ignoredRows;
    public LongAccumulator catchThrownRows;
    public LongAccumulator catchRetriedRows;

    public boolean permissive;
    public long badRecordsSeen;
    public long badRecordThreshold;
    public boolean failed;
    private String importFileName;

    public SparkOperationContext(){

    }

    @SuppressWarnings("unchecked")
    protected SparkOperationContext(Op spliceOperation,BroadcastedActivation broadcastedActivation){
        this.broadcastedActivation = broadcastedActivation;
        this.op=spliceOperation;
        this.activation=op.getActivation();
        String baseName="("+op.resultSetNumber()+") "+op.getName();
        this.rowsRead=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows read");
        this.rowsFiltered=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows filtered");
        this.rowsWritten=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows written");
        this.rowsJoinedLeft=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows joined left");
        this.rowsJoinedRight=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows joined right");
        this.rowsProduced=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows produced");
        initWritePipeline();
    }

    @SuppressWarnings("unchecked")
    protected SparkOperationContext(Activation activation, BroadcastedActivation broadcastedActivation){
        this.op=null;
        this.activation=activation;
        this.broadcastedActivation = broadcastedActivation;
        this.rowsRead=SpliceSpark.getContext().sc().longAccumulator("rows read");
        this.rowsFiltered=SpliceSpark.getContext().sc().longAccumulator("rows filtered");
        this.rowsWritten=SpliceSpark.getContext().sc().longAccumulator("rows written");
        this.rowsJoinedLeft=SpliceSpark.getContext().sc().longAccumulator("rows joined left");
        this.rowsJoinedRight=SpliceSpark.getContext().sc().longAccumulator("rows joined right");
        this.rowsProduced=SpliceSpark.getContext().sc().longAccumulator("rows produced");
        initWritePipeline();
    }

    private void initWritePipeline() {
        this.retryAttempts =SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) retry attempts");
        this.regionTooBusyExceptions =SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) region too busy exceptions");
        this.thrownErrorsRows=SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) rows thrown errors");
        this.retriedRows=SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) rows retried");
        this.partialRows=SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) total rows partial error");
        this.partialThrownErrorRows=SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) rows partial thrown error");
        this.partialRetriedRows=SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) rows partial retried");
        this.partialIgnoredRows=SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) rows partial ignored");
        this.partialWrite=SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) partial write");
        this.ignoredRows=SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) rows ignored error");
        this.catchThrownRows=SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) rows catch error rethrown");
        this.catchRetriedRows=SpliceSpark.getContext().sc().longAccumulator("(WritePipeline) rows catch error retried");
    }

    public void readExternalInContext(ObjectInput in) throws IOException, ClassNotFoundException{
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        if (SpliceClient.isClient()) {
            out.writeBoolean(true);
            out.writeUTF(SpliceClient.connectionString);
        } else {
            out.writeBoolean(false);
        }
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
        out.writeObject(importFileName);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException{
        if (in.readBoolean()) {
            SpliceClient.connectionString = in.readUTF();
            SpliceClient.setClient(HConfiguration.getConfiguration().getAuthenticationTokenEnabled(), SpliceClient.Mode.EXECUTOR);
        }
        badRecordsSeen = in.readLong();
        badRecordThreshold = in.readLong();
        permissive=in.readBoolean();
        SpliceSpark.setupSpliceStaticComponents();
        boolean isOp=in.readBoolean();
        if(isOp){
            broadcastedActivation = (BroadcastedActivation)in.readObject();
            ActivationHolder ah = broadcastedActivation.getActivationHolder();
            op=(Op)ah.getOperationsMap().get(in.readInt());
            activation = ah.getActivation();
            TaskContext taskContext = TaskContext.get();
            if (taskContext != null) {
                taskContext.addTaskCompletionListener((TaskCompletionListener)(ctx) -> ah.close());
            }
        }
        rowsRead=(LongAccumulator)in.readObject();
        rowsFiltered=(LongAccumulator)in.readObject();
        rowsWritten=(LongAccumulator)in.readObject();
        retryAttempts =(LongAccumulator)in.readObject();
        regionTooBusyExceptions =(LongAccumulator)in.readObject();
        rowsJoinedLeft=(LongAccumulator)in.readObject();
        rowsJoinedRight=(LongAccumulator)in.readObject();
        rowsProduced=(LongAccumulator)in.readObject();
        badRecordsAccumulator = (Accumulable<BadRecordsRecorder,String>) in.readObject();

        thrownErrorsRows=(LongAccumulator)in.readObject();
        retriedRows=(LongAccumulator)in.readObject();
        partialRows=(LongAccumulator)in.readObject();
        partialThrownErrorRows=(LongAccumulator)in.readObject();
        partialRetriedRows=(LongAccumulator)in.readObject();
        partialIgnoredRows=(LongAccumulator)in.readObject();
        partialWrite=(LongAccumulator)in.readObject();
        ignoredRows=(LongAccumulator)in.readObject();
        catchThrownRows=(LongAccumulator)in.readObject();
        catchRetriedRows=(LongAccumulator)in.readObject();
        importFileName= (String) in.readObject();
    }

    @Override
    public void prepare(){
//        impl.prepareContextManager();
//        impl.marshallTransaction(txn);
    }

    @Override
    public void reset(){
//        impl.resetContextManager();
        if (this.permissive) {
            BadRecordsRecorder oldRecorder = this.badRecordsAccumulator.value();
            BadRecordsRecorder badRecordsRecorder = new BadRecordsRecorder(
                    oldRecorder.getStatusDirectory(),
                    importFileName,
                    badRecordThreshold);
            this.badRecordsAccumulator = SpliceSpark.getContext().accumulable(badRecordsRecorder,
                    badRecordsRecorder.getUniqueName(),
                    new BadRecordsAccumulator());
        }
        badRecordsSeen = 0;
        String baseName="";
        if (op != null) {
            baseName = "(" + op.resultSetNumber() + ") " + op.getName() + " ";
        }
        this.rowsRead = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows read");
        this.rowsFiltered = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows filtered");
        this.rowsWritten = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows written");
        this.rowsJoinedLeft = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows joined left");
        this.rowsJoinedRight = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows joined right");
        this.rowsProduced = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows produced");
        initWritePipeline();

        List<SpliceOperation> operations=getOperation().getSubOperations();
        if(operations!=null){
            for(SpliceOperation operation : operations){
                if(operation.getOperationContext()!=null)
                    operation.getOperationContext().reset();
            }
        }
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
    public void recordRead(long w){
        rowsRead.add(w);
    }

    @Override
    public void recordFilter(long w){
        rowsFiltered.add(w);
    }

    @Override
    public void recordWrite(){
        rowsWritten.add(1l);
    }

    @Override
    public void recordPipelineWrites(long w) {
        rowsWritten.add(w);
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
            SpliceSpark.pushScope(op !=null?getOperation().getScopeName():"");
    }

    @Override
    public void pushScopeForOp(Scope step){
        SpliceSpark.pushScope(op !=null?getOperation().getScopeName():""+": "+step.displayName());
    }

    @Override
    public void pushScopeForOp(String step){
            SpliceSpark.pushScope(op !=null?getOperation().getScopeName():"" + (step != null ? ": " + step : ""));
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
    public String getStatusDirectory() {
        // can only be called after we're back on the client side since we need to reference accumulator value
        return (getBadRecordsRecorder() != null ? getBadRecordsRecorder().getStatusDirectory() : "");
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
        if(importFileName == null)importFileName=BAD_FILENAME + System.currentTimeMillis();
        this.importFileName = importFileName;
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
