/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.derby.stream.control.BadRecordsRecorder;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.stream.accumulator.BadRecordsAccumulator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.TaskContext;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.TaskCompletionListener;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * Operation Context for Spark that avoids memory intensive accumulators.
 *
 */
public class SparkLeanOperationContext<Op extends SpliceOperation> implements OperationContext<Op>{
    protected static Logger LOG=Logger.getLogger(SparkLeanOperationContext.class);
    private static final String LINE_SEP = System.lineSeparator();
    private static final String BAD_FILENAME = "unspecified_";
    protected BroadcastedActivation broadcastedActivation;
    private AccumulatorV2<String,BadRecordsRecorder> badRecordsAccumulator;

    private SpliceTransactionResourceImpl impl;
    protected Activation activation;
    private SpliceOperationContext context;
    protected Op op;

    private LongAccumulator rowsWritten;
    private boolean permissive;
    private long badRecordsSeen;
    private long badRecordThreshold;
    private boolean failed;
    private String importFileName;

    public SparkLeanOperationContext(){
    }

    @SuppressWarnings("unchecked")
    protected SparkLeanOperationContext(Op spliceOperation, BroadcastedActivation broadcastedActivation){
        this.broadcastedActivation = broadcastedActivation;
        this.op=spliceOperation;
        this.activation=op.getActivation();
        String baseName="("+op.resultSetNumber()+") "+op.getName();
        // We use this accumulator to report written rows back to the user, we always need it
        this.rowsWritten=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows written");
    }

    @SuppressWarnings("unchecked")
    protected SparkLeanOperationContext(Activation activation, BroadcastedActivation broadcastedActivation){
        this.op=null;
        this.activation=activation;
        this.broadcastedActivation = broadcastedActivation;
        this.rowsWritten=SpliceSpark.getContext().sc().longAccumulator("rows written");
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
        out.writeObject(badRecordsAccumulator);
        out.writeObject(importFileName);
        out.writeObject(rowsWritten);
    }

    @Override
    @SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification = "intended")
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
        badRecordsAccumulator = (AccumulatorV2<String,BadRecordsRecorder>) in.readObject();
        importFileName= (String) in.readObject();
        rowsWritten=(LongAccumulator)in.readObject();
    }

    @Override
    public void prepare(){
    }

    @Override
    public void reset(){
        if (this.permissive) {
            BadRecordsRecorder oldRecorder = this.badRecordsAccumulator.value();
            BadRecordsRecorder badRecordsRecorder = new BadRecordsRecorder(
                    oldRecorder.getStatusDirectory(),
                    importFileName,
                    badRecordThreshold);
            this.badRecordsAccumulator = new BadRecordsAccumulator(badRecordsRecorder);

            SpliceSpark.getContext().sc().register(this.badRecordsAccumulator, badRecordsRecorder.getUniqueName());
        }
        badRecordsSeen = 0;
        String baseName="";
        if (op != null) {
            baseName = "(" + op.resultSetNumber() + ") " + op.getName() + " ";
        }
        this.rowsWritten = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows written");

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
    public void recordJoinedLeft() {

    }

    @Override
    public void recordJoinedRight() {

    }

    @Override
    public long getRecordsRead() {
        return 0;
    }

    @Override
    public long getRecordsFiltered() {
        return 0;
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
    public long getRetryAttempts() {
        return 0;
    }

    @Override
    public long getRegionTooBusyExceptions() {
        return 0;
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
    public long getRecordsWritten(){
        return rowsWritten.value();
    }

    @Override
    public void recordRead() {

    }

    @Override
    public void recordRead(long w) {

    }

    @Override
    public void recordFilter() {

    }

    @Override
    public void recordFilter(long w) {

    }

    @Override
    public void recordThrownErrorRows(long w) {

    }

    @Override
    public void recordRetriedRows(long w) {

    }

    @Override
    public void recordPartialRows(long w) {

    }

    @Override
    public void recordPartialThrownErrorRows(long w) {

    }

    @Override
    public void recordPartialRetriedRows(long w) {

    }

    @Override
    public void recordPartialIgnoredRows(long w) {

    }

    @Override
    public void recordPartialWrite(long w) {

    }

    @Override
    public void recordIgnoredRows(long w) {

    }

    @Override
    public void recordCatchThrownRows(long w) {

    }

    @Override
    public void recordCatchRetriedRows(long w) {

    }

    @Override
    public void recordRetry(long w) {

    }

    @Override
    public void recordProduced() {

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
    public void recordRegionTooBusy(long w) {

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
        this.badRecordsAccumulator = new BadRecordsAccumulator(badRecordsRecorder);

        SpliceSpark.getContext().sc().register(this.badRecordsAccumulator, badRecordsRecorder.getUniqueName());
    }

    @Override
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
        try (InputStream is = bs.newInput();
             ObjectInputStream ois = new ObjectInputStream(is)) {
            SparkLeanOperationContext operationContext = (SparkLeanOperationContext) ois.readObject();
            BroadcastedActivation broadcastedActivation = operationContext.broadcastedActivation;
            BroadcastedActivation.ActivationHolderAndBytes activationHolderAndBytes = broadcastedActivation.readActivationHolder();
            broadcastedActivation.setActivationHolder(activationHolderAndBytes.getActivationHolder());
            operationContext.op = broadcastedActivation.getActivationHolder().getOperationsMap().get(op.resultSetNumber());
            operationContext.activation = operationContext.broadcastedActivation.getActivationHolder().getActivation();
            return operationContext;
        }
    }
}
