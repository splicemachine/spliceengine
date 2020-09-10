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
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.log4j.Logger;
import org.apache.spark.util.LongAccumulator;

import java.io.*;

/**
 *
 * Operation Context for recording Operation activities and state.
 *
 */
public class SparkOperationContext<Op extends SpliceOperation> extends SparkLeanOperationContext<Op>{
    protected static Logger LOG=Logger.getLogger(SparkOperationContext.class);

    public LongAccumulator rowsRead;
    public LongAccumulator rowsJoinedLeft;
    public LongAccumulator rowsJoinedRight;
    public LongAccumulator rowsProduced;
    public LongAccumulator rowsFiltered;
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

    public SparkOperationContext(){

    }

    @SuppressWarnings("unchecked")
    protected SparkOperationContext(Op spliceOperation,BroadcastedActivation broadcastedActivation){
        super(spliceOperation, broadcastedActivation);
        String baseName="("+op.resultSetNumber()+") "+op.getName();
        this.rowsRead=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows read");
        this.rowsFiltered=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows filtered");
        this.rowsJoinedLeft=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows joined left");
        this.rowsJoinedRight=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows joined right");
        this.rowsProduced=SpliceSpark.getContext().sc().longAccumulator(baseName+" rows produced");
        initWritePipeline();
    }

    @SuppressWarnings("unchecked")
    protected SparkOperationContext(Activation activation, BroadcastedActivation broadcastedActivation){
        super(activation, broadcastedActivation);
        this.rowsRead=SpliceSpark.getContext().sc().longAccumulator("rows read");
        this.rowsFiltered=SpliceSpark.getContext().sc().longAccumulator("rows filtered");
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
        super.writeExternal(out);
        out.writeObject(rowsRead);
        out.writeObject(rowsFiltered);
        out.writeObject(retryAttempts);
        out.writeObject(regionTooBusyExceptions);
        out.writeObject(rowsJoinedLeft);
        out.writeObject(rowsJoinedRight);
        out.writeObject(rowsProduced);
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
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException{
        super.readExternal(in);
        rowsRead=(LongAccumulator)in.readObject();
        rowsFiltered=(LongAccumulator)in.readObject();
        retryAttempts =(LongAccumulator)in.readObject();
        regionTooBusyExceptions =(LongAccumulator)in.readObject();
        rowsJoinedLeft=(LongAccumulator)in.readObject();
        rowsJoinedRight=(LongAccumulator)in.readObject();
        rowsProduced=(LongAccumulator)in.readObject();
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
    }

    @Override
    public void prepare(){
    }

    @Override
    public void reset(){
        super.reset();
        String baseName="";
        if (op != null) {
            baseName = "(" + op.resultSetNumber() + ") " + op.getName() + " ";
        }
        this.rowsRead = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows read");
        this.rowsFiltered = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows filtered");
        this.rowsJoinedLeft = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows joined left");
        this.rowsJoinedRight = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows joined right");
        this.rowsProduced = SpliceSpark.getContext().sc().longAccumulator(baseName + "rows produced");
        initWritePipeline();
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
    public long getRetryAttempts(){
        return retryAttempts.value();
    }

    public long getRegionTooBusyExceptions(){
        return regionTooBusyExceptions.value();
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
            SparkOperationContext operationContext = (SparkOperationContext) ois.readObject();
            BroadcastedActivation broadcastedActivation = operationContext.broadcastedActivation;
            BroadcastedActivation.ActivationHolderAndBytes activationHolderAndBytes = broadcastedActivation.readActivationHolder();
            broadcastedActivation.setActivationHolder(activationHolderAndBytes.getActivationHolder());
            operationContext.op = broadcastedActivation.getActivationHolder().getOperationsMap().get(op.resultSetNumber());
            operationContext.activation = operationContext.broadcastedActivation.getActivationHolder().getActivation();
            return operationContext;
        }
    }
}
