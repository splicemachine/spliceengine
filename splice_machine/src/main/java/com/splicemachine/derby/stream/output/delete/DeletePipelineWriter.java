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

package com.splicemachine.derby.stream.output.delete;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DeleteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.AbstractPipelineWriter;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.Exceptions;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by jleach on 5/5/15.
 */
public class DeletePipelineWriter extends AbstractPipelineWriter<ExecRow>{
    private static final FixedDataHash EMPTY_VALUES_ENCODER = new FixedDataHash(new byte[]{});
    protected static final KVPair.Type dataType = KVPair.Type.DELETE;
    protected PairEncoder encoder;
    public int rowsDeleted = 0;
    protected DeleteOperation deleteOperation;

    public DeletePipelineWriter(Txn txn,long heapConglom,OperationContext operationContext) throws StandardException {
        super(txn,heapConglom,operationContext);
        if (operationContext != null) {
            deleteOperation = (DeleteOperation)operationContext.getOperation();
        }
    }

    public void open() throws StandardException {
        open(deleteOperation != null ? deleteOperation.getTriggerHandler() : null, deleteOperation);
    }

    public void open(TriggerHandler triggerHandler, SpliceOperation operation) throws StandardException {
        super.open(triggerHandler, operation);
        try{
            writeBuffer=writeCoordinator.writeBuffer(destinationTable,txn,Metrics.noOpMetricFactory());
            encoder=new PairEncoder(getKeyEncoder(),getRowHash(),dataType);
            flushCallback=triggerHandler==null?null:TriggerHandler.flushCallback(writeBuffer);
        }catch(IOException ioe){
           throw Exceptions.parseException(ioe);
        }
    }
    public KeyEncoder getKeyEncoder() throws StandardException {
        return new KeyEncoder(NoOpPrefix.INSTANCE,new DataHash<ExecRow>(){
            private ExecRow currentRow;

            @Override
            public void setRow(ExecRow rowToEncode) {
                this.currentRow = rowToEncode;
            }

            @Override
            public byte[] encode() throws StandardException, IOException {
                RowLocation location = (RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject();
                return location.getBytes();
            }

            @Override public void close() throws IOException {  }

            @Override public KeyHashDecoder getDecoder() {
                return NoOpKeyHashDecoder.INSTANCE;
            }
        },NoOpPostfix.INSTANCE);
    }

    public DataHash getRowHash() throws StandardException {
        return EMPTY_VALUES_ENCODER;
    }

    public void delete(ExecRow execRow) throws StandardException {
        try {
            beforeRow(execRow);
            KVPair encode = encoder.encode(execRow);
            rowsDeleted++;
            writeBuffer.add(encode);
            TriggerHandler.fireAfterRowTriggers(triggerHandler, execRow, flushCallback);
            operationContext.recordWrite();
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    public void delete(Iterator<ExecRow> execRows) throws StandardException {
        while (execRows.hasNext())
            delete(execRows.next());
    }

    public void write(ExecRow execRow) throws StandardException {
        delete(execRow);
    }

    public void write(Iterator<ExecRow> execRows) throws StandardException {
        delete(execRows);
    }

}
