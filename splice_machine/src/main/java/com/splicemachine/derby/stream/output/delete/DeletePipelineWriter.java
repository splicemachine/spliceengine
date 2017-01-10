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
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DeleteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.AbstractPipelineWriter;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.RecordType;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by jleach on 5/5/15.
 */
public class DeletePipelineWriter extends AbstractPipelineWriter<ExecRow>{
    protected static final RecordType dataType = RecordType.DELETE;
    public int rowsDeleted = 0;
    protected DeleteOperation deleteOperation;

    public DeletePipelineWriter(Txn txn, long heapConglom, OperationContext operationContext) throws StandardException {
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
            flushCallback=triggerHandler==null?null:TriggerHandler.flushCallback(writeBuffer);
        }catch(IOException ioe){
           throw Exceptions.parseException(ioe);
        }
    }
    public void delete(ExecRow execRow) throws StandardException {
        try {
            beforeRow(execRow);
            rowsDeleted++;
//            writeBuffer.add(encode); JL -TODOD
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
