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

package com.splicemachine.derby.stream.output.delete;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DeleteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.AbstractPipelineWriter;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.config.RollforwardWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.si.api.txn.TxnView;
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

    public DeletePipelineWriter(TxnView txn,byte[] token,long heapConglom,long tempConglomID, String tableVersion, ExecRow execRowDefinition, OperationContext operationContext) throws StandardException {
        super(txn, token, heapConglom, tempConglomID, tableVersion, execRowDefinition, operationContext);
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
            WriteConfiguration writeConfiguration = writeCoordinator.defaultWriteConfiguration();
            if(rollforward)
                writeConfiguration = new RollforwardWriteConfiguration(writeConfiguration);
            writeBuffer=writeCoordinator.writeBuffer(destinationTable,txn,null, PipelineUtils.noOpFlushHook, writeConfiguration, Metrics.noOpMetricFactory());
            encoder=new PairEncoder(getKeyEncoder(),getRowHash(),dataType);
            flushCallback=triggerHandler==null?null:TriggerHandler.flushCallback(writeBuffer);

        if (triggerHandler != null && triggerHandler.hasStatementTriggerWithReferencingClause())
            triggerRowsEncoder=new PairEncoder(getTriggerKeyEncoder(),getTriggerRowHash(),KVPair.Type.INSERT);
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
            if (triggerRowsEncoder != null) {
                KVPair encodeTriggerRow = triggerRowsEncoder.encode(execRow);
                addRowToTriggeringResultSet(execRow, encodeTriggerRow);
            }
            TriggerHandler.fireAfterRowTriggers(triggerHandler, execRow, flushCallback);
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
