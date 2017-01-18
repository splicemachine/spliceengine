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

package com.splicemachine.derby.stream.output.insert;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.PermissiveInsertWriteConfiguration;
import com.splicemachine.derby.stream.output.AbstractPipelineWriter;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by jleach on 5/5/15.
 */
public class InsertPipelineWriter extends AbstractPipelineWriter<ExecRow>{
    protected int[] pkCols;
    protected String tableVersion;
    protected ExecRow execRowDefinition;
    protected RowLocation[] autoIncrementRowLocationArray;
    protected RecordType dataType;
    protected SpliceSequence[] spliceSequences;
    protected InsertOperation insertOperation;
    protected boolean isUpsert;
    private Partition table;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public InsertPipelineWriter(int[] pkCols,
                                String tableVersion,
                                ExecRow execRowDefinition,
                                RowLocation[] autoIncrementRowLocationArray,
                                SpliceSequence[] spliceSequences,
                                long heapConglom,
                                Txn txn,
                                OperationContext operationContext,
                                boolean isUpsert) {
        super(txn,heapConglom,operationContext);
        assert txn !=null:"txn not supplied";
        this.pkCols = pkCols;
        this.tableVersion = tableVersion;
        this.execRowDefinition = execRowDefinition;
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        this.spliceSequences = spliceSequences;
        this.destinationTable = Bytes.toBytes(Long.toString(heapConglom));
        this.isUpsert = isUpsert;
        this.dataType = isUpsert?RecordType.UPSERT:RecordType.INSERT;
        if (operationContext!=null) {
            this.insertOperation = (InsertOperation) operationContext.getOperation();
        }
    }

    public void open() throws StandardException {
          open(insertOperation==null?null:insertOperation.getTriggerHandler(),insertOperation);
    }

    public void open(TriggerHandler triggerHandler, SpliceOperation operation) throws StandardException {
        super.open(triggerHandler, operation);
        try {
            WriteConfiguration writeConfiguration = writeCoordinator.defaultWriteConfiguration();
            if(insertOperation!=null && operationContext.isPermissive())
                    writeConfiguration = new PermissiveInsertWriteConfiguration(writeConfiguration,
                            operationContext,
                            execRowDefinition);

            writeConfiguration.setRecordingContext(operationContext);
            this.table =SIDriver.driver().getTableFactory().getTable(Long.toString(heapConglom));
            writeBuffer = writeCoordinator.writeBuffer(table,txn,writeConfiguration);
            if (insertOperation != null)
                insertOperation.tableWriter = this;
            flushCallback = triggerHandler == null ? null : TriggerHandler.flushCallback(writeBuffer);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    public void insert(ExecRow execRow) throws StandardException {
        try {
            if (operationContext!=null && operationContext.isFailed())
                return;
            beforeRow(execRow);
            Record record = SIDriver.driver().getOperationFactory().newRecord(txn,execRow.generateRowKey(pkCols));
            writeBuffer.add(record);
            TriggerHandler.fireAfterRowTriggers(triggerHandler, execRow, flushCallback);
            if (operationContext!=null)
                operationContext.recordWrite();
        } catch (Exception e) {
            if (operationContext!=null && operationContext.isPermissive()) {
                    operationContext.recordBadRecord(e.getLocalizedMessage() + execRow.toString(), e);
                return;
            }
            throw Exceptions.parseException(e);
        }
    }

    public void insert(Iterator<ExecRow> execRows) throws StandardException {
        while (execRows.hasNext())
            insert(execRows.next());
    }

    public void write(ExecRow execRow) throws StandardException {
        insert(execRow);
    }

    public void write(Iterator<ExecRow> execRows) throws StandardException {
        insert(execRows);
    }

    @Override
    public void close() throws StandardException{
        super.close();
        if(table!=null){
            try{
                table.close();
            }catch(IOException e){
                throw Exceptions.parseException(e);
            }
        }
    }

}