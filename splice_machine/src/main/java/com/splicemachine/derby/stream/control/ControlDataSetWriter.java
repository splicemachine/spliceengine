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

package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.AbstractPipelineWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class ControlDataSetWriter<K> implements DataSetWriter, AutoCloseable{
    private final ControlDataSet<ExecRow> dataSet;
    private final OperationContext operationContext;
    private final AbstractPipelineWriter<ExecRow> pipelineWriter;
    private final int[] updateCounts;
    private static final Logger LOG = Logger.getLogger(ControlDataSetWriter.class);
    private Txn     txn = null;
    private TxnView parent = null;

    public ControlDataSetWriter(ControlDataSet<ExecRow> dataSet, AbstractPipelineWriter<ExecRow> pipelineWriter, OperationContext opContext, int[] updateCounts){
        this.dataSet=dataSet;
        this.operationContext=opContext;
        this.pipelineWriter=pipelineWriter;
        this.updateCounts = updateCounts;
    }

    @Override
    public DataSet<ExecRow> write() throws StandardException{
        SpliceOperation operation=operationContext.getOperation();

        try{
            boolean inMemoryTxn = !operation.isOlapServer();
            parent = getTxn();
            txn = SIDriver.driver().lifecycleManager().beginChildTransaction(
                    parent,
                    parent.getIsolationLevel(),
                    parent.isAdditive(),
                    pipelineWriter.getDestinationTable(),
                    inMemoryTxn);
            pipelineWriter.setTxn(txn);
            operation.fireBeforeStatementTriggers();
            pipelineWriter.open(operation.getTriggerHandler(),operation);
            pipelineWriter.write(dataSet.toLocalIterator());

            if (txn.getState() != Txn.State.COMMITTED)
                txn.commit();

            pipelineWriter.firePendingAfterTriggers();

            // Defer closing the pipeline in case we need to rollback.
            if (operation.getActivation().isSubStatement())
                pipelineWriter.close();
            else {
                operation.registerCloseable(pipelineWriter);
            }

            // Only fire the statement triggers if above operations did not
            // cause a rollback (throw an exception).
            // Previously this was in a 'finally' block, and was executed
            // even when we did rollback, which is not right.
            operation.fireAfterStatementTriggers();
            if (!operation.getActivation().isSubStatement())
                pipelineWriter.close();

            long recordsWritten = operationContext.getRecordsWritten();
            operationContext.getActivation().getLanguageConnectionContext().setRecordsImported(operationContext.getRecordsWritten());
            long badRecords = 0;
            if(operation instanceof InsertOperation){
                InsertOperation insertOperation = (InsertOperation)operation;
                BadRecordsRecorder brr = operationContext.getBadRecordsRecorder();
                /*
                 * In Control-side execution, we have different operation contexts for each operation,
                 * and all operations are held in this JVM. this means that parse errors could be present
                 * at the context for the lower operation (i.e. in an import), so we need to collect those errors
                 * directly.
                 */
                List<SpliceOperation> ops =insertOperation.getOperationStack();
                for(SpliceOperation op:ops){
                    if(op==null || op==insertOperation || op.getOperationContext()==null) continue;
                    if (brr != null) {
                        brr = brr.merge(op.getOperationContext().getBadRecordsRecorder());
                    } else {
                        brr = op.getOperationContext().getBadRecordsRecorder();
                    }
                }
                badRecords = (brr != null ? brr.getNumberOfBadRecords() : 0);
                operationContext.getActivation().getLanguageConnectionContext().setFailedRecords(badRecords);
                if(badRecords > 0){
                    String fileName = operationContext.getStatusDirectory();
                    operationContext.getActivation().getLanguageConnectionContext().setBadFile(fileName);
                    if (insertOperation.isAboveFailThreshold(badRecords)) {
                        throw ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException(fileName);
                    }
                }
            }
            if (updateCounts != null) {
                long total = 0;
                List<ExecRow> rows = new ArrayList<>();
                for (int count : updateCounts) {
                    total += count;
                    ValueRow valueRow = new ValueRow(1);
                    valueRow.setColumn(1, new SQLLongint(count));
                    rows.add(valueRow);
                }
                assert total == recordsWritten;
                return new ControlDataSet<>(rows.iterator());
            }
            ValueRow valueRow=null;
            if (badRecords > 0) {
                valueRow = new ValueRow(3);
                valueRow.setColumn(2, new SQLLongint(badRecords));
                valueRow.setColumn(3, new SQLVarchar(operationContext.getStatusDirectory()));
            }
            else {
                valueRow = new ValueRow(1);
            }
            valueRow.setColumn(1,new SQLLongint(recordsWritten));

            return new ControlDataSet<>(new SingletonIterator(valueRow));
       }catch(Exception e){
            if(txn!=null){
                try{
                    txn.rollback();
                }catch(IOException e1){
                    e.addSuppressed(e1);
                }
            }
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void setTxn(TxnView childTxn){
        pipelineWriter.setTxn(childTxn);
    }

    @Override
    public TxnView getTxn(){
        return pipelineWriter.getTxn();
    }

    @Override
    public byte[] getDestinationTable(){
        return pipelineWriter.getDestinationTable();
    }

    @Override
    public void close() throws Exception {
        if (txn != null && txn.getState() != Txn.State.COMMITTED &&
                           txn.getState() != Txn.State.ROLLEDBACK) {
            txn.commit();
        }
    }
}
