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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.client.SpliceClient;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class DeleteOperation extends DMLWriteOperation {
	private static final Logger LOG = Logger.getLogger(DeleteOperation.class);
	protected  boolean cascadeDelete;
    protected static final String NAME = DeleteOperation.class.getSimpleName().replaceAll("Operation","");
    protected String bulkDeleteDirectory;
    protected int colMapRefItem;
    protected int[] colMap;

	@Override
	public String getName() {
			return NAME;
	}

	public DeleteOperation(){
		super();
	}

	public DeleteOperation(SpliceOperation source, Activation activation,double optimizerEstimatedRowCount,
                           double optimizerEstimatedCost, String tableVersion,
						   String bulkDeleteDirectory, int colMapRefItem) throws StandardException, IOException {

        super(source, activation,optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion);
        this.bulkDeleteDirectory = bulkDeleteDirectory;
        this.colMapRefItem = colMapRefItem;
        init();
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException, IOException {
		SpliceLogUtils.trace(LOG,"DeleteOperation init");
		super.init(context);
		heapConglom = writeInfo.getConglomerateId();
        if (colMapRefItem >= 0) {
            colMap = (int[]) context.getPreparedStatement().getSavedObject(colMapRefItem);
        }
	}

    @Override
	public String toString() {
		return "Delete{destTable="+heapConglom+",source=" + source + "}";
	}

    @Override
    public String prettyPrint(int indentLevel) {
        return "Delete"+super.prettyPrint(indentLevel);
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");


        Pair<DataSet, int[]> pair = getBatchedDataset(dsp);
        DataSet set = pair.getFirst();
        int[] expectedUpdateCounts = pair.getSecond();
        OperationContext operationContext = dsp.createOperationContext(this);
        operationContext.pushScope();
        if (dsp.isSparkExplain()) {
            dsp.prependSpliceExplainString(this.explainPlan);
            return set;
        }
        TxnView txn = getTransactionForWrite(dsp);
        DataSetWriterBuilder dataSetWriterBuilder = null;
        try {
            // initTriggerRowHolders can't be called in the TriggerHandler constructor
            // because it has to be called after getCurrentTransaction() elevates the
            // transaction to writable.
            if (triggerHandler != null)
                triggerHandler.initTriggerRowHolders(isOlapServer(), txn, SpliceClient.token, 0);

            if (bulkDeleteDirectory != null) {
                dataSetWriterBuilder = set
                        .bulkDeleteData(operationContext)
                        .bulkDeleteDirectory(bulkDeleteDirectory)
                        .colMap(colMap);
            }
            if (dataSetWriterBuilder == null) {
                dataSetWriterBuilder = set.deleteData(operationContext);
            }
            DataSetWriter dataSetWriter = dataSetWriterBuilder
                    .updateCounts(expectedUpdateCounts)
                    .destConglomerate(heapConglom)
                    .tempConglomerateID(getTriggerConglomID())
                    .operationContext(operationContext)
                    .tableVersion(tableVersion)
                    .execRowDefinition(getExecRowDefinition())
                    .txn(txn).build();
            return dataSetWriter.write();

        }
        catch (Exception e) {
            exceptionHit = true;
            throw Exceptions.parseException(e);
        }
        finally {
            finalizeNestedTransaction();
            operationContext.popScope();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        writeNullableString(bulkDeleteDirectory,out);
        out.writeInt(colMapRefItem);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException{
        super.readExternal(in);
        bulkDeleteDirectory = readNullableString(in);
        colMapRefItem = in.readInt();
    }
}
