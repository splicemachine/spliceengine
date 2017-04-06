/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.InsertPairFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import org.apache.log4j.Logger;
import java.io.IOException;

public class DeleteOperation extends DMLWriteOperation {
	private static final Logger LOG = Logger.getLogger(DeleteOperation.class);
	protected  boolean cascadeDelete;
    protected static final String NAME = DeleteOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

	public DeleteOperation(){
		super();
	}

	public DeleteOperation(SpliceOperation source, Activation activation,double optimizerEstimatedRowCount,
                           double optimizerEstimatedCost, String tableVersion) throws StandardException, IOException {
		super(source, activation,optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion);
        init();
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException, IOException {
		SpliceLogUtils.trace(LOG,"DeleteOperation init");
		super.init(context);
		heapConglom = writeInfo.getConglomerateId();
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
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet set = source.getDataSet(dsp);
        OperationContext operationContext = dsp.createOperationContext(this);
        TxnView txn = getCurrentTransaction();
		operationContext.pushScope();
        try {
            PairDataSet toWrite = set.index(new InsertPairFunction(operationContext),true);
			DataSetWriterBuilder dataSetWriterBuilder = toWrite.deleteData(operationContext)
                .destConglomerate(heapConglom)
                .operationContext(operationContext)
				.txn(txn);
			return dataSetWriterBuilder.build().write();
        } finally {
            operationContext.popScope();
        }
    }
}
