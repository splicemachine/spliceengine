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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.InsertPairFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.si.api.txn.Txn;
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
        Txn txn = getCurrentTransaction();
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
