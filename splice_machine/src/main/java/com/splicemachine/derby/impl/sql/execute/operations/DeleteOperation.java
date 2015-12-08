package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.InsertPairFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.delete.DeleteTableWriterBuilder;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;

import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 
 *
 */
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
                           double optimizerEstimatedCost, String tableVersion) throws StandardException {
		super(source, activation,optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion);
		recordConstructorTime();
	}

	public DeleteOperation(SpliceOperation source, ConstantAction passedInConstantAction, Activation activation,double optimizerEstimatedRowCount,
                           double optimizerEstimatedCost, String tableVersion) throws StandardException {
		super(source, activation,optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion);
		recordConstructorTime();
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

    @Override
    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet set = source.getDataSet(dsp);
        OperationContext operationContext = dsp.createOperationContext(this);
        TxnView txn = getCurrentTransaction();
        DeleteTableWriterBuilder builder = new DeleteTableWriterBuilder()
                .heapConglom(heapConglom)
                .txn(txn)
                .operationContext(operationContext);
        try {
            operationContext.pushScope();
            return set.index(new InsertPairFunction(operationContext), true).deleteData(builder, operationContext);
        } finally {
            operationContext.popScope();
        }
    }
}
