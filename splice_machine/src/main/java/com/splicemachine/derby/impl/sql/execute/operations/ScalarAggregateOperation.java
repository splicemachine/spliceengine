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

import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.ScalarAggregateFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Operation for performing Scalar Aggregations (sum, avg, max/min, etc.). 
 *
 * @author Scott Fines
 */
public class ScalarAggregateOperation extends GenericAggregateOperation {

    public static final long serialVersionUID = 1l;
    private static Logger LOG = Logger.getLogger(ScalarAggregateOperation.class);
    protected boolean isInSortedOrder;
    protected boolean singleInputRow;
    protected static final String NAME = ScalarAggregateOperation.class.getSimpleName().replaceAll("Operation","");
    
    @Override
    public String getName() {
		return NAME;
    }
    
    
    public ScalarAggregateOperation () {
		super();
    }
    
    public ScalarAggregateOperation(SpliceOperation s,
                                    boolean isInSortedOrder,
                                    int	aggregateItem,
                                    Activation a,
                                    GeneratedMethod ra,
                                    int resultSetNumber,
                                    boolean singleInputRow,
                                    double optimizerEstimatedRowCount,
                                    double optimizerEstimatedCost,
                                    CompilerContext.NativeSparkModeType nativeSparkMode) throws StandardException  {
		super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost, nativeSparkMode);
		this.isInSortedOrder = isInSortedOrder;
		this.singleInputRow = singleInputRow;
    }
    
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		isInSortedOrder = in.readBoolean();
		singleInputRow = in.readBoolean();
    }
    
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeBoolean(isInSortedOrder);
		out.writeBoolean(singleInputRow);
    }
    
    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
		super.init(context);
		source.init(context);
		try {
			sortTemplateRow = this.aggregateContext.getSortTemplateRow();
			sourceExecIndexRow = this.aggregateContext.getSourceIndexRow();
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
    }
    
    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG,"getExecRowDefinition");
		ExecRow row = sourceExecIndexRow.getClone();
		// Set the default values to 0 in case a ProjectRestrictOperation has set the default values to 1.
		// That is done to avoid division by zero exceptions when executing a projection for defining the rows
		// before execution.
		EngineUtils.populateDefaultValues(row.getRowArray(),0);
		return row;
    }

    @Override
    public String toString() {
        return String.format("ScalarAggregateOperation {resultSetNumber=%d, source=%s}", resultSetNumber, source);
    }

	@Override
	public String prettyPrint(int indentLevel) {
		return "Scalar"+super.prettyPrint(indentLevel);
	}

    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        OperationContext<ScalarAggregateOperation> operationContext = dsp.createOperationContext(this);
        dsp.incrementOpDepth();
        DataSet<ExecRow> dsSource = source.getDataSet(dsp);
        dsp.decrementOpDepth();
        DataSet<ExecRow> dataSetWithNativeSparkAggregation = null;

        if (nativeSparkForced())
            dsSource = dsSource.upgradeToSparkNativeDataSet(operationContext);

        // If the aggregation can be applied using native Spark UnsafeRow, then do so
        // and return immediately.  Otherwise, use traditional Splice lower-level
        // functional APIs.
        if (nativeSparkEnabled())
            dataSetWithNativeSparkAggregation =
                dsSource.applyNativeSparkAggregation(null, aggregates,
                                                     false, operationContext);
        if (dataSetWithNativeSparkAggregation != null) {
            dsp.finalizeTempOperationStrings();
            nativeSparkUsed = true;
            return dataSetWithNativeSparkAggregation;
        }
        DataSet<ExecRow> ds = dsSource.mapPartitions(new ScalarAggregateFlatMapFunction(operationContext, false), false, /*pushScope=*/true, "First Aggregation");
        DataSet<ExecRow> ds2 = ds.coalesce(1, /*shuffle=*/true, /*isLast=*/false, operationContext, /*pushScope=*/true, "Coalesce");
        handleSparkExplain(ds2, dsSource, dsp);
        return ds2.mapPartitions(new ScalarAggregateFlatMapFunction(operationContext, true), /*isLast=*/true, /*pushScope=*/true, "Final Aggregation");
    }
}
