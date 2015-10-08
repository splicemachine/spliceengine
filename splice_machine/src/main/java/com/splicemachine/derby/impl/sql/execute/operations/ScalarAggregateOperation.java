package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.scalar.ScalarAggregator;
import com.splicemachine.derby.stream.function.ScalarAggregateFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
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

        public static long serialVersionUID = 1l;
		private static Logger LOG = Logger.getLogger(ScalarAggregateOperation.class);
        protected boolean isInSortedOrder;
		protected boolean singleInputRow;
		protected boolean isOpen=false;
        boolean returnDefault = true;
        private ScalarAggregator scanAggregator;
		private ScalarAggregator sinkAggregator;

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
																		double optimizerEstimatedCost) throws StandardException  {
				super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				this.isInSortedOrder = isInSortedOrder;
				this.singleInputRow = singleInputRow;
				recordConstructorTime();
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
				SpliceUtils.populateDefaultValues(row.getRowArray(),0);
				return row;
		}

    @Override
    public String toString() {
        return String.format("ScalarAggregateOperation {resultSetNumber=%d, source=%s}", resultSetNumber, source);
    }

		public boolean isSingleInputRow() {
				return this.singleInputRow;
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "Scalar"+super.prettyPrint(indentLevel);
		}

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        LocatedRow result = source.getDataSet(dsp)
                .fold(null,new ScalarAggregateFunction(dsp.createOperationContext(this)));
        if (result==null) {
			return returnDefault ?
					dsp.singleRowDataSet(new LocatedRow(getExecRowDefinition())) : null;
		}
        if (!isInitialized(result.getRow())) {
            initializeVectorAggregation(result.getRow());
        }
        finishAggregation(result.getRow());
        LocatedRow lr = new LocatedRow(result.getRowLocation(),result.getRow());
        setCurrentLocatedRow(lr);
        return dsp.singleRowDataSet(lr);
    }

}