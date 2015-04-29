package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.spark.RDDUtils;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.scalar.ScalarAggregator;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.derby.stream.DataSetProcessor;
import com.splicemachine.derby.stream.OperationContext;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.IndexValueRow;
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
        LocatedRow result = source.getDataSet()
                .fold(null,new ScalarAggregatorFunction(dsp.createOperationContext(this)));
        if (result==null)
            return dsp.singleRowDataSet(new LocatedRow(getExecRowDefinition()));
        if (!(result.getRow() instanceof ExecIndexRow)) {
            sourceExecIndexRow.execRowToExecIndexRow(result.getRow());
            initializeVectorAggregation(result.getRow());
        }
        finishAggregation(result.getRow());
        return dsp.singleRowDataSet(new LocatedRow(result.getRow()));
    }

    private static final class ScalarAggregatorFunction extends SpliceFunction2<SpliceOperation, LocatedRow, LocatedRow, LocatedRow> {
        private static final long serialVersionUID = -4150499166764796082L;

        public ScalarAggregatorFunction() {
        }

        public ScalarAggregatorFunction(OperationContext<SpliceOperation> operationContext) {
            super(operationContext);
        }

        @Override
        public LocatedRow call(LocatedRow t1, LocatedRow t2) throws Exception {
            operationContext.recordRead();
            ScalarAggregateOperation op = (ScalarAggregateOperation) getOperation();
            if (t2 == null) {
                return t1;
            }
            if (t1 == null) {
                return t2;
            }
            if (RDDUtils.LOG.isDebugEnabled()) {
                RDDUtils.LOG.debug(String.format("Reducing %s and %s", t1, t2));
            }
            ExecRow r1 = t1.getRow();
            if (!(r1 instanceof ExecIndexRow)) {
                r1 = new IndexValueRow(r1);
            }
            if (!op.isInitialized(r1)) {
                op.initializeVectorAggregation(r1);
            }
            aggregate(t2.getRow(), (ExecIndexRow) r1);
            return new LocatedRow(r1);
        }

        private void aggregate(ExecRow next, ExecIndexRow agg) throws StandardException {
            ScalarAggregateOperation op = (ScalarAggregateOperation) getOperation();
            if (next instanceof ExecIndexRow) {
                if (RDDUtils.LOG.isDebugEnabled()) {
                    RDDUtils.LOG.debug(String.format("Merging %s with %s", next, agg));
                }
                for (SpliceGenericAggregator aggregate : op.aggregates) {
                    aggregate.merge(next, agg);
                }
            } else {
                next = new IndexValueRow(next);
                if (RDDUtils.LOG.isDebugEnabled()) {
                    RDDUtils.LOG.debug(String.format("Aggregating %s to %s", next, agg));
                }
                for (SpliceGenericAggregator aggregate : op.aggregates) {
                    aggregate.accumulate(next, agg);
                }
            }
        }

    }


}