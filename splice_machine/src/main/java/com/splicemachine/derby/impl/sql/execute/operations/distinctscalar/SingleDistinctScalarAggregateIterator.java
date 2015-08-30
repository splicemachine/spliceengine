package com.splicemachine.derby.impl.sql.execute.operations.distinctscalar;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractStandardIterator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.EmptyRowSupplier;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.StandardIterator;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class SingleDistinctScalarAggregateIterator extends AbstractStandardIterator {
		private ExecRow currentRow;
		private EmptyRowSupplier emptyRowSupplier;
		private SpliceGenericAggregator[] aggregates;
		private WarningCollector warningCollector;
		private long rowsRead;
        private DataValueDescriptor currentValue;


		public SingleDistinctScalarAggregateIterator(StandardIterator<ExecRow> source, EmptyRowSupplier emptyRowSupplier, WarningCollector warningCollector, SpliceGenericAggregator[] aggregates) {
				super(source);
				this.emptyRowSupplier = emptyRowSupplier;
				this.aggregates = aggregates;
				this.warningCollector = warningCollector;
		}

		public void merge(ExecRow newRow) throws StandardException{
                if (currentValue == null) {
                    if (currentRow != null) {
                        currentValue = aggregates[0].getInputColumnValue(currentRow).cloneValue(false);
                    }
                }
                DataValueDescriptor newValue = aggregates[0].getInputColumnValue(newRow);
                if (currentValue == null || currentValue.compare(newValue) != 0) {
                    for (SpliceGenericAggregator aggregator : aggregates) {
                        initialize(newRow);
                        aggregator.merge(newRow, currentRow);
                    }
                    currentValue = newValue.cloneValue(false);
                }
		}

		public void initialize(ExecRow newRow) throws StandardException{
				for(SpliceGenericAggregator aggregator:aggregates) {
						aggregator.initializeAndAccumulateIfNeeded(newRow,newRow);
				}
		}

		@Override
		public GroupedRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				ExecRow nextRow = source.next(spliceRuntimeContext);
				if (nextRow == null) {// Return Default Values...
						currentRow = emptyRowSupplier.get();
						finish();
						return new GroupedRow(currentRow,new byte[0]);
				}
				currentRow = nextRow.getClone();
				initialize(currentRow);
				boolean shouldContinue = true;
				do{
                        rowsRead++;
						SpliceBaseOperation.checkInterrupt(rowsRead, SpliceConstants.interruptLoopCheck);
						nextRow = source.next(spliceRuntimeContext);
						shouldContinue = nextRow!=null;
						if(!shouldContinue)
								continue; //iterator exhausted, break from the loop
						merge(nextRow);
				}while(shouldContinue);
				finish();

				return new GroupedRow(currentRow,new byte[0]);
		}

		public long getRowsRead(){
				return rowsRead;
		}

		public void finish() throws StandardException{
				if(currentRow==null)
						currentRow = emptyRowSupplier.get();
				boolean eliminatedNulls = false;
				for(SpliceGenericAggregator aggregate:aggregates){
						if(aggregate.finish(currentRow))
								eliminatedNulls=true;
				}
//				if(eliminatedNulls)
//						warningCollector.addWarning(SQLState.LANG_NULL_ELIMINATED_IN_SET_FUNCTION);
		}
}
