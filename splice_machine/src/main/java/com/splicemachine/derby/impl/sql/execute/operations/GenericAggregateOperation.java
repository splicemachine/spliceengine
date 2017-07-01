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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.DerbyAggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.AggregateContext;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.sparkproject.guava.base.Strings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public abstract class GenericAggregateOperation extends SpliceBaseOperation {
		private static final long serialVersionUID = 1l;
		private static Logger LOG = Logger.getLogger(GenericAggregateOperation.class);
		protected SpliceOperation source;
		protected AggregateContext aggregateContext;
		public SpliceGenericAggregator[] aggregates;
		protected ExecIndexRow sourceExecIndexRow;
		protected ExecIndexRow sortTemplateRow;

		public GenericAggregateOperation () {
				super();
				SpliceLogUtils.trace(LOG, "instantiated");
		}

		public GenericAggregateOperation (SpliceOperation source,
																			int	aggregateItem,
																			Activation activation,
																			GeneratedMethod	ra,
																			int resultSetNumber,
																			double optimizerEstimatedRowCount,
																			double optimizerEstimatedCost) throws StandardException {
				super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
				this.source = source;
				this.aggregateContext = new DerbyAggregateContext(ra==null? null:ra.getMethodName(),aggregateItem);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				SpliceLogUtils.trace(LOG,"readExternal");
				super.readExternal(in);
				this.aggregateContext = (AggregateContext)in.readObject();
				source = (SpliceOperation)in.readObject();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				SpliceLogUtils.trace(LOG,"writeExternal");
				super.writeExternal(out);
				out.writeObject(aggregateContext);
				out.writeObject(source);
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				SpliceLogUtils.trace(LOG, "getSubOperations");
				List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
				operations.add(source);
				return operations;
		}


		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				SpliceLogUtils.trace(LOG, "init called");
				super.init(context);
				if(source!=null)
						source.init(context);
				aggregateContext.init(context);
				aggregates = aggregateContext.getAggregators();
				sortTemplateRow = aggregateContext.getSortTemplateRow();
				sourceExecIndexRow = aggregateContext.getSourceIndexRow();
		}

		@Override
		public SpliceOperation getLeftOperation() {
				if (LOG.isTraceEnabled())
						LOG.trace("getLeftOperation");
				return this.source;
		}

		//	@Override
		public void cleanup() {
				if (LOG.isTraceEnabled())
						LOG.trace("cleanup");
		}

		public SpliceOperation getSource() {
				return this.source;
		}

    @Override
    public String toString() {
        return String.format("GenericAggregateOperation {resultSetNumber=%d, source=%s}", resultSetNumber, source);
    }

		@Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n"+ Strings.repeat("\t",indentLevel);

				return "Aggregate:" + indent +
								"resultSetNumber:" + operationInformation.getResultSetNumber() +
								"optimizerEstimatedCost:" + optimizerEstimatedCost + 
								"optimizerEstimatedRowCount:" + optimizerEstimatedRowCount + 								
								indent +
								"source:" + source.prettyPrint(indentLevel + 1);
		}

		@Override
		public int[] getRootAccessedCols(long tableNumber) throws StandardException {
				if(source.isReferencingTable(tableNumber))
						return source.getRootAccessedCols(tableNumber);

				return null;
		}

		@Override
		public boolean isReferencingTable(long tableNumber) {
				return source.isReferencingTable(tableNumber);
		}

    public void initializeVectorAggregation(ExecRow aggResult) throws StandardException{
        for(SpliceGenericAggregator aggregator:aggregates){
            aggregator.initialize(aggResult);
            aggregator.accumulate(aggResult,aggResult);
        }
    }

    public boolean isInitialized(ExecRow aggResult) throws StandardException{
        for(SpliceGenericAggregator aggregator:aggregates){
            if (!aggregator.isInitialized(aggResult))
                return false;
        }
        return true;
    }

    public void finishAggregation(ExecRow row) throws StandardException {
        for(SpliceGenericAggregator aggregator:aggregates){
            aggregator.finish(row);
        }
        this.setCurrentRow(row);
    }

	public ExecIndexRow getSourceExecIndexRow() {
		return sourceExecIndexRow;
	}

}
