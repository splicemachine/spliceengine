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

import com.splicemachine.EngineDriver;
import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.DerbyAggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.AggregateContext;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Strings;

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
		protected CompilerContext.NativeSparkModeType nativeSparkMode =
		           CompilerContext.DEFAULT_SPLICE_NATIVE_SPARK_AGGREGATION_MODE;
		protected boolean nativeSparkUsed = false;

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
		                                  double optimizerEstimatedCost,
		                                  CompilerContext.NativeSparkModeType nativeSparkMode) throws StandardException {
				super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
				this.source = source;
				this.aggregateContext = new DerbyAggregateContext(ra==null? null:ra.getMethodName(),aggregateItem);
				if (nativeSparkMode == CompilerContext.NativeSparkModeType.SYSTEM) {
					this.nativeSparkMode =
					  EngineDriver.driver().getConfiguration().getNativeSparkAggregationMode();
					// If the system-level setting was never set up for whatever reason, pick
					// up the default setting here.
					if (this.nativeSparkMode == CompilerContext.NativeSparkModeType.SYSTEM)
						this.nativeSparkMode = SQLConfiguration.DEFAULT_NATIVE_SPARK_AGGREGATION_MODE_VALUE;
				}
				else
					this.nativeSparkMode = nativeSparkMode;
		}

		public boolean nativeSparkForced() {
			return nativeSparkMode == CompilerContext.NativeSparkModeType.FORCED;
		}
		public boolean nativeSparkEnabled() {
			return nativeSparkMode == CompilerContext.NativeSparkModeType.ON ||
			       nativeSparkMode == CompilerContext.NativeSparkModeType.FORCED;
		}
		public boolean nativeSparkUsed() { return nativeSparkUsed; }

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				SpliceLogUtils.trace(LOG,"readExternal");
				super.readExternal(in);
				this.nativeSparkMode = CompilerContext.NativeSparkModeType.values()[in.readInt()];
				this.aggregateContext = (AggregateContext)in.readObject();
				source = (SpliceOperation)in.readObject();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				SpliceLogUtils.trace(LOG,"writeExternal");
				super.writeExternal(out);
				out.writeInt(nativeSparkMode.ordinal());
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
		@SuppressFBWarnings(value = "NM_VERY_CONFUSING", justification = "DB-9844")
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
