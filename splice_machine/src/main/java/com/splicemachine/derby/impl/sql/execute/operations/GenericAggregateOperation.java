package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Strings;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.sql.execute.operations.framework.DerbyAggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class GenericAggregateOperation extends SpliceBaseOperation implements SinkingOperation {
		private static final long serialVersionUID = 1l;
		private static Logger LOG = Logger.getLogger(GenericAggregateOperation.class);
		protected SpliceOperation source;
		protected AggregateContext aggregateContext;
		protected SpliceGenericAggregator[] aggregates;
		protected SpliceMethod<ExecRow> rowAllocator;
		protected ExecIndexRow sourceExecIndexRow;
		protected ExecIndexRow sortTemplateRow;
		protected static List<NodeType> nodeTypes;
		protected Scan reduceScan;
		protected boolean serializeSource = true;

		protected long rowsInput;

		static {
				nodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SINK);
		}
		public GenericAggregateOperation () {
				super();
				SpliceLogUtils.trace(LOG, "instantiated");
		}

		public GenericAggregateOperation(SpliceOperation source,
																		 OperationInformation baseOpInformation,
																		 AggregateContext aggregateContext) throws StandardException{
				super(baseOpInformation);
				this.source = source;
				this.aggregateContext = aggregateContext;
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

		public GenericAggregateOperation (SpliceOperation source,
																			Activation activation,
																			int resultSetNumber,
																			double optimizerEstimatedRowCount,
																			double optimizerEstimatedCost,
																			AggregateContext context) throws StandardException {
				super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
				this.source = source;
				this.aggregateContext = context;
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				SpliceLogUtils.trace(LOG,"readExternal");
				super.readExternal(in);
				this.aggregateContext = (AggregateContext)in.readObject();
				if(in.readBoolean())
						source = (SpliceOperation)in.readObject();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				SpliceLogUtils.trace(LOG,"writeExternal");
				super.writeExternal(out);
				out.writeObject(aggregateContext);
				out.writeBoolean(serializeSource);
				if(serializeSource)
						out.writeObject(source);
		}
		@Override
		public List<NodeType> getNodeTypes() {
				SpliceLogUtils.trace(LOG, "getNodeTypes");
				return nodeTypes;
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

		public long getRowsInput() {
        return 0l; //TODO -sf- implement
		}

		public long getRowsOutput() {
        return 0l; //TODO -sf- implement
		}

		@Override
		public void open() throws StandardException, IOException {
				super.open();
				if(source!=null)source.open();
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

		@Override
		public byte[] getUniqueSequenceId() {
				return uniqueSequenceID;
		}

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        // TODO: subclasses also do this - check if redundant
        if (source != null) source.close();
    }

	@Override
	public String getOptimizerOverrides(SpliceRuntimeContext ctx){
		return source.getOptimizerOverrides(ctx);
	}

	protected void initializeVectorAggregation(ExecRow aggResult) throws StandardException{
        for(SpliceGenericAggregator aggregator:aggregates){
            aggregator.initialize(aggResult);
            aggregator.accumulate(aggResult,aggResult);
        }
    }

    protected boolean isInitialized(ExecRow aggResult) throws StandardException{
        for(SpliceGenericAggregator aggregator:aggregates){
            if (!aggregator.isInitialized(aggResult))
                return false;
        }
        return true;
    }

    protected void finishAggregation(ExecRow row) throws StandardException {
        for(SpliceGenericAggregator aggregator:aggregates){
            aggregator.finish(row);
        }
    }
}
