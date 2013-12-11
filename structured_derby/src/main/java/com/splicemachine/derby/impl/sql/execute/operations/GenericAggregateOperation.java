package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.SQLWarningFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.sql.execute.AggregatorInfoList;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG, "init called");
		super.init(context);
		source.init(context);
        aggregateContext.init(context);
        aggregates = aggregateContext.getAggregators();
        sortTemplateRow = aggregateContext.getSortTemplateRow();
        sourceExecIndexRow = aggregateContext.getSourceIndexRow();
	}

    protected final ExecRow finishAggregation(ExecRow row) throws StandardException {
		SpliceLogUtils.trace(LOG, "finishAggregation");

		/*
		** If the row in which we are to place the aggregate
		** result is null, then we have an empty input set.
		** So we'll have to create our own row and set it
		** up.  Note: we needn't initialize in this case,
		** finish() will take care of it for us.
		*/ 
		if (row == null) {
			row = this.getActivation().getExecutionFactory().getIndexableRow(rowAllocator.invoke());
		}
		setCurrentRow(row);
		boolean eliminatedNulls = false;
        for (SpliceGenericAggregator currAggregate : aggregates) {
            if (currAggregate.finish(row))
                eliminatedNulls = true;
        }

        /*
		if (eliminatedNulls)
			addWarning(SQLWarningFactory.newSQLWarning(SQLState.LANG_NULL_ELIMINATED_IN_SET_FUNCTION));
	    */
	
		return row;
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
		return getRegionStats() == null ? 0l : getRegionStats().getTotalProcessedRecords();
	}
	
	public long getRowsOutput() {
		return getRegionStats() == null ? 0l : getRegionStats().getTotalSunkRecords();
	}

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        if(source!=null)source.open();
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);

        return "Aggregate:" + indent +
                "resultSetNumber:" + operationInformation.getResultSetNumber() + indent +
                "source:" + source.prettyPrint(indentLevel + 1);
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
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
}
