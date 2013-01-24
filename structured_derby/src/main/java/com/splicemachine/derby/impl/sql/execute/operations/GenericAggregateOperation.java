package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import org.apache.derby.iapi.error.SQLWarningFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.impl.sql.execute.AggregatorInfo;
import org.apache.derby.impl.sql.execute.AggregatorInfoList;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class GenericAggregateOperation extends SpliceBaseOperation {
	private static Logger LOG = Logger.getLogger(GenericAggregateOperation.class);
	protected NoPutResultSet source;
	protected String rowAllocatorMethodName;
	protected int aggregateItem;
	protected SpliceGenericAggregator[] aggregates;	
	protected GeneratedMethod rowAllocator;
	protected AggregatorInfoList aggInfoList;
	protected HTableInterface tempTable;
	
	protected ExecIndexRow sourceExecIndexRow;
	protected ExecIndexRow sortTemplateRow;

	protected static List<NodeType> nodeTypes; 
	protected Scan reduceScan;
	
	static {
		nodeTypes = new ArrayList<NodeType>();
//		nodeTypes.add(NodeType.MAP);
		nodeTypes.add(NodeType.REDUCE);
		nodeTypes.add(NodeType.SINK);
	}
    public GenericAggregateOperation () {
    	super();
    	SpliceLogUtils.trace(LOG, "instantiated");
    }
    public GenericAggregateOperation (NoPutResultSet source,
		int	aggregateItem,
		Activation activation,
		GeneratedMethod	ra,
		int resultSetNumber,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost) throws StandardException {
    	super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
    	SpliceLogUtils.trace(LOG, "instantiated with parameters");
    	SpliceLogUtils.trace(LOG, "source="+source);
    	this.source = source;
    	this.rowAllocator = ra;
    	this.rowAllocatorMethodName = (ra == null) ? null : ra.getMethodName();
    	SpliceLogUtils.trace(LOG,"RowAllocatorMethodNameXX "+rowAllocatorMethodName);
    	this.aggregateItem = aggregateItem;
	}
    
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		SpliceLogUtils.trace(LOG,"readExternal");
		super.readExternal(in);
		rowAllocatorMethodName = readNullableString(in);	
		aggregateItem = in.readInt();
		source = (SpliceOperation)in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		SpliceLogUtils.trace(LOG,"writeExternal");
		super.writeExternal(out);
		writeNullableString(rowAllocatorMethodName, out);		
		out.writeInt(aggregateItem);
		out.writeObject((SpliceOperation)source);
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
		operations.add((SpliceOperation) source);
		return operations;
	}


	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG, "init called");
		super.init(context);
		((SpliceOperation)source).init(context);
		try {
            GenericStorablePreparedStatement statement = context.getPreparedStatement();
            LanguageConnectionContext lcc = context.getLanguageConnectionContext();
			rowAllocator = (rowAllocatorMethodName == null) ? null : statement.getActivationClass().getMethod(rowAllocatorMethodName);
			aggInfoList = (AggregatorInfoList) (statement.getSavedObject(aggregateItem));
			aggregates = getSortAggregators(aggInfoList, false, lcc);
//			tempTable = SpliceAccessManager.getHTable(SpliceOperationCoprocessor.TEMP_TABLE);
			ExecutionFactory factory = activation.getExecutionFactory();
			sortTemplateRow = factory.getIndexableRow((ExecRow)rowAllocator.invoke(activation));
			sourceExecIndexRow = factory.getIndexableRow(sortTemplateRow);
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		}			
	}
	
	protected final SpliceGenericAggregator[] getSortAggregators (
		AggregatorInfoList 			list,
		boolean 					eliminateDistincts,
		LanguageConnectionContext	lcc) throws StandardException {
		SpliceLogUtils.trace(LOG,"getSortAggregators");
		SpliceGenericAggregator[] 	aggregators; 
		Vector<SpliceGenericAggregator> tmpAggregators = new Vector<SpliceGenericAggregator>();
		ClassFactory		cf = lcc.getLanguageConnectionFactory().getClassFactory();
		int count = list.size();
		for (int i = 0; i < count; i++) {
			AggregatorInfo aggInfo = (AggregatorInfo) list.elementAt(i);
			if (! (eliminateDistincts && aggInfo.isDistinct())){
			// if (eliminateDistincts == aggInfo.isDistinct()) 
				tmpAggregators.addElement(new SpliceGenericAggregator(aggInfo, cf));
			}
		}
		aggregators = new SpliceGenericAggregator[tmpAggregators.size()];
		tmpAggregators.copyInto(aggregators);
		return aggregators;
	}
	
	private List<SpliceOperation> getOperations(){
		SpliceLogUtils.trace(LOG, "getOperations");
		List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
		generateLeftOperationStack(operations);
		return operations;
	}	
		
	/**
	 * Finish the aggregation for the current row.  
	 * Basically call finish() on each aggregator on
	 * this row.  Called once per grouping on a vector
	 * aggregate or once per table on a scalar aggregate.
	 *
	 * If the input row is null, then rowAllocator is
	 * invoked to create a new row.  That row is then
	 * initialized and used for the output of the aggregation.
	 *
	 * @param 	row	the row to finish aggregation
	 *
	 * @return	the result row.  If the input row != null, then
	 *	the result row == input row
	 *
	 * @exception StandardException Thrown on error
	 */
	protected final ExecIndexRow finishAggregation(ExecIndexRow row) throws StandardException {
		SpliceLogUtils.trace(LOG, "finishAggregation");
		int	size = aggregates.length;

		/*
		** If the row in which we are to place the aggregate
		** result is null, then we have an empty input set.
		** So we'll have to create our own row and set it
		** up.  Note: we needn't initialize in this case,
		** finish() will take care of it for us.
		*/ 
		if (row == null) {
			row = this.getActivation().getExecutionFactory().getIndexableRow((ExecRow) rowAllocator.invoke(activation));
		}
		setCurrentRow(row);
		boolean eliminatedNulls = false;
		for (int i = 0; i < size; i++) {
			SpliceGenericAggregator currAggregate = aggregates[i];
			if (currAggregate.finish(row))
				eliminatedNulls = true;
		}

		if (eliminatedNulls)
			addWarning(SQLWarningFactory.newSQLWarning(SQLState.LANG_NULL_ELIMINATED_IN_SET_FUNCTION));
	
		return row;
	}

	public void finish() throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("finish");
		source.finish();
		super.finish();
	}
	@Override
	public SpliceOperation getLeftOperation() {
		if (LOG.isTraceEnabled())
			LOG.trace("getLeftOperation");
		return (SpliceOperation) this.source;
	}

	@Override
	public void cleanup() {
		if (LOG.isTraceEnabled())
			LOG.trace("cleanup");
		try {
			tempTable.close();
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Error closing Temp Table",e);
		}
	}
}
