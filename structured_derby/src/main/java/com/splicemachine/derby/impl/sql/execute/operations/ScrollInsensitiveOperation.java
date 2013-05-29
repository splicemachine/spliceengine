package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;

public class ScrollInsensitiveOperation extends SpliceBaseOperation {
	protected int sourceRowWidth;
	protected NoPutResultSet source;
	protected boolean scrollable;
	private static Logger LOG = Logger.getLogger(ScrollInsensitiveOperation.class);
	protected static List<NodeType> nodeTypes; 
	static {
		nodeTypes = new ArrayList<NodeType>();
		nodeTypes.add(NodeType.SCROLL);
		nodeTypes.add(NodeType.MAP);		
	}
    public ScrollInsensitiveOperation () {
    	super();
    }
    public ScrollInsensitiveOperation(NoPutResultSet source,
			  Activation activation, int resultSetNumber,
			  int sourceRowWidth,
			  boolean scrollable,
			  double optimizerEstimatedRowCount,
			  double optimizerEstimatedCost) throws StandardException {
		super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
		this.sourceRowWidth = sourceRowWidth;
		this.source = source;
		this.scrollable = scrollable;
		recordConstructorTime(); 
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		if (LOG.isTraceEnabled())
			LOG.trace("readExternal");
		super.readExternal(in);
		sourceRowWidth = in.readInt();
		scrollable = in.readBoolean();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		if (LOG.isTraceEnabled())
			LOG.trace("writeExternal");
		super.writeExternal(out);
		out.writeInt(sourceRowWidth);
		out.writeBoolean(scrollable);
	}
	
	@Override
	public List<SpliceOperation> getSubOperations() {
		if (LOG.isTraceEnabled())
			LOG.trace("getSubOperations");
		List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
		operations.add((SpliceOperation) source);
		return operations;
	}
	
	@Override
	public SpliceOperation getLeftOperation() {
		if (LOG.isTraceEnabled())
			LOG.trace("getLeftOperation");
		return (SpliceOperation) source;
	}

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        return ((SpliceOperation)source).getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return ((SpliceOperation)source).isReferencingTable(tableNumber);
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "ScrollInsensitive"; //this class is never used
    }

    @Override
	public List<NodeType> getNodeTypes() {
		return nodeTypes;
	}
	@Override
	public ExecRow getNextRowCore() throws StandardException {
		throw new RuntimeException("Not Implemented");
	}

	public NoPutResultSet getSource() {
		return this.source;
	}
	@Override
	public long getTimeSpent(int type)
	{
		long totTime = constructorTime + openTime + nextTime + closeTime;

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		else
			return totTime;
	}
}
