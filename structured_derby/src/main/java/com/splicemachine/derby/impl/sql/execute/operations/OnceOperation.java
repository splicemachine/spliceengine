package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OnceOperation extends SpliceBaseOperation {
	private static final long serialversionUID = 1l;
	private static Logger LOG = Logger.getLogger(OnceOperation.class);
	public static final int DO_CARDINALITY_CHECK		= 1;
	public static final int NO_CARDINALITY_CHECK		= 2;
	public static final int UNIQUE_CARDINALITY_CHECK	= 3;
	private ExecRow rowWithNulls;

	/* Used to cache the StatementContext */
	private StatementContext statementContext;

    // set in constructor and not altered during
    // life of object.
    public SpliceOperation source;
	protected SpliceMethod<ExecRow> emptyRowFun;
	protected String emptyRowFunMethodName;	
	private int cardinalityCheck;
	public int subqueryNumber;
	public int pointOfAttachment;

	private RowProvider dataProvider; // used for local calls to nextRow()

    @Deprecated
    public OnceOperation(){}

	   public OnceOperation(SpliceOperation s, Activation a, GeneratedMethod emptyRowFun,
				 int cardinalityCheck, int resultSetNumber,
				 int subqueryNumber, int pointOfAttachment,
				 double optimizerEstimatedRowCount,
				 double optimizerEstimatedCost) throws StandardException {
		   super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
			SpliceLogUtils.trace(LOG, "instantiated");
		   this.source = s;
		   this.emptyRowFunMethodName = (emptyRowFun != null)?emptyRowFun.getMethodName():null;
		   this.cardinalityCheck = cardinalityCheck;
		   this.subqueryNumber = subqueryNumber;
		   this.pointOfAttachment = pointOfAttachment;
           init(SpliceOperationContext.newContext(a));
		   recordConstructorTime(); 
	   }
	   
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			SpliceLogUtils.trace(LOG, "readExternal");
			super.readExternal(in);
			source = (SpliceOperation) in.readObject();
			emptyRowFunMethodName = readNullableString(in);
			cardinalityCheck = in.readInt();
			subqueryNumber = in.readInt();
			pointOfAttachment = in.readInt();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			SpliceLogUtils.trace(LOG, "writeExternal");
			super.writeExternal(out);
			out.writeObject(source);
			writeNullableString(emptyRowFunMethodName, out);
			out.writeInt(cardinalityCheck);
			out.writeInt(subqueryNumber);
			out.writeInt(pointOfAttachment);
		}
		
		@Override
		public SpliceOperation getLeftOperation() {
			SpliceLogUtils.trace(LOG,"getLeftOperation");
			return source;
		}

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
        source.init(context);

        if(emptyRowFun == null) {
            emptyRowFun = new SpliceMethod<ExecRow>(emptyRowFunMethodName, activation);
        }
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        //do our cardinality checking
        ExecRow rowToReturn = null;

        ExecRow nextRow = source.nextRow(spliceRuntimeContext);
        if(nextRow!=null){
            switch (cardinalityCheck) {
                case DO_CARDINALITY_CHECK:
                case NO_CARDINALITY_CHECK:
                    nextRow = nextRow.getClone();
                    if (cardinalityCheck == DO_CARDINALITY_CHECK) {
                    /* Raise an error if the subquery returns > 1 row
                     * We need to make a copy of the current candidateRow since
                     * the getNextRow() for this check will wipe out the underlying
                     * row.
                     */
                        ExecRow secondRow = source.nextRow(spliceRuntimeContext);
                        if(secondRow!=null){
                            close();
                            throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
                        }
                    }
                    rowToReturn = nextRow;
                    break;
                case UNIQUE_CARDINALITY_CHECK:
                    //TODO -sf- I don't think that this will work unless there's a sort order on the first column..
                    nextRow = nextRow.getClone();
                    DataValueDescriptor orderable1 = nextRow.getColumn(1);
                    ExecRow secondRow = source.nextRow(spliceRuntimeContext);
                    while(secondRow!=null){
                        DataValueDescriptor orderable2 = secondRow.getColumn(1);
                        if (! (orderable1.compare(DataValueDescriptor.ORDER_OP_EQUALS, orderable2, true, true))) {
                            close();
                            throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
                        }
                        secondRow = source.nextRow(spliceRuntimeContext);
                    }
                    rowToReturn = nextRow;
                    break;
                default:
                    if (SanityManager.DEBUG) {
                        SanityManager.THROWASSERT(
                                "cardinalityCheck not unexpected to be " +
                                        cardinalityCheck);
                    }
                    break;
            }
        }
        //do null-filling on the other side of the serialization barrier
        setCurrentRow(rowToReturn);
        return rowToReturn;
	}

    @Override
    public void close() throws StandardException {
        if(dataProvider!=null)
            dataProvider.close();
        dataProvider = null;
    }

	@Override
	public NoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
		SpliceLogUtils.trace(LOG, "executeScan");
		final List<SpliceOperation> operationStack =getOperationStack();
		SpliceLogUtils.trace(LOG, "operationStack=%s",operationStack);
		SpliceOperation regionOperation = operationStack.get(0);
		SpliceLogUtils.trace(LOG,"regionOperation=%s",regionOperation);
		RowProvider provider = getReduceRowProvider(this, OperationUtils.getPairDecoder(this,runtimeContext),runtimeContext);
		return new SpliceNoPutResultSet(activation,this, provider);
	}

	@Override
	public ExecRow getExecRowDefinition() throws StandardException {
		return source.getExecRowDefinition();
	}

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    @Override
	public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException{
		SpliceLogUtils.trace(LOG, "getMapRowProvider");
        return source.getMapRowProvider(top,rowDecoder,spliceRuntimeContext);
	}
	
	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException{
		SpliceLogUtils.trace(LOG, "getReduceRowProvider");
        return new OnceRowProvider(source.getReduceRowProvider(top,rowDecoder, spliceRuntimeContext));
	}

    @Override
	public List<NodeType> getNodeTypes() {
		SpliceLogUtils.trace(LOG, "getNodeTypes");
		return Collections.singletonList(NodeType.SCAN);
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		SpliceLogUtils.trace(LOG, "getSubOperations");
		List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
		operations.add(source);
		return operations;
	}
	
//	@Override
//	public long getTimeSpent(int type)
//	{
//		long totTime = constructorTime + openTime + nextTime + closeTime;
//
//		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
//			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
//		else
//			return totTime;
//	}


    @Override
    public void open() throws StandardException, IOException {
        super.open();
        if(source!=null)source.open();
    }

    private class OnceRowProvider implements RowProvider {
        private final RowProvider delegate;
        private ExecRow rowToReturn;

        public OnceRowProvider(RowProvider delegate) {
            this.delegate = delegate;
        }

        @Override public void open() throws StandardException { 
        	delegate.open(); 
        }
        @Override public void close() { delegate.close(); }
        @Override public RowLocation getCurrentRowLocation() { return delegate.getCurrentRowLocation(); }
        @Override public byte[] getTableName() { return delegate.getTableName(); }
        @Override public int getModifiedRowCount() { return delegate.getModifiedRowCount(); }

        @Override
        public JobStats shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
            return delegate.shuffleRows(instructions);
        }

        @Override
        public boolean hasNext() throws StandardException, IOException {
            rowToReturn = delegate.hasNext() ? delegate.next() : getRowWithNulls();
            // OnceOp always has another row…
            return true;
        }

        @Override
        public ExecRow next() throws StandardException {
            return rowToReturn;
        }

		@Override
		public SpliceRuntimeContext getSpliceRuntimeContext() {
			return delegate.getSpliceRuntimeContext();
		}

        public ExecRow getRowWithNulls() throws StandardException {
            if (rowWithNulls == null){
                rowWithNulls = emptyRowFun.invoke();
            }
            return rowWithNulls;
        }
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);

        return new StringBuilder("Once:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("emptyRowFunName:").append(emptyRowFunMethodName)
                .append(indent).append("cardinalityCheck:").append(cardinalityCheck)
                .append(indent).append("subqueryNumber:").append(subqueryNumber)
                .append(indent).append("pointOfAttachment:").append(pointOfAttachment)
                .append(indent).append("source:").append(source.prettyPrint(indentLevel+1))
                .toString();
    }
}
