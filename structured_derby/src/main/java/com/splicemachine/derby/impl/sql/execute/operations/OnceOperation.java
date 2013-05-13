package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
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
	protected GeneratedMethod emptyRowFun;
	protected String emptyRowFunMethodName;	
	private int cardinalityCheck;
	public int subqueryNumber;
	public int pointOfAttachment;

	private RowProvider dataProvider; // used for local calls to getNextRowCore()

	   public OnceOperation(NoPutResultSet s, Activation a, GeneratedMethod emptyRowFun,
				 int cardinalityCheck, int resultSetNumber,
				 int subqueryNumber, int pointOfAttachment,
				 double optimizerEstimatedRowCount,
				 double optimizerEstimatedCost) throws StandardException {
		   super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
			SpliceLogUtils.trace(LOG, "instantiated");
		   this.source = (SpliceOperation) s;
		   this.emptyRowFunMethodName = (emptyRowFun != null)?emptyRowFun.getMethodName():null;
		   this.cardinalityCheck = cardinalityCheck;
		   this.subqueryNumber = subqueryNumber;
		   this.pointOfAttachment = pointOfAttachment;
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
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG, "getNextRowCore");
		if(dataProvider == null)
			dataProvider = RowProviders.sourceProvider(source,LOG);

		return nextRow(dataProvider);
	}

	private ExecRow nextRow(RowProvider source) throws StandardException {
		ExecRow candidateRow = null;
		ExecRow secondRow = null;
		ExecRow result = null;
		if(!source.hasNext()) return null;
		candidateRow = source.next();
		if (candidateRow != null) {
			switch (cardinalityCheck) {
				case DO_CARDINALITY_CHECK:
				case NO_CARDINALITY_CHECK:
					candidateRow = candidateRow.getClone();
					if (cardinalityCheck == DO_CARDINALITY_CHECK) {
							/* Raise an error if the subquery returns > 1 row
							 * We need to make a copy of the current candidateRow since
							 * the getNextRow() for this check will wipe out the underlying
							 * row.
							 */
						if(source.hasNext()){
//							close();
							throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
						}
					}
					result = candidateRow;
					break;

				case UNIQUE_CARDINALITY_CHECK:
					candidateRow = candidateRow.getClone();
					DataValueDescriptor orderable1 = candidateRow.getColumn(1);
					while(source.hasNext()){
						secondRow = source.next();
						DataValueDescriptor orderable2 = secondRow.getColumn(1);
						if (! (orderable1.compare(DataValueDescriptor.ORDER_OP_EQUALS, orderable2, true, true))) {
							throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
						}
					}
					result = candidateRow;
					break;

				default:
					if (SanityManager.DEBUG)
					{
						SanityManager.THROWASSERT(
								"cardinalityCheck not unexpected to be " +
										cardinalityCheck);
					}
					break;
			}
		}
		else if (rowWithNulls == null)
		{
			rowWithNulls = (ExecRow) emptyRowFun.invoke(activation);
			result = rowWithNulls;
		}
		else
		{
			result = rowWithNulls;
		}
		setCurrentRow(result);
		return result;
	}

	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG, "executeScan");
		final List<SpliceOperation> operationStack =getOperationStack();
		SpliceLogUtils.trace(LOG, "operationStack=%s",operationStack);
		SpliceOperation regionOperation = operationStack.get(0);
		LOG.trace("regionOperation="+regionOperation);
		RowProvider provider;
		if (regionOperation.getNodeTypes().contains(NodeType.REDUCE) && this != regionOperation) {
			SpliceLogUtils.trace(LOG,"scanning Temp Table");
			provider = regionOperation.getReduceRowProvider(source,source.getExecRowDefinition());
		} else {
			SpliceLogUtils.trace(LOG,"scanning Map Table");
			provider = regionOperation.getMapRowProvider(source,source.getExecRowDefinition());
		}
		return new SpliceNoPutResultSet(activation,this, new RowProviders.DelegatingRowProvider(provider) {
			@Override
			public ExecRow next() throws StandardException {
					return nextRow(provider);
			}
		});
	}

	@Override
	public ExecRow getExecRowDefinition() throws StandardException {
		return source.getExecRowDefinition();
	}

	@Override
	public RowProvider getMapRowProvider(SpliceOperation top,ExecRow rowTemplate) throws StandardException{
		SpliceLogUtils.trace(LOG, "getMapRowProvider");
		return RowProviders.singletonProvider(getExecRowDefinition());
	}
	
	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow rowTemplate) throws StandardException{
		SpliceLogUtils.trace(LOG, "getReduceRowProvider");
		return RowProviders.singletonProvider(getExecRowDefinition());
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
	
	@Override
	public long getTimeSpent(int type)
	{
		long totTime = constructorTime + openTime + nextTime + closeTime;

		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		else
			return totTime;
	}

    @Override
    public void openCore() throws StandardException {
        super.openCore();
        if(source!=null)source.openCore();
    }
}
