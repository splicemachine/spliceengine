package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.utils.SpliceLogUtils;

public class OnceOperation extends SpliceBaseOperation {
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
	    ExecRow candidateRow = null;
		ExecRow secondRow = null;
	    ExecRow result = null;
		candidateRow = source.getNextRowCore();
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
							secondRow = source.getNextRowCore();
							if (secondRow != null) {
								close();
								StandardException se = StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
								throw se;
							}
						}
						result = candidateRow;
						break;

					case UNIQUE_CARDINALITY_CHECK:
						candidateRow = candidateRow.getClone();
						secondRow = source.getNextRowCore();
						DataValueDescriptor orderable1 = candidateRow.getColumn(1);
						while (secondRow != null) {
							DataValueDescriptor orderable2 = secondRow.getColumn(1);
							if (! (orderable1.compare(DataValueDescriptor.ORDER_OP_EQUALS, orderable2, true, true))) {
								close();
								StandardException se = StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
								throw se;
							}
							secondRow = source.getNextRowCore();
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
	public RowProvider getMapRowProvider(SpliceOperation top,ExecRow rowTemplate){
		SpliceLogUtils.trace(LOG, "getMapRowProvider");
		return RowProviders.singletonProvider(getExecRowDefinition());
	}
	
	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,ExecRow rowTemplate){
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


}
