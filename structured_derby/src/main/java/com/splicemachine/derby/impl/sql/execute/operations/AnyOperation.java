package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation.NodeType;
import com.splicemachine.utils.SpliceLogUtils;


/**
 * Takes a quantified predicate subquery's result set.
 * NOTE: A row with a single column containing null will be returned from
 * getNextRow() if the underlying subquery ResultSet is empty.
 *
 */
public class AnyOperation extends SpliceBaseOperation {
	private static Logger LOG = Logger.getLogger(AnyOperation.class);

	/* Used to cache row with nulls for case when subquery result set
	 * is empty.
	 */
	private ExecRow rowWithNulls;

	/* Used to cache the StatementContext */
	private StatementContext statementContext;

    // set in constructor and not altered during
    // life of object.
    public final SpliceOperation source;
	private GeneratedMethod emptyRowFun;
	public int subqueryNumber;
	public int pointOfAttachment;

    //
    // class interface
    //
    public AnyOperation(NoPutResultSet s, Activation a, GeneratedMethod emptyRowFun,
						int resultSetNumber, int subqueryNumber,
						int pointOfAttachment,
						double optimizerEstimatedRowCount,
						double optimizerEstimatedCost) throws StandardException {
		super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        source = (SpliceOperation) s;
		this.emptyRowFun = emptyRowFun;
		this.subqueryNumber = subqueryNumber;
		this.pointOfAttachment = pointOfAttachment;
		recordConstructorTime(); 
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//

	/**
     * open a scan on the table. scan parameters are evaluated
     * at each open, so there is probably some way of altering
     * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
     */
	public void	openCore() throws StandardException {
        source.openCore();
        super.openCore();
		if (statementContext == null) {
			statementContext = activation.getLanguageConnectionContext().getStatementContext();
		}
		statementContext.setSubqueryResultSet(subqueryNumber, this, activation.getNumSubqueries());
	}

	/**
	 * reopen a scan on the table. scan parameters are evaluated
	 * at each open, so there is probably some way of altering
	 * their values...
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	public void	reopenCore() throws StandardException {
        source.reopenCore();
	}

	public void	finish() throws StandardException {
		source.finish();
	}

	/**
     * Return the requested value computed from the next row.  
	 *
	 * @exception StandardException thrown on failure.
	 */
	public ExecRow	getNextRowCore() throws StandardException  {
	    ExecRow result = null;
	    ExecRow candidateRow = source.getNextRowCore();
		if (candidateRow != null) {
			result = candidateRow;
		}
		else if (rowWithNulls == null) {
			rowWithNulls = (ExecRow) emptyRowFun.invoke(activation);
			result = rowWithNulls;
		}
		else {
			result = rowWithNulls;
		}

		setCurrentRow(result);
	    return result;
	}

	/**
	 * If the result set has been opened,
	 * close the open scan.
 	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException {
		SpliceLogUtils.trace(LOG, "close in AnyOperation");
		beginTime = getCurrentTimeMillis();
		clearCurrentRow();
		source.close();
		super.close();
		closeTime += getElapsedMillis(beginTime);
	}

	/**
	 * @see NoPutResultSet#getPointOfAttachment
	 */
	public int getPointOfAttachment() {
		return pointOfAttachment;
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
    public boolean isReferencingTable(long tableNumber){
        return source.isReferencingTable(tableNumber);
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