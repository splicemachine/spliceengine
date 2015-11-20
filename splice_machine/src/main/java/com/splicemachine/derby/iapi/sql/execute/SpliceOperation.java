package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.OperationInformation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;

/**
 * 
 * Interface for Parallel Operations in the Splice Machine.
 *
 *
 */

public interface SpliceOperation extends StandardCloseable, NoPutResultSet, ConvertedResultSet, CursorResultSet {

    RowLocation getCurrentRowLocation();

    void setCurrentRowLocation(RowLocation rowLocation);

    void setCurrentLocatedRow(LocatedRow locatedRow);

    public <Op extends SpliceOperation> DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException;

    public OperationContext getOperationContext();

    public void setOperationContext(OperationContext operationContext);

    /**
     * @return a descriptive name for this operation. Used for reporting information.
     */
    String getName();

    OperationInformation getOperationInformation();

    public int modifiedRowCount();

    public Activation getActivation();

    public void clearCurrentRow();

    public void markAsTopResultSet();

    public void open() throws StandardException;

    public int resultSetNumber();

    public void setCurrentRow(ExecRow row);

	/**
	 * Initializes the node with the statement and the language context from the SpliceEngine.
	 * 
	 * @throws StandardException
	 */
	public void init(SpliceOperationContext context) throws IOException, StandardException;

	/**
	 * Unique node sequence id.  Should move from Zookeeper to uuid generator.
	 * 
	 */
	public byte[] getUniqueSequenceID();

	/**
	 * 
	 * Gets the left Operation for a Operation.  They can be named different things in different operations (Source, LeftResultSet, etc.).  
	 * This gives a simple method to retrieve that operation.  This needs to be implemented in each operation.
	 * 
	 * @return
	 */
	public SpliceOperation getLeftOperation();
	/**
	 * 
	 * Recursively generates the left operation stack.  This method is implemented properly as long as you inherit from
	 * the SpliceBaseOperation.
	 * 
	 * @return
	 */
	public void generateLeftOperationStack(List<SpliceOperation> operations);

	/**
	 * 
	 * Gets the right Operation for a Operation.  They can be named different things in different operations (Source, LeftResultSet, etc.).  
	 * This gives a simple method to retrieve that operation.  This needs to be implemented in each operation.
	 * 
	 * @return
	 */
	public SpliceOperation getRightOperation();
	/**
	 * 
	 * Recursively generates the left operation stack.  This method is implemented properly as long as you inherit from
	 * the SpliceBaseOperation.
	 * 
	 * @return
	 */
	public void generateRightOperationStack(boolean initial, List<SpliceOperation> operations);

    public void generateAllOperationStack(List<SpliceOperation> operations);
	/**
	 * 
	 * The outgoing field definition of the record.  Do we need incoming as well?
	 * 
	 * @return
	 * @throws StandardException 
	 */

	public ExecRow getExecRowDefinition() throws StandardException;

    /**
     * Returns an array containing integer pointers into the columns of the table. This
     * array relates the compact row column location (integer pointer) to the original
     * location (the nth column on the original table).
     *
     *
     * @param tableNumber
     * @return
     */
    int[] getRootAccessedCols(long tableNumber) throws StandardException;

    /**
     * Returns true if this operation references the given table number.  For a join,
     * this means either the left side or the right side involves that table.  For things
     * like table scans, it's true if it is scanning that table.
     */
    boolean isReferencingTable(long tableNumber);

    /**
     * Prints out a string representation of this operation, formatted for easy human consumption.
     *
     * @return a pretty-printed string representation of this operation.
     */
    String prettyPrint(int indentLevel);
    /**
     * @return -1l if no statementId has been set on this operation, or the statement
     * id if one has. Generally, a statementId is only set on the top operation
     */
    long getStatementId();

    void setStatementId(long statementId);

    int[] getAccessedNonPkColumns() throws StandardException;
		
	void setActivation(Activation activation) throws StandardException;

    String getInfo();

    double getEstimatedCost();

    double getEstimatedRowCount();

    public TxnView getCurrentTransaction() throws StandardException;

    public List<SpliceOperation> getSubOperations();

    Iterator<LocatedRow> getLocatedRowIterator();

    public void openCore(DataSetProcessor dsp) throws StandardException;

    public void registerCloseable(AutoCloseable closeable) throws StandardException;

    public void fireBeforeStatementTriggers () throws StandardException;

    public void fireAfterStatementTriggers () throws StandardException;

    public TriggerHandler getTriggerHandler() throws StandardException;

    public ExecIndexRow getStartPosition() throws StandardException;

    public String getVTIFileName();

    public TxnView createChildTransaction(byte[] table) throws StandardException;

    public void rollbackTransaction(long txnId) throws StandardException;

    public void commitTransaction(long txnId) throws StandardException;

}
