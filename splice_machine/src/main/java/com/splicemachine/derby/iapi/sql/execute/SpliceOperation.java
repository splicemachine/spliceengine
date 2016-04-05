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
import com.splicemachine.si.api.txn.TxnView;

/**
 * Interface for Parallel Operations in the Splice Machine.
 */
public interface SpliceOperation extends StandardCloseable, NoPutResultSet, ConvertedResultSet, CursorResultSet {

    RowLocation getCurrentRowLocation();

    void setCurrentRowLocation(RowLocation rowLocation);

    void setCurrentLocatedRow(LocatedRow locatedRow);

    DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException;

    /**
     * Dataset that's going to be consumed right away rather than operated on, transformed, joined... It might have some
     * optimizations like implementing LIMIT on the control side
     *
     * @param dsp
     * @return Dataset ready to be consumed
     * @throws StandardException
     */
    DataSet<LocatedRow> getResultDataSet(DataSetProcessor dsp) throws StandardException;

    OperationContext getOperationContext();

    void setOperationContext(OperationContext operationContext);

    String getScopeName();
    
    String getPrettyExplainPlan();
    
    void setExplainPlan(String plan);
    
    /**
     * @return a descriptive name for this operation. Used for reporting information.
     */
    String getName();

    OperationInformation getOperationInformation();

    int modifiedRowCount();

    Activation getActivation();

    void clearCurrentRow();

    void markAsTopResultSet();

    void open() throws StandardException;

    int resultSetNumber();

    void setCurrentRow(ExecRow row);

	/**
	 * Initializes the node with the statement and the language context from the SpliceEngine.
	 * 
	 * @throws StandardException
	 */
    void init(SpliceOperationContext context) throws IOException, StandardException;

	/**
	 * Unique node sequence id.  Should move from Zookeeper to uuid generator.
	 * 
	 */
    byte[] getUniqueSequenceID();

	/**
	 * 
	 * Gets the left Operation for a Operation.  They can be named different things in different operations (Source, LeftResultSet, etc.).  
	 * This gives a simple method to retrieve that operation.  This needs to be implemented in each operation.
	 *
	 * @return
	 */
    SpliceOperation getLeftOperation();
	/**
	 *
	 * Recursively generates the left operation stack.  This method is implemented properly as long as you inherit from
	 * the SpliceBaseOperation.
	 *
	 * @return
	 */
    void generateLeftOperationStack(List<SpliceOperation> operations);

	/**
	 * 
	 * Gets the right Operation for a Operation.  They can be named different things in different operations (Source, LeftResultSet, etc.).  
	 * This gives a simple method to retrieve that operation.  This needs to be implemented in each operation.
	 * 
	 * @return
	 */
    SpliceOperation getRightOperation();
	/**
	 * 
	 * Recursively generates the left operation stack.  This method is implemented properly as long as you inherit from
	 * the SpliceBaseOperation.
	 * 
	 * @return
	 */
    void generateRightOperationStack(boolean initial,List<SpliceOperation> operations);

	/**
	 * 
	 * The outgoing field definition of the record.  Do we need incoming as well?
	 * 
	 * @return
	 * @throws StandardException 
	 */

    ExecRow getExecRowDefinition() throws StandardException;

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

    int[] getAccessedNonPkColumns() throws StandardException;
		
	void setActivation(Activation activation) throws StandardException;

    double getEstimatedCost();

    double getEstimatedRowCount();

    TxnView getCurrentTransaction() throws StandardException;

    List<SpliceOperation> getSubOperations();

    Iterator<LocatedRow> getLocatedRowIterator();

    void openCore(DataSetProcessor dsp) throws StandardException;

    void registerCloseable(AutoCloseable closeable) throws StandardException;

    void fireBeforeStatementTriggers() throws StandardException;

    void fireAfterStatementTriggers() throws StandardException;

    TriggerHandler getTriggerHandler() throws StandardException;

    ExecIndexRow getStartPosition() throws StandardException;

    String getVTIFileName();
}
