/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
    /**
     *
     * Retrieve the current Row Location (Cursor Concept) on the operation.
     *
     * @return
     */
    RowLocation getCurrentRowLocation();

    /**
     *
     * Set the current Row Location (Cursor Concept) on the operation.
     *
     * @param rowLocation
     */
    void setCurrentRowLocation(RowLocation rowLocation);

    /**
     *
     * Set the current Located Row.
     *
     * @param locatedRow
     */
    void setCurrentLocatedRow(LocatedRow locatedRow);

    /**
     *
     * Get the dataset abstraction for the operation.
     *
     * @see DataSet
     *
     * @param dsp
     * @return
     * @throws StandardException
     */
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

    /**
     *
     * Retrieve the operation context.  This context provides startup context for the different execution
     * engines.
     *
     * @see OperationContext
     *
     * @return
     */
    OperationContext getOperationContext();

    /**
     *
     * Sets the operationContext on this operation.
     *
     * @param operationContext
     */
    void setOperationContext(OperationContext operationContext);

    /**
     *
     * Hack to allow Splice Machine to modify the spark API
     * Should this be moved or handled better?
     *
     * @return
     */
    String getScopeName();

    /**
     * Retrieve a well formatted explain plan.
     *
     * @return
     */
    String getPrettyExplainPlan();

    /**
     *
     * Sets the explain plan value when the statement starts with "explain <statement>"
     *
     * @param plan
     */
    void setExplainPlan(String plan);
    
    /**
     * @return a descriptive name for this operation. Used for reporting information.
     */
    String getName();

    /**
     *
     * Aggregating all the operation information into a class.
     *
     * @return
     */
    OperationInformation getOperationInformation();

    /**
     *
     * Number of rows modified.  This can be used via the resultset on returning inserts, updates,
     * deletes.
     *
     * @return
     */
    int modifiedRowCount();

    /**
     *
     * Return the activation for the operation.
     *
     * @return
     */
    Activation getActivation();

    /**
     *
     * Clear the current row in the operation.
     * Usually also clears the row in the activation.
     *
     */
    void clearCurrentRow();

    /**
     *
     * This is the result set that is returning to the sql user.
     *
     */
    void markAsTopResultSet();

    /**
     *
     * Open the operation.
     *
     * @throws StandardException
     */
    void open() throws StandardException;

    /**
     *
     * The generated resultSetNumber (0-n for an activation).
     *
     * @return
     */
    int resultSetNumber();
    /**
     *
     * Set the current row of data.  Important if projections, etc. act directly by
     * reference on other rows.
     *
     * @param row the new current row
     */
    void setCurrentRow(ExecRow row);

	/**
	 * Initializes the node with the statement and the language context from the SpliceEngine.
	 * 
	 * @throws StandardException
	 */
    void init(SpliceOperationContext context) throws IOException, StandardException;

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

    /**
     *
     * Grabbing the accessedNonPKColumns
     *
     * @return
     * @throws StandardException
     */
    int[] getAccessedNonPkColumns() throws StandardException;

    /**
     *
     * Set the activation on the operation.
     *
      * @param activation
     * @throws StandardException
     */
	void setActivation(Activation activation) throws StandardException;

    /**
     *
     * Returns the optimizers estimated cost for the operation.  Not currently
     * used inside the operation stack.
     *
     * @return
     */
    double getEstimatedCost();

    /**
     *
     * Retrieve the estimatedRowCount.  There are optimizations where this
     * can be used for cache buffers, etc.
     *
     * @return
     */
    double getEstimatedRowCount();

    /**
     *
     * Retrieve the current transaction for the operation.
     *
     * @return
     * @throws StandardException
     */
    TxnView getCurrentTransaction() throws StandardException;

    /**
     * Retrieve the sub operations for this operation.  This is mostly used
     * in explain plans and VTI.
     *
     * @return
     */
    List<SpliceOperation> getSubOperations();

    /**
     *
     * Retrieve an iterator on the locatedRow.
     *
     * @return
     */
    Iterator<LocatedRow> getLocatedRowIterator();

    /**
     *
     * Open Core on the operation.
     *
     * @param dsp
     * @throws StandardException
     */
    void openCore(DataSetProcessor dsp) throws StandardException;

    /**
     *
     * Register a resource to be closed when the activation is closed.
     *
     * @param closeable
     * @throws StandardException
     */
    void registerCloseable(AutoCloseable closeable) throws StandardException;

    /**
     *
     * Fire Before Statement Triggers on this operation.
     *
     * @throws StandardException
     */
    void fireBeforeStatementTriggers() throws StandardException;
    /**
     *
     * Fire After Statement Triggers on this operation.
     *
     * @throws StandardException
     */
    void fireAfterStatementTriggers() throws StandardException;
    /**
     *
     * Retrieve the trigger handler for this operation.
     *
     * @throws StandardException
     */
    TriggerHandler getTriggerHandler() throws StandardException;

    /**
     *
     * Retrieve the start position for this operation.
     *
     * @return
     * @throws StandardException
     */
    ExecIndexRow getStartPosition() throws StandardException;

    /**
     *
     * Return the VTI file name for this operation.
     *
     * @return
     */
    String getVTIFileName();
}
