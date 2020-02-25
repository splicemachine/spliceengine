/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.SQLSessionContext;
import com.splicemachine.db.iapi.sql.depend.Dependent;

import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;

import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.TemporaryRowHolder;

import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.TransactionController;

import com.splicemachine.db.iapi.types.DataValueFactory;

import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.utils.Pair;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;
import java.util.Hashtable;


/**
 * An activation contains all the local state information necessary
 * to execute a re-entrant PreparedStatement. The way it will actually work
 * is that a PreparedStatement will have an executable plan, which will be
 * a generated class. All of the local state will be variables in the class.
 * Creating a new instance of the executable plan will create the local state
 * variables. This means that an executable plan must implement this interface,
 * and that the PreparedStatement.getActivation() method will do a
 * "new" operation on the executable plan.
 * <p>
 * The fixed implementations of Activation in the Execution impl
 * package are used as skeletons for the classes generated for statements
 * when they are compiled.
 * <p>
 * There are no fixed implementations of Activation for statements;
 * a statement has an activation generated for it when it is compiled.
 *
 */

public interface Activation extends Dependent
{
    /**
     * Resets the activation to the "pre-execution" state -
     * that is, the state where it can be used to begin a new execution.
     * Frees local buffers, stops scans, resets counters to zero, sets
     * current date and time to an unitialized state, etc.
     *
     * @exception StandardException thrown on failure
     */
    void    reset() throws StandardException;

    /**
     * JDBC requires that all select statements be converted into cursors,
     * and that the cursor name be settable for each execution of a select
     * statement. The Language Module will support this, so that the JDBC
     * driver will not have to parse JSQL text. This method will have no
     * effect when called on non-select statements.
     * <p>
     * There will be a JSQL statement to disable the "cursorization" of
     * all select statements. For non-cursorized select statements, this
     * method will have no effect.
     * <p>
     * This has no effect if the activation has been closed.
     * <p>
     * @param cursorName  The cursor name to use.
     */
    void    setCursorName(String cursorName);

    /**
     * Temporary tables can be declared with ON COMMIT DELETE ROWS. But if the table has a held curosr open at
     * commit time, data should not be deleted from the table. This method, (gets called at commit time) checks if this
     * activation held cursor and if so, does that cursor reference the passed temp table name.
     *
     * @return    true if this activation has held cursor and if it references the passed temp table name
     */
    boolean checkIfThisActivationHasHoldCursor(String tableName);

    /**
     * Gets the ParameterValueSet for this execution of the statement.
     *
     * @return    The ParameterValueSet for this execution of the
     *        statement. Returns NULL if there are no parameters.
     */
    ParameterValueSet    getParameterValueSet();

    /**
     * Sets the parameter values for this execution of the statement.
     * <p>
     * Has no effect if the activation has been closed.
     *
     * <p>
     * NOTE: The setParameters() method is currently unimplemented.
     * A statement with parameters will generate its own ParameterValueSet,
     * which can be gotten with the getParameterValueSet() method (above).
     * The idea behind setParameters() is to improve performance when
     * operating across a network by allowing all the parameters to be set
     * in one call, as opposed to one call per parameter.
     *
     * @param parameterValues    The values of the parameters.
     */
    void    setParameters(ParameterValueSet parameterValues, DataTypeDescriptor[] parameterTypes) throws StandardException;

    /**
     * When the prepared statement is executed, it passes
     * execution on to the activation execution was requested for.
     *
     * @return the ResultSet for further manipulation, if any.
     *
     * @exception StandardException        Thrown on failure
     */
    ResultSet execute() throws StandardException;

    /**
        Closing an activation statement marks it as unusable. Any other
        requests made on it will fail.  An activation should be
        marked closed when it is expected to not be used any longer,
        i.e. when the connection for it is closed, or it has suffered some
        sort of severe error. This will also close its result set and
        release any resources it holds e.g. for parameters.
        <P>
        Any class that implements this must be prepared to be executed
        from garbage collection, ie. there is no matching context stack.

        @exception StandardException        Thrown on failure
     */
    void close() throws StandardException;

    /**
        Find out if the activation is closed or not.

        @return true if the Activation has been closed.
     */
    boolean isClosed();

    /**
        Set this Activation for a single execution.
        E.g. a java.sql.Statement execution.
    */
    void setSingleExecution();

    /**
        Returns true if this Activation is only going to be used for
        one execution.
    */
    boolean isSingleExecution();

    /**
      Returns the chained list of warnings. Returns null
      if there are no warnings.
      */
    SQLWarning getWarnings();

    /**
      Add a warning to the activation
      */
    void addWarning(SQLWarning w);

    /**
      Clear the activation's warnings.
      */
    void clearWarnings();

    /**
     * Get the language connection context associated with this activation
     */
    LanguageConnectionContext    getLanguageConnectionContext();

    /**
     * Get the Execution TransactionController associated with this
     * activation/lcc.
     */
    TransactionController getTransactionController();

    /**
     * Returns the current result set for this activation, i.e.
     * the one returned by the last execute() call.  If there has
     * been no execute call or the activation has been reset or closed,
     * a null is returned.
     *
     * @return the current ResultSet of this activation.
     */
    ResultSet getResultSet();

    /**
     * Generated plans have a current row field for ease in defining
     * the methods and finding them dynamically. The interface is
     * used to set the row before a dynamic method that uses it is
     * invoked.
     * <p>
     * When all processing on the currentRow has been completed,
     * callers should call activation.clearCurrentRow(resultSetNumber)
     * to ensure that no unnecessary references are retained to rows.
     * This will allow the rows no longer in use to be collected by
     * the garbage collecter.
     *
     * @param currentRow        The row to be operated upon.
     * @param resultSetNumber    The resultSetNumber for the current ResultSet
     */
    void setCurrentRow(ExecRow currentRow, int resultSetNumber);

    /**
     * Get the current row at the given index.
     */
    Row getCurrentRow(int resultSetNumber);
    
    /**
     * Generated plans have a current row field for ease in defining
     * the methods and finding them dynamically. The interface is
     * used to set the row before a dynamic method that uses it is
     * invoked.
     * <p>
     * When all processing on the currentRow has been completed,
     * callers should call activation.clearCurrentRow(resultSetNumber)
     * to ensure that no unnecessary references are retained to rows.
     * This will allow the rows no longer in use to be collected by
     * the garbage collecter.
     *
     * @param resultSetNumber    The resultSetNumber for the current ResultSet
     */
    /* RESOLVE - this method belongs on an internal, not external, interface */
    void clearCurrentRow(int resultSetNumber);

    /**
     * Get the prepared statement that this activation is for.
     *
     * @return the prepared statement this activation is for.
     *
     */
    ExecPreparedStatement getPreparedStatement();

    /**
        Check the validity of the current executing statement. Needs to be
        called after a statement has obtained the relevant table locks on
        the
    */
    void checkStatementValidity() throws StandardException;

    /**
     * Get the result description for this activation, if it has one.
     *
     * @return result description for this activation, if it has one;
     * otherwise, null.
     */
    ResultDescription getResultDescription();

    /**
     * Get the DataValueFactory
     *
     * @return DataValueFactory
     */
    DataValueFactory getDataValueFactory();

    /**
     * Get the ExecutionFactory
     *
     * @return ExecutionFactory
     */
    ExecutionFactory getExecutionFactory();

    /**
        Get the saved RowLocation.

        @param itemNumber    The saved item number.

        @return    A RowLocation template for the conglomerate
     */
    RowLocation getRowLocationTemplate(int itemNumber);

    /**
        Get the number of subqueries in the entire query.
        @return int     The number of subqueries in the entire query.
     */
    int getNumSubqueries();

    /**
     * Return the cursor name of this activation. This will differ
     * from its ResultSet's cursor name if it has been
     * altered with setCursorName. Thus this always returns the cursor
     * name of the next execution of this activation. The cursor name
     * of the current execution must be obtained from the ResultSet.
     * or this.getResultSet.getCursorName() [with null checking].
     * <p>
     * Statements that do not support cursors will return a null.
     * <p>
     * @return The cursor name.
     */
    String    getCursorName();

    /**
     * Return the holdability of this activation.
     * <p>
     * @return The holdability of this activation.
     */
    boolean    getResultSetHoldability();

    /**
     * Set current resultset holdability.
     *
     * @param resultSetHoldability    The new resultset holdability.
     */
    void setResultSetHoldability(boolean resultSetHoldability);

    /**
     * Set the auto-generated keys resultset mode to true for this activation.
     *
     * The specific columns for auto-generated keys resultset can be requested by
     * passing column positions array
     *
     * The specific columns for auto-generated keys resultset can be requested by
     * passing column names array
     *
     * Both the parameters would be null if user didn't request specific keys.
     * Otherwise, the user could request specific columns by passing column positions
     * or names array but not both.
     *
     * @param columnIndexes Request specific columns in auto-generated keys
     * resultset by passing column positions. null means no specific columns
     * requested by position
     *
     * @param columnNames Request specific columns in auto-generated keys
     * resultset by passing column names.  null means no specific columns
     * requested by position
     */
    void setAutoGeneratedKeysResultsetInfo(int[] columnIndexes, String[] columnNames);

    /**
     * Returns true if auto-generated keys resultset request was made for this
     * avtivation.
     * <p>
     * @return auto-generated keys resultset mode for this activation.
     */
    boolean    getAutoGeneratedKeysResultsetMode();

    /**
     * Returns the column positions array of columns requested in auto-generated
     * keys resultset for this avtivation. Returns null if no specific column
     * requested by positions
     * <p>
     * @return column positions array of columns requested.
     */
    int[] getAutoGeneratedKeysColumnIndexes();

    /**
     * Returns the column names array of columns requested in auto-generated
     * keys resultset for this avtivation. Returns null if no specific column
     * requested by names
     * <p>
     * @return column names array of columns requested.
     */
    String[] getAutoGeneratedKeysColumnNames();

    /**
     * Mark the activation as unused.
     */
    void markUnused();

    /**
     * Is the activation in use?
     *
     * @return true/false
     */
    boolean isInUse();

    /**
     * Get the ConglomerateController, if any, that has already
     * been opened for the heap when scaning for an update or delete.
     * (Saves opening the ConglomerateController twice.)
     *
     * @return The ConglomerateController, if available, to use for the update.
     */
    ConglomerateController getHeapConglomerateController();

    /**
     * Set the ConglomerateController to be used for an update or delete.
     * (Saves opening the ConglomerateController twice.)
     *
     * @param updateHeapCC    The ConglomerateController to reuse for the update or delete.
     */
    void setHeapConglomerateController(ConglomerateController updateHeapCC);

    /**
     * Clear the ConglomerateController to be used for an update or delete.
     * (Saves opening the ConglomerateController twice.)
     */
    void clearHeapConglomerateController();

    /**
     * Get the ScanController, if any, that has already
     * been opened for the index when scaning for an update or delete.
     * (Saves opening the ScanController twice.)
     *
     * @return The ScanController, if available, to use for the update.
     */
    ScanController getIndexScanController();

    /**
     * Set the ScanController to be used for an update or delete,
     * when scanning an index that will also be updated
     * (Saves opening the ScanController twice.)
     *
     * @param indexSC    The ScanController to reuse for the update or delete.
     */
    void setIndexScanController(ScanController indexSC);

    /**
     * Get the conglomerate number of the index, if any, that has already
     * been opened for scaning for an update or delete.
     * (Saves opening the ScanController twice.)
     *
     * @return The conglomerate number, if available, to use for the update.
     */
    long getIndexConglomerateNumber();

    /**
     * Set the conglomerate number of the index to be used for an update or delete,
     * when scanning an index that will also be updated
     * (Saves opening the ScanController twice.)
     *
     * @param indexConglomerateNumber The conglomerate number of the index to reuse for the update or delete.
     */
    void setIndexConglomerateNumber(long indexConglomerateNumber);

    /**
     * Clear the info for the index to be re-used for update/delete.
     * (ScanController and conglomerate number.)
     */
    void clearIndexScanInfo();

    /**
     * Mark the Activation as being for create table.
     * (NOTE: We can do certain optimizations for
     * create table that we can't do for other DDL.)
     */
    void setForCreateTable();

    /**
     * Get whether or not this activation is for
     * create table.
     * (NOTE: We can do certain optimizations for
     * create table that we can't do for other DDL.)
     *
     * @return Whether or not this activation is for
     *           create table.
     */
    boolean getForCreateTable();

    /**
     * Save the TableDescriptor for the target of
     * DDL so that it can be passed between the
     * various ConstantActions during execution.
     */
    void setDDLTableDescriptor(TableDescriptor td);

    /**
     * Get the TableDescriptor for the target of
     * DDL.
     *
     * @return The TableDescriptor for the target of
     * DDL.
     */
    TableDescriptor getDDLTableDescriptor();

    /**
     * Set the maximum # of rows.  (# of rows that can
     * be returned by a ResultSet.  0 means no limit.)
     *
     * @param maxRows Maximum # of rows. (0 means no limit.)
     */
    void setMaxRows(int maxRows);

    /**
     * Get the maximum # of rows.  (# of rows that can
     * be returned by a ResultSet.  0 means no limit.)
     *
     * @return Maximum # of rows.  (0 means no limit.)
     */
    int getMaxRows();

        /**
         * Get the number of rows seen by this activation. This
         * is only set manually (i.e. underlying objects are
         * not required to keep track).
         *
         * @return the number of rows seen.
         */
        long getRowsSeen();

        /**
         * Add a number of rows seen by the activation.
         * @param rowsSeen the number of rows seen.
         */
        void addRowsSeen(long rowsSeen);
    /**
     * Is this Activation for a cursor?
     *
     * @return Whether or not this Activation is for a cursor.
     */
    boolean isCursorActivation();

    /**
     * Save the ResultSet for the target
     * of an update/delete to a VTI.
     */
    void setTargetVTI(java.sql.ResultSet targetVTI);

    /**
     * Get the ResultSet for the target
     * of an update/delete to a VTI.
     *
     * @return The ResultSet for the target
     * of an update/delete to a VTI.
     */
    java.sql.ResultSet getTargetVTI();

    ConstantAction    getConstantAction();

    //store a reference to the parent table result sets
    void setParentResultSet(TemporaryRowHolder rs, String resultSetId);

    /**
     * get the reference to parent table ResultSets, that will be needed by the
     * referential action dependent table scans.
     */
    Vector getParentResultSet(String resultSetId);

    //clear the parent resultset hash table;
    void clearParentResultSets();

    Hashtable getParentResultSets();

    /**
     * beetle 3865: updateable cursor using index.  A way of communication
     * between cursor activation and update activation.
     */
    void setForUpdateIndexScan(CursorResultSet forUpdateResultSet);

    CursorResultSet getForUpdateIndexScan();

    /**
        Return the set of dynamical created result sets, for procedures.
        Base implementation returns null, a generated class for a procedure overwrites
        this with a real implementation.
        @return null if no dynamic results exists. Otherwise an array of ResultSet
        arrays, each of length one containing null or a reference to a ResultSet.
    */
    java.sql.ResultSet[][] getDynamicResults();

    /**
        Return the maximum number of dynamical created result sets from the procedure definition.
        Base implementation returns 0, a generated class for a procedure overwrites
        this with a real implementation.
    */
    int getMaxDynamicResults();


    /**
     * Get the current SQL session context if in a nested connection of a
     * stored routine or in a substatement.
     */
    SQLSessionContext getSQLSessionContextForChildren();

    /**
     * Set up and return the current SQL session context for all immediately
     * nested connections stemming from the call or function invocation of the
     * statement corresponding to this activation (push=true) or for a
     * substatement, which shares the parents statement's session context
     * (push=false).
     * @param push true if used to push a new connection context
     */
    SQLSessionContext setupSQLSessionContextForChildren(boolean push);

    /**
     * This activation is created in a dynamic call context or a substatement
     * execution context, chain its parent statements activation..
     */
    void setParentActivation(Activation a);

    /**
     * This activation is created in a dynamic call context, or substatement
     * execution context; get its caller's or superstatement's activation.
     *
     * @return The caller's activation
     */
    Activation getParentActivation();

    void materialize() throws StandardException;

    boolean isMaterialized();

    boolean isBatched();

    boolean nextBatchElement() throws StandardException;

    Pair<PreparedStatement, Iterator<ParameterValueSet>> getBatch();

    void setBatch(Iterator<ParameterValueSet> params, PreparedStatement ps);

    DataSetProcessorType datasetProcessorType();

    boolean isSubStatement();

    void setSubStatement(boolean newValue);
}
