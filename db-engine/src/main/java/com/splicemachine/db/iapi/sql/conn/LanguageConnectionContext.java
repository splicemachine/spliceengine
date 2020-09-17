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

package com.splicemachine.db.iapi.sql.conn;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.db.InternalDatabase;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ContextId;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.LanguageFactory;
import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.db.iapi.sql.PreparedStatement;
import com.splicemachine.db.iapi.sql.compile.ASTVisitor;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.compile.OptimizerFactory;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.CursorActivation;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecutionStmtValidator;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionContext;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionStack;
import com.splicemachine.db.impl.sql.misc.CommentStripper;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * LanguageConnectionContext keeps the result sets,
 * and activations in use by the current connection.
 * <p>
 * More stable items, like other factories, are accessible through
 * the LanguageConnectionFactory or the LanguageFactory.
 *
 * @see LanguageConnectionFactory
 * @see com.splicemachine.db.iapi.sql.LanguageFactory
 */
public interface LanguageConnectionContext extends Context {

    /**
     * this is the ID we expect these contexts
     * to be stored into a context manager under.
     */
    String CONTEXT_ID = ContextId.LANG_CONNECTION;

    int    OUTERMOST_STATEMENT = 1;

    // Constants describing how this connection handles schemas
    int SQL92_SCHEMAS = 0;
    int USER_NAME_SCHEMA = 1; // User names are schema names.
    int NO_SCHEMAS = 2; // Schemas not implemented.

    /* String for logStatementText output */
    String xidStr = "(XID = ";
    String lccStr = "(SESSIONID = ";
    String dbnameStr = "(DATABASE = ";
    String drdaStr = "(DRDAID = ";
    String uuidStr = "(UUID = ";
    String execStr = "(ENGINE = ";
    String useridStr = "(USERID = ";

    // Lock Management

    int    SINGLE_TRANSACTION_LOCK = 1;
    int    MULTI_TRANSACTION_LOCK = 2;

    // controls casing of NON-delimited identifiers. ANSI casing forces all
    // non-delimited identifiers to be lower case.

    int    UNKNOWN_CASING = -1;
    int    ANSI_CASING = 0;
    int    ANTI_ANSI_CASING = 1;

    /**
     * Initialize. For use after pushing the contexts that initialization needs.
     *
     *
     * @exception StandardException thrown if something goes wrong
     */
    void initialize() throws StandardException;

    /**
     * Initialize Splice. For use after pushing the contexts that initialization needs.
     *
     *
     * @exception StandardException thrown if something goes wrong
     */
    void initializeSplice(String sessionUser, SchemaDescriptor defaultSchemaDescriptor) throws StandardException;


    /**
     * Get value of logStatementText.
     * (Whether or not to write info on currently
     * executing statement to error log.)
     *
     * @return value of logStatementText
     */
    boolean getLogStatementText();

    /**
     * Set value of logStatementText
     * (Whether or not to write info on currently
     * executing statement to error log.)
     *
     * @param logStatementText    Whether or not logStatementText property is set.
     */
    void setLogStatementText(boolean logStatementText);

    /**
     * Get value of logQueryPlan.
     * (Whether or not to write query plan info on currently
     * executing statement to error log.)
     *
     * @return value of logQueryPlan
     */
    boolean getLogQueryPlan();

    /**
     * Get value of tableLimitForExhaustiveSearch
     * @return value of tableLimitForExhaustiveSearch
     */
    int getTableLimitForExhaustiveSearch();

    /**
     * get the lock escalation threshold to use with this connection.
     */
    int getLockEscalationThreshold();

    /**
     * Add the activation to those known about by this connection.
     *
     */
    void addActivation(Activation a)
        throws StandardException;

    /**
     * Make a note that some activations are marked unused
     */
    void notifyUnusedActivation();

    /**
     * Remove the activation from those known about by this connection.
     *
     * @exception StandardException thrown if something goes wrong
     */
    void removeActivation(Activation a)
        throws StandardException;

    /**
     * Return the number of activation known for this connection.
     *
     */
    int getActivationCount();

    /**
     * See if a given cursor is available for use.  This is used
     * to locate the cursor during its execution.
     *
     * @return the activation for the given cursor, null if none exists.
     */
    CursorActivation lookupCursorActivation(String cursorName);

    /**
     * Return the last activation added
     * This is used to find the drop activation in dropViewCascade
     * so we can add warning messages to the activation
     *
     */
    Activation getLastActivation();

    /**
        Get a connection unique system generated name for a cursor.
    */
    String getUniqueCursorName();

    /**
        Get a connection unique system generated name for an unnamed savepoint.
    */
    String getUniqueSavepointName();

    /**
        Get a connection unique system generated id for an unnamed savepoint.
    */
    int getUniqueSavepointID();

    /**
     * Check if there are any global temporary tables declared for this connection.
     * @return true if there are declared temp tables for this connectoin else false
     *
     */
    boolean checkIfAnyDeclaredGlobalTempTablesForThisConnection();

    /**
     * Mark the passed temporary table as modified in the current unit of work. That information will be used at rollback time
     * The compile phase will generate code to call this method if the DML is on a temporary table
     *
     * @param tableName Mark the passed temporary table name as modified
     */
    void markTempTableAsModifiedInUnitOfWork(String tableName);
  
    /**
     * Add the declared global temporary table to the list of temporary tables known by this connection.
     * @param td Corresponding to the temporary table
     *
     */
    void addDeclaredGlobalTempTable(TableDescriptor td) throws StandardException;

    /**
     * Drop (mark the declared global temporary table for dropping) from the list of temporary tables known by this connection.
     * @param td Corresponding to the temporary table
     * @return true if dropped the temporary table. False if no such temporary table exists.
     *
     * @see com.splicemachine.db.impl.sql.conn.TempTableInfo
     */
    boolean dropDeclaredGlobalTempTable(TableDescriptor td);

    /**
     * Get table descriptor for the declared global temporary table from the list of temporary
     * tables known by this connection.
     * @param tableName Get table descriptor for the passed table name
     * @return TableDescriptor if found the temporary table. Else return null
     *
     */
    TableDescriptor getTableDescriptorForDeclaredGlobalTempTable(String tableName);

    /**
        Reset the connection before it is returned (indirectly) by
        a PooledConnection object. See EmbeddedConnection.
     */
    void resetFromPool()
         throws StandardException;

    /**
        Do a commit, as internally needed by Derby.  E.g.
         a commit for sync, or a commit for autocommit.  Skips
        checks that a user isn't doing something bad like issuing
        a commit in a nested xact.

        @param    commitStore    true if we should commit the Store transaction

        @exception StandardException thrown if something goes wrong
     */
    void internalCommit( boolean commitStore )
         throws StandardException;

    /**
        Similar to internalCommit() but has logic for an unsynchronized commit

        @param    commitflag    the flags to pass to commitNoSync in the store's
                            TransactionController

        @exception StandardException    thrown if something goes wrong
     */
    void internalCommitNoSync(int commitflag) throws StandardException;

    /**
        Do a commit, as issued directly by a user (e.g. via Connection.commit()
        or the JSQL 'COMMIT' statement.

        @exception StandardException thrown if something goes wrong
     */
    void userCommit() throws StandardException;

    /**
        Commit a distrubuted transaction.

        @param onePhase if true, allow it to commit without first going thru a
        prepared state.

        @exception StandardException    thrown if something goes wrong
     */
    void xaCommit(boolean onePhase) throws StandardException;


    /**
        Do a rollback, as internally needed by Derby.  E.g.
         a rollback for sync, or a rollback for an internal error.  Skips
        checks that a user isn't doing something bad like issuing
        a rollback in a nested xact.

        @exception StandardException thrown if something goes wrong
     */
    void internalRollback() throws StandardException;

    /**
        Do a rollback, as issued directly by a user (e.g. via Connection.rollback()
        or the JSQL 'ROLLBACK' statement.

        @exception StandardException thrown if something goes wrong
     */
    void userRollback() throws StandardException;

    /**
     * Let the context deal with a rollback to savepoint
     *
     * @param    savepointName    Name of the savepoint that needs to be rolled back
     * @param    refreshStyle    boolean indicating whether or not the controller should close
     * open conglomerates and scans. Also used to determine if language should close
     * open activations.
     * @param    kindOfSavepoint     A NULL value means it is an internal savepoint (ie not a user defined savepoint)
     * Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
     *   A String value for kindOfSavepoint would mean it is SQL savepoint
     *   A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint
     *
     * @exception StandardException thrown if something goes wrong
     */
    void internalRollbackToSavepoint
    ( String savepointName, boolean refreshStyle, Object kindOfSavepoint ) throws StandardException;

    /**
     * Let the context deal with a release of a savepoint
     *
     * @param    savepointName    Name of the savepoint that needs to be released
     * @param    kindOfSavepoint     A NULL value means it is an internal savepoint (ie not a user defined savepoint)
     * Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
     *   A String value for kindOfSavepoint would mean it is SQL savepoint
     *   A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint
     *
     * @exception StandardException thrown if something goes wrong
     */
    void releaseSavePoint ( String savepointName, Object kindOfSavepoint ) throws StandardException;


    /**
        Roll back a distrubuted transaction.

        @exception StandardException    thrown if something goes wrong
     */
    void xaRollback() throws StandardException;

    /**
      Sets a savepoint. Causes the Store to set a savepoint.

      @param    savepointName    name of savepoint
      @param    kindOfSavepoint     A NULL value means it is an internal savepoint (ie not a user defined savepoint)
      Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
      A String value for kindOfSavepoint would mean it is SQL savepoint
      A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint

        @exception StandardException thrown if something goes wrong
      */
    void    languageSetSavePoint( String savepointName, Object kindOfSavepoint )  throws StandardException;

    /**
     * Begin a nested transaction.
     *
     * @param readOnly The nested transaction would be read only if param value true
     *
     * @exception StandardException on error.
     * @see TransactionController#startNestedUserTransaction
     */

    void beginNestedTransaction(boolean readOnly) throws StandardException;

    /**
     * commit a nested transaction.
     * We do not provide a abortNestedTransaction.
     * If a nested xaction is aborted, then this results in the parent xaction
     * also being aborted. This is not what we need for releasing
     * compile time locks or autoincrement-- hence we do not provide
     * abortNestedTransaction.
     *
     * @exception StandardException thrown on erro
     *
     * @see TransactionController#startNestedUserTransaction
     */
    void commitNestedTransaction() throws StandardException;

    /**
        Get the transaction controller to use with this language connection
        context at compile time.
     */
    TransactionController getTransactionCompile();

    /**
        Get the transaction controller to use with this language connection
        context during execute time.
     */

    TransactionController getTransactionExecute();

    void pushNestedTransaction(TransactionController trans);

    TransactionController popNestedTransaction();
    /**
        Get the data dictionary

        @return the data dictionary

     */
    DataDictionary getDataDictionary();

    /**
        Get the data value factory to use with this language connection
        context.
     */
    DataValueFactory getDataValueFactory();

    /**
        Get the language factory to use with this language connection
        context.
     */
    LanguageFactory getLanguageFactory();

    /**
     * get the optimizer factory to use with this language connection context.
     */
    OptimizerFactory getOptimizerFactory();

    /**
        Get the language connection factory to use with this language connection
        context.
     */
    LanguageConnectionFactory getLanguageConnectionFactory();

    /**
     * Get the Authorization Id of the current user
     *
     * @param a activation
     * @return String the authorization id
     */
    String getCurrentUserId(Activation a);

    void setCurrentUser(Activation a, String userName);

    void setCurrentGroupUser(Activation a, List<String> groupUsers);
    /**
     * Get the current group user
     * @param a activation
     * @return String current group user
     */
    public List<String> getCurrentGroupUser(Activation a);

    /**
     *  Get the Authorization Id of the session user
     *
     * @return String   the authorization id
     */
    String getSessionUserId();


    /**
     * Get the default schema (used at compile-time when no activation
     * is yet available, cf. the activation argument overload version.
     *
     * @return SchemaDescriptor    the default schema
     */
    SchemaDescriptor getDefaultSchema();

    /**
     * Get the default schema (used at execution time).  At execution
     * time, the current statement context is not always a reliable
     * place to find the correct SQL session context, viz. when a
     * dynamic result set referencing CURRENT SCHEMA is accessed after
     * a called procedure has returned only the activation of the call
     * is live and still holds the correct session context.
     * @param a current activation
     *
     * @return SchemaDescriptor    the default schema
     */
    SchemaDescriptor getDefaultSchema(Activation a);

    /**
     * Set the default schema (at compile-time, see explanations for
     * getDefaultSchema overloads).
     *
     * @param sd the new default schema.
     * If null, then the default schema descriptor is used.
     *
     * @exception StandardException thrown on failure
     */
    void setDefaultSchema(SchemaDescriptor sd)
        throws StandardException;

    /**
     * Set the default schema (at execution time, see explanations for
     * getDefaultSchema overloads); This version is used by SET SCHEMA.
     *
     * @param a current activation
     * @param sd the new default schema.
     * If null, then the default schema descriptor is used.
     *
     * @exception StandardException thrown on failure
     */
    void setDefaultSchema(Activation a, SchemaDescriptor sd)
        throws StandardException;

    /**
     * Reset any occurence of schemaName as current default schema in
     * the SQLSessionContext stack to the initial default,
     * because schemaName is no longer a valid schema.
     *
     * @param activation current activation
     * @param schemaName the schema name occurences of which is to be reset
     *
     * @throws StandardException
     */
    void resetSchemaUsages(Activation activation, String schemaName)
        throws StandardException;

    /**
     *    Get the current schema name (at compile-time, see explanations for
     * getDefaultSchema overloads).
     *
     * @return SchemaDescriptor    the current schema
     */
    String getCurrentSchemaName();

    /**
     * Get the current schema name (at execution time, see explanations for
     * getDefaultSchema overloads); This version is used by CURRENT SCHEMA.
     *
     * @return SchemaDescriptor    the current schema
     */
    String getCurrentSchemaName(Activation a);


    /**
     * Return true if this schema name is the initial default schema for the
     * current session.
     * @param schemaName
     * @return true
     */
    boolean isInitialDefaultSchema(String schemaName);

    /**
     * Get the identity column value most recently generated.
     *
     * @return the generated identity column value
     */
    Long getIdentityValue();

    /**
     * Set the field of most recently generated identity column value.
     *
     * @param val the generated identity column value
     */
    void setIdentityValue(long val);

    /**
     * Verify that there are no activations with open result sets
     * on the specified prepared statement.
     *
     * @param pStmt        The prepared Statement
     * @param provider    The object precipitating a possible invalidation
     * @param action    The action causing the possible invalidation
     *
     * @return Nothing.
     *
     * @exception StandardException thrown on failure
     */
    boolean verifyNoOpenResultSets(PreparedStatement pStmt, Provider provider,
                                       int action)
            throws StandardException;

    /**
     * Verify that there are no activations with open held result sets.
     *
     * @return boolean  Found no open resultsets.
     *
     * @exception StandardException thrown on failure
     */
    boolean verifyAllHeldResultSetsAreClosed()
            throws StandardException;

    /**
     * Push a CompilerContext on the context stack with
     * the current default schema as the default schema
     * which we compile against.
     *
     * @return the compiler context
     *
     * @exception StandardException thrown on failure
     */
    CompilerContext pushCompilerContext();

    /**
     * Push a CompilerContext on the context stack with
     * the passed in default schema as the default schema
     * we compile against.
     *
     * @param sd the default schema
     *
     * @return the compiler context
     *
     * @exception StandardException thrown on failure
     */
    CompilerContext pushCompilerContext(SchemaDescriptor sd);

    /**
     * Pop a CompilerContext off the context stack.
     *
     * @param compilerContext  The compiler context.
     *
     * @exception StandardException thrown on failure
     */
    void popCompilerContext(CompilerContext compilerContext);

    /**
     * Push a StatementContext on the context stack.
     *
     * @param isAtomic whether a commit/rollback is permitted
     *    from a nested connection under this statement
     *
     * @param stmtText the text of the statement.  Needed for any language
     *     statement (currently, for any statement that can cause a trigger
     *     to fire).  Please set this unless you are some funky jdbc setXXX
     *    method or something.
     *
     * @param pvs parameter value set, if it has one
     *
     * @param rollbackParentContext True if 1) the statement context is
     *     NOT a top-level context, AND 2) in the event of a statement-level
     *    exception, the parent context needs to be rolled back, too.
     *
     * @param timeoutMillis Timeout value for this statement, in milliseconds.
     *  Zero means no timeout.
     *
     * @return StatementContext The statement context.
     *
     */
    StatementContext pushStatementContext(boolean isAtomic, boolean isForReadOnly, String stmtText,
        ParameterValueSet pvs, boolean rollbackParentContext, long timeoutMillis);

    /**
     * Pop a StatementContext of the context stack.
     *
     * @param statementContext  The statement context.
     * @param error                The error, if any  (Only relevant for DEBUG)
     */
    void popStatementContext(StatementContext statementContext,
                             Throwable error);

    /**
     * Push a new execution statement validator.  An execution statement
     * validator is an object that validates the current statement to
     * ensure that it is permitted given the current execution context.
     * An example of a validator a trigger ExecutionStmtValidator that
     * doesn't allow ddl on the trigger target table.
     * <p>
     * Multiple ExecutionStmtValidators may be active at any given time.
     * This mirrors the way there can be multiple connection nestings
     * at a single time.  The validation is performed by calling each
     * validator's validateStatement() method.  This yields the union
     * of all validations.
     *
     * @param validator the validator to add
     */
    void pushExecutionStmtValidator(ExecutionStmtValidator validator);

    /**
     * Remove the validator.  Does an object identity (validator == validator)
      * comparison.  Asserts that the validator is found.
     *
     * @param validator the validator to remove
     *
     * @exception StandardException on error
     */
    void popExecutionStmtValidator(ExecutionStmtValidator validator)
        throws StandardException;

    /**
     * Validate a statement.  Does so by stepping through all the validators
     * and executing them.  If a validator throws and exception, then the
     * checking is stopped and the exception is passed up.
     *
     * @param constantAction the constantAction that is about to be executed (and
     *    should be validated
      *
     * @exception StandardException on validation failure
     */
    void validateStmtExecution(ConstantAction constantAction)
        throws StandardException;

    /**
     * Push a new trigger execution context.
     * <p>
     * Multiple TriggerExecutionContexts may be active at any given time.
     *
     * @param tec the trigger execution context
     *
     * @exception StandardException on trigger recursion error
     */
    void pushTriggerExecutionContext(TriggerExecutionContext tec)
        throws StandardException;

    /**
     * Remove the tec.  Does an object identity (tec == tec)
      * comparison.  Asserts that the tec is found.
     *
     * @param tec the tec to remove
     *
     * @exception StandardException on error
     */
    void popTriggerExecutionContext(TriggerExecutionContext tec)
        throws StandardException;

    /**
     * Pop all trigger execution contexts. Either means an error has occurred
     * or a trigger has been dropped.
     */
    void popAllTriggerExecutionContexts();

    /**
     * Get the topmost tec.
     *
     * @return the tec
     */
    TriggerExecutionContext getTriggerExecutionContext();

    /**
     * Set the trigger table descriptor.  Used to compile
     * statements that may special trigger pseudo tables.
     *
     * @param td the table that the trigger is
     * defined upon
     *
     */
    void pushTriggerTable(TableDescriptor td);

    /**
     * Remove the trigger table descriptor.
     *
     * @param td the table to remove from the stack.
     */
    void popTriggerTable(TableDescriptor td);

    /**
     * Get the topmost trigger table descriptor
     *
     * @return the table descriptor, or null if we
     * aren't in the middle of compiling a create
     * trigger.
     */
    TableDescriptor getTriggerTable();

    /**
     * Increment the DataDictionary bind count.  This is for keeping track
     * of nested binding, which can happen if SQL statements are bound from
     * within static initializers.
     *
     * @return    The new bind count
     */
    int incrementBindCount();

    /**
     * Decrement the DataDictionary bind count.
     *
     * @return    The new bind count
     */
    int decrementBindCount();

    /**
     * Get the DataDictionary bind count.
     *
     * @return    The current bind count.
     */
    int getBindCount();

    /**
     * Remember that the DataDictionary is in write mode, so we can take
     * it out of write mode at the end of the transaction.
     */
    void setDataDictionaryWriteMode();

    /**
     * Return true if the data dictionary is in write mode (that is, this
     * context was informed that is is in write mode by the method call
     * setDataDictionaryWriteMode().
     */
    boolean dataDictionaryInWriteMode();

    /**
      *    Reports how many statement levels deep we are.
      *
      *    @return    a statement level >= OUTERMOST_STATEMENT
      */
    int        getStatementDepth();

    /**
      Returns the Database of this connection.
     */
    InternalDatabase getDatabase();

    /**
     * Returns true if isolation level has been set using JDBC/SQL.
     */
    boolean isIsolationLevelSetUsingSQLorJDBC();
    /**
     * Reset the isolation level flag used to keep correct isolation level
     * state in BrokeredConnection. This resetting will happen at the start
     * and end of a global transaction, after the BrokeredConection's
     * isolation level state is brought upto date with the EmbedConnection's
     * isolation state.
     * The flag gets set to true when isolation level is set using JDBC/SQL.
     */
    void resetIsolationLevelFlagUsedForSQLandJDBC();

    /**
     * Set current isolation level.
     *
     * @param isolationLevel    The new isolationLevel.
     */
    void setIsolationLevel(int isolationLevel) throws StandardException;

    /**
     * Get the current isolation level.
     *
     * @return The current isolation level.
     */
    int getCurrentIsolationLevel();

    /**
     * Get the current isolation level in DB2 format.
     *
     * @return The current isolation level as a 2 character string.
     */
    String getCurrentIsolationLevelStr();
    void setPrepareIsolationLevel(int isolationLevel) ;

    /**
     * Get the prepare isolation level.
     * If the isolation level has been explicitly set with a SQL statement or
     * embedded call to setTransactionIsolation, this will return
     * ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL
     * SET ISOLATION always takes priority.
     *
     */
    int getPrepareIsolationLevel();

    /**
     * Set the readOnly status for the current connection. This can
     * only be called when the current transaction has not done
     * any work.
     *
     * @param onOrOff true sets the connection to be readOnly and
     *                false sets it to readWrite.
     *
     * @exception StandardException The call failed and the readOnly
     *                status has not changed.
     */
    void setReadOnly(boolean onOrOff) throws StandardException;

    /**
      * Get the readOnly status for the current connection.
      */
    boolean isReadOnly() throws StandardException;

    /**
     * Get an Authorizer for this connection.
     */
    Authorizer getAuthorizer() throws StandardException;

    /**
     *    Get the current StatementContext.
     */
    StatementContext getStatementContext();

    /**
     * Return a PreparedStatement object for the query.
     * This method first tries to locate the PreparedStatement object from a statement
     * cache.  If the statement is not found in the cache, the query will be compiled and
     * put into the cache.
     * @param compilationSchema schema
     * @param sqlText sql query string
     * @param isForReadOnly read only status for resultset. Set to true if the concurrency mode for the resultset
     *                      is CONCUR_READ_ONLY
     * @param allowInternalSyntax If true, then this query is allowed to use internal
     *                      sql syntax. One instance where this will be true is if a
     *                      metadata query is getting executed.
     */
    PreparedStatement prepareInternalStatement(SchemaDescriptor compilationSchema,
                                               String sqlText, boolean isForReadOnly, boolean allowInternalSyntax)
        throws StandardException;

        /**
     * Return a PreparedStatement object for the query.
     * This method first tries to locate the PreparedStatement object from a statement
     * cache.  If the statement is not found in the cache, the query will be compiled and
     * put into the cache.
     * The schema used when compiling the statement is the same schema as returned by
     * getDefaultSchema().  For internal statements, the read only status is set to
     * true.
     * Calling this method is equivalent to calling
     * prepareExternalStatement(lcc.getDefaultSchema(), sqlText, true);
     *
     * @param sqlText sql query string
     */
        PreparedStatement prepareInternalStatement(String sqlText)
        throws StandardException;

    /**
     * Control whether or not optimizer trace is on.
     *
     * @param onOrOff    Whether to turn optimizer trace on (true) or off (false).
     *
     * @return Whether or not the call was successful.  (false will be returned when optimizer tracing is not supported.)
     */
    boolean setOptimizerTrace(boolean onOrOff);

    /**
     * Get whether or not optimizer trace is on.
     *
     * @return Whether or not optimizer trace is on.
     */
    boolean getOptimizerTrace();

    /**
     * Control whether or not optimizer trace is generated in html.
     *
     * @param onOrOff    Whether or not optimizer trace will be in html (true) or not (false).
     *
     * @return Whether or not the call was successful.  (false will be returned when optimizer tracing is not supported.)
     */
    boolean setOptimizerTraceHtml(boolean onOrOff);

    /**
     * Get whether or not optimizer trace html is on.
     *
     * @return Whether or not optimizer trace html is on.
     */
    boolean getOptimizerTraceHtml();

    /**
     * Get the optimizer trace output for the last optimized query as a String.  If optimizer trace
     * html is on, then the String will contain the html tags.
     *
     * @return The optimizer trace output for the last optimized query as a String.
     *    Null will be returned if optimizer trace output is off or not supported
     *    or no trace output was found or an exception occurred.
     */
    String getOptimizerTraceOutput();

    /**
     * Set the optimizer trace output to the specified String.
     * (Done at the beginning of each statement.)
     */
    void setOptimizerTraceOutput(String startingText);

    /**
     * Append the latest output to the optimizer trace output.
     */
    void appendOptimizerTraceOutput(String output);

    /**
      *    Reports whether there is any outstanding work in the transaction.
      *
      *    @return        true if there is outstanding work in the transaction
      *                false otherwise
      */
    boolean    isTransactionPristine();


    /**
     * Returns the last autoincrement value inserted by this connection.
     * If no values have been inserted into the given column a NULL value
     * is returned.
     *
     * @param schemaName
     * @param tableName
     * @param columnName
     */
    Long lastAutoincrementValue(String schemaName,
                                String tableName,
                                String columnName);

    /**
     * Sets autoincrementUpdate-- this variable allows updates to autoincrement
     * columns if it is set to true. The default is ofcourse false; i.e
     * ai columns cannot be directly modified by the user. This is set to
     * true by AlterTableConstantAction, when a new ai column is being added
     * to an existing table.
     *
     * @param flag     the value for autoincrementUpdate (TRUE or FALSE)
     * @see com.splicemachine.db.impl.sql.execute.AlterTableConstantAction#updateNewAutoincrementColumn
     *
     */
    void setAutoincrementUpdate(boolean flag);

    /**
     * Returns the current value of autoincrementUpdate.
     *
     * @return true if updates to autoincrement columns is permitted.
     */
    boolean getAutoincrementUpdate();

    /**
     * Copy a map of autoincrement key value pairs into the cache of
     * ai values stored in the language connection context.
     */
    void copyHashtableToAIHT(Map<String, Long> from);

    /**
     * returns the <b>next</b> value to be inserted into an autoincrement col.
     * This is used internally by the system to generate autoincrement values
     * which are going to be inserted into a autoincrement column. This is
     * used when as autoincrement column is added to a table by an alter
     * table statemenet and during bulk insert.
     *
     * @param schemaName
     * @param tableName
     * @param columnName identify the column uniquely in the system.
     *
     * @exception StandardException on error.
     */
    long nextAutoincrementValue(String schemaName, String tableName,
                                String columnName)
        throws StandardException;

    /**
     * Flush the cache of autoincrement values being kept by the lcc.
     * This will result in the autoincrement values being written to the
     * SYSCOLUMNS table as well as the mapping used by lastAutoincrementValue
     *
     * @param tableUUID the table which is being flushed; we need this value to
     * identify the table for which the autoincrement counter is being
     * maintained.
     *
     * @exception StandardException thrown on error.
     *
     * @see LanguageConnectionContext#lastAutoincrementValue
     * @see com.splicemachine.db.impl.sql.conn.GenericLanguageConnectionContext#lastAutoincrementValue
     * @see com.splicemachine.db.iapi.db.ConnectionInfo#lastAutoincrementValue
     */
    void autoincrementFlushCache(UUID tableUUID)
        throws StandardException;

    /**
     * Create an autoincrement counter to be used on behalf of a SQL-J
     * statement. The counter is identified by (schemaName, tableName,
     * columnName). The counter must be freed up by calling
     * autoincrementFlushCache at the end of the statement. It is expected
     * that a ai-counter with the same signaure doesn't exist when the
     * method is called.
     *
     * @param         s                    SchemaName
     * @param        t                    TableName
     * @param        c                    ColumnName
     * @param        initialValue        initial value of the counter.
     * @param        increment            increment for the counter.
     * @param        position            column position (1-based).
     */
    void autoincrementCreateCounter(String s, String t, String c,
                                    Long initialValue, long increment,
                                    int position);

    /**
     * Get the instance number of this LCC.
     *
     * @return instance number of this LCC.
     */
    int getInstanceNumber();

    /**
     * Get the DRDA ID of this LCC.
     *
     * @return DRDA ID this LCC.
     */
    String getDrdaID();


    /**
     * Get the DRDA interruption token of this LCC
     *
     * @return DRDA interruption token this LCC.
     */
    String getRdbIntTkn();

    /**
     * Set the DRDA ID of this LCC.
     *
     * @param drdaID DRDA ID.
     */
    void setDrdaID(String drdaID);

    /**
     * Get the database name of this LCC.
     *
     * @return database name of this LCC.
     */
    String getDbname();

    /**
     * Check if in SQL standard mode, with support for Grant & Revoke
     *
     * @return True if SQL standard permissions are being used
     */
    boolean usesSqlAuthorization();

    /**
     * Close any unused activations in this connection context.
     */
    void closeUnusedActivations() throws StandardException;

    /**
     * Set the current role
     *
     * @param a activation of set role statement
     * @param role  the id of the role to be set to current
     */
    void setCurrentRole(Activation a, String role);

    /**
     * Set the current role
     *
     * @param a activation of set role statement
     * @param roles  the list of roles to be set to current
     */
    public void setCurrentRoles(Activation a, List<String> roles);

    /**
     * Get the current set of roles of the dynamic
     * call context associated with this activation.
     *
     * @param a activation of statement needing current role
     * @return List of roleids
     */
    List<String> getCurrentRoles(Activation a);

    /**
     * Removes all revoked roles from the list of current roles of the dynamic
     * call context associated with this activation.
     * @param a activation of the statement wanting to refresh the roles.
     */
    void refreshCurrentRoles(Activation a) throws StandardException;

    /**
     * Get the current role authorization identifier in external delimited form
     * (not case normal form) of the dynamic call context associated with this
     * activation.
     * @param a activation of statement needing current role
     * @return String the role id in delimited form (i.e. <b>not</b>
     * internal case normal form</b>)
     *
     * @throws StandardException  standard exception policy
     */
    String getCurrentRoleIdDelimited(Activation a)
            throws StandardException;

    /**
     * @param a activation of statement
     * @return String of the group user list in delimited form
     * @throws StandardException
     */
    String getCurrentGroupUserDelimited(Activation a) throws StandardException;

    /**
     * Checks whether the given role can be legally set for the current user.
     *
     * This method will read (potentially) the dictionary, so it needs
     * a transaction context.
     *
     * @param a activation
     * @param role string containing role name
     *
     * @return true if the role can be set
     * @throws StandardException standard exception policy
     */
    boolean roleIsSettable(Activation a, String role)
            throws StandardException;

    /**
     *
     * @param a activation
     * @param roleName role name for the role to remove from session's current role list.
     * @throws StandardException
     */
    public void removeRole(Activation a, String roleName);

    /**
     *
     * @param a activation
     * @param rolesToRemove a list of roles to remove from session's current role list.
     * @throws StandardException
     */
    public void removeRoles(Activation a, List<String> rolesToRemove);

    /**
     * Create a new SQL session context for the current activation on the basis
     * of the existing SQL session context. This happens when a stored
     * procedure or function that can contain SQL is invoked, cf. SQL 2003
     * section 4.27.3, since this gives rise to a nested connection.
     * <p>
     * Called from generated code, see
     * {@code StaticMethodCallNode#generateSetupNestedSessionContext}.
     * <p>
     * The new SQL session context is also set in the current statement
     * context (of the invocation).
     *
     * @see com.splicemachine.db.impl.sql.compile.StaticMethodCallNode#generateSetupNestedSessionContext
     * @see StatementContext#getSQLSessionContext
     * @see #setupSubStatementSessionContext
     *
     * @param a activation of the statement which performs the call.
     * @param definersRights if the method should run with definer's rights
     * @param definer authorization id of the definer
     */
    void setupNestedSessionContext(Activation a,
                                   boolean definersRights,
                                   String definer)
            throws StandardException;

    /**
     * Get the value of top level session context of the top level connection.
     */
    SQLSessionContext getTopLevelSQLSessionContext();

    /**
     * Used when a statement as part of its operation executes an other
     * statement. In contrast to setupNestedSessionContext, the activation (for
     * the substatement) just inherits the current session context from the
     * parent statements activation, it does <b>not</b> push a new copy on the
     * stack of session contexts.
     *
     * Currently, this is used in the following situations:
     * <ul>
     *     <li>With {@code ALTER TABLE} adding a column which has a default
     *         values, the default value for all the existing rows is added
     *         using an {@code UPDATE} substatement.
     *     <li>With {@code ALTER TABLE} adding a a check constraint, we will use
     *         a substatement {@code SELECT} to check if all rows satisfy the
     *         constraint.
     *     <li>{@code ResultSet.insertRow}, {@code updateRow}
     *         and {@code deleteRow}.
     *     <li>During trigger body execution.
     * </ul>
     * @see #setupNestedSessionContext
     */
    void setupSubStatementSessionContext(Activation a)
            throws StandardException;

    /**
     * Create a fresh SQLSessionContext for this connection.
     * @return new SQLSessionContext
     */
    SQLSessionContext createSQLSessionContext();

    /**
     * Debug method for remembering the last query tree.
     */
    void    setLastQueryTree(Object queryTree);

    /**
     * Debug method for retrieving the last query tree.
     */
    Object    getLastQueryTree();

    /**
     * Return a map of AST nodes that have already been printed during a
     * compiler phase, so as to be able to avoid printing a node more than once.
     * @see com.splicemachine.db.impl.sql.compile.QueryTreeNode#treePrint(int)
     * @return the map
     */
    Map getPrintedObjectsMap();

    /**
     * Set a Visitor which walks the AST at various stages. This is useful
     * for poking user-written inspectors into the parse, bind, and optimize phases.
     *
     * @param visitor The Visitor which should walk the tree. Could be null.
     */
    void setASTVisitor(ASTVisitor visitor);

    /**
     * Get the Visitor which should walk the AST.
     *
     * @return The Visitor for that phase. Could be null.
     */
    ASTVisitor getASTVisitor();

    /**
     * Set the exception created and associated with the detected interruped
     * status flag.
     *
     * @param e the created exception
     */
    void setInterruptedException(StandardException e);

    /**
     * Get exception created when we detected interruped status
     * flag.
     *
     * @return saved exception
     */
    StandardException getInterruptedException();

    /**
     * Get the referenced column map for a table
     *
     * @return the map
     */
    FormatableBitSet getReferencedColumnMap(TableDescriptor td);

    /**
     * Set the referenced column map for a table
     *
     * @param td the table descriptor
     * @param map the map
     */
    void setReferencedColumnMap(TableDescriptor td,
                                FormatableBitSet map);

    /**
     * Enter restore mode. In this mode no statements can be executed. It is reset when the
     * database is rebooted.
     */
    void enterRestoreMode();

    /**
     * Set a trigger execution context (TEC) stack on this LCC.<br/>
     * Used to serialize TECs across serialization boundaries.
     * @param triggerStack the stack to set
     */
    void setTriggerStack(TriggerExecutionStack triggerStack);

    /**
     * Get the trigger execution context (TEC) stack from this LCC><br/>
     * Used to serialize TECs across serialization boundaries.<br/>
     * <b>Note:</b> may be null.  Use {@link #hasTriggers()} to check.
     * @return the trigger execution stack, possibly null.
     */
    TriggerExecutionStack getTriggerStack();

    /**
     * Check to see if this LCC has any triggers.<br/>
     * Used to see if we need to serialize a trigger execution context stack.
     * @return true, if this LCC has any trigger execution contexts.
     */
    boolean hasTriggers();

    void setBadFile(String badFile);

    String getBadFile();

    void resetBadFile();

    void setFailedRecords(long failedRecords);

    long getFailedRecords();

    void resetFailedRecords();

    void setRecordsImported(long recordsImported);

    long getRecordsImported();

    void resetRecordsImported();

    DataSetProcessorType getDataSetProcessorType();

    /**
     *
     * Setting the dynamic withDescriptors
     *
     * @param withDescriptors
     */
    void setWithStack(Map<String,TableDescriptor> withDescriptors);

    /**
     *
     * Checking for with clause handling
     *
     * @param name
     * @return
     */
    TableDescriptor getWithDescriptor(String name);

    /**
     *
     * Null out the with stack.
     *
     */
    void popWithStack();

    /**
     * Gets the current control execution limiter
     * @return
     */
    ControlExecutionLimiter getControlExecutionLimiter();

    /**
     * Sets the current control execution limiter
     * @return
     */
    void setControlExecutionLimiter(ControlExecutionLimiter executionLimiter);


    String getClientIPAddress();

    void logStartCompiling(String statement);
    void logEndCompiling(String statement, long nanoTimeSpent);
    void logErrorCompiling(String statement, Throwable t, long nanoTimeSpent);
    void logCommit();
    void logRollback();
    void logStartFetching(String uuid, String statement);
    void logEndFetching(String uuid, String statement, long fetchedRows);
    void logNextBatch(ParameterValueSet pvs);
    void logStartExecuting(String uuid, String engine, String stmt, ExecPreparedStatement ps,
                           ParameterValueSet pvs);
    void logEndExecuting(String uuid, long modifiedRows, long badRecords, long nanoTimeSpent);

    void setSessionProperties(Properties newProperties);
    SessionProperties getSessionProperties();
    String getCurrentSessionPropertyDelimited();

    void setOrigStmtTxt(String stmt);
    String getOrigStmtTxt();

    CommentStripper getCommentStripper();
    boolean getIgnoreCommentOptEnabled();

    // True if the connecting client can read decimal
    // data with 38 digits of precision.
    boolean clientSupportsDecimal38();

	void setClientSupportsDecimal38(boolean newVal);

	String getUserName();

	void setReplicationRole(String role);

	String getReplicationRole();

    boolean isNLJPredicatePushDownDisabled();

}
