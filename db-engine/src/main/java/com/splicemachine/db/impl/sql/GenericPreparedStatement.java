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

package com.splicemachine.db.impl.sql;


import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.services.monitor.ModuleFactory;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.*;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.ResubmitDistributedException;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SPSDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecCursorTableReference;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.util.ByteArray;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryCache;
import com.splicemachine.db.impl.sql.compile.CursorNode;
import com.splicemachine.db.impl.sql.compile.StatementNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.List;

/**
 * Basic implementation of prepared statement. Relies on implementation of ResultDescription and Statement that
 * are also in this package.
 * <p/>
 * These are both dependents (of the schema objects and prepared statements they depend on) and providers.  Prepared
 * statements that are providers are cursors that end up being used in positioned delete and update statements
 * (at present).
 * <p/>
 * This is impl with the regular prepared statements; they will never have the cursor info fields set.
 * <p/>
 * Stored prepared statements extend this implementation
 */
@SuppressFBWarnings(value = "IS2_INCONSISTENT_SYNC", justification = "DB-10223")
public class GenericPreparedStatement implements ExecPreparedStatement {

    ///////////////////////////////////////////////
    //
    // WARNING: when adding members to this class, be
    // sure to do the right thing in getClone(): if
    // it is PreparedStatement specific like finished,
    // then it shouldn't be copied, but stuff like parameters
    // must be copied.
    //
    ////////////////////////////////////////////////

    ////////////////////////////////////////////////
    // STATE that is copied by getClone()
    ////////////////////////////////////////////////
    public Statement statement;
    protected GeneratedClass activationClass; // satisfies Activation
    protected ResultDescription resultDesc;
    protected DataTypeDescriptor[] paramTypeDescriptors;
    private String spsName;
    private SQLWarning warnings;

    //If the query node for this statement references SESSION schema tables, mark it so in the boolean below
    //This information will be used by EXECUTE STATEMENT if it is executing a statement that was created with NOCOMPILE. Because
    //of NOCOMPILE, we could not catch SESSION schema table reference by the statement at CREATE STATEMENT time. Need to catch
    //such statements at EXECUTE STATEMENT time when the query is getting compiled.
    //This information will also be used to decide if the statement should be cached or not. Any statement referencing SESSION
    //schema tables will not be cached.
    private boolean referencesSessionSchema;

    // fields used for cursors
    protected ExecCursorTableReference targetTable;
    protected ResultColumnDescriptor[] targetColumns;
    protected String[] updateColumns;
    protected int updateMode;

    protected ConstantAction executionConstants;
    protected Object[] savedObjects;
    protected List requiredPermissionsList;

    // fields for dependency tracking
    protected String UUIDString;
    protected UUID UUIDValue;

    private boolean needsSavepoint;

    private String execStmtName;
    private String execSchemaName;
    protected boolean isAtomic;
    protected String sourceTxt;

    private int inUseCount;

    // true if the statement is being compiled.
    boolean compilingStatement;

    /* True if the statement was invalidated while it was being compiled. */
    boolean invalidatedWhileCompiling;

    ////////////////////////////////////////////////
    // STATE that is not copied by getClone()
    ////////////////////////////////////////////////
    // fields for run time stats
    protected long parseTime;
    protected long bindTime;
    protected long optimizeTime;
    protected long generateTime;
    protected long compileTime;
    protected Timestamp beginCompileTimestamp;
    protected Timestamp endCompileTimestamp;

    //private boolean finished;
    protected boolean isValid;
    protected boolean spsAction;

    /* Incremented for each (re)compile. */
    private long versionCounter;

    private boolean isAutoTraced;

    private boolean hasXPlainTableOrProcedure;

    private DataSetProcessorType datasetProcessorType;
    //
    // constructors
    //

    GenericPreparedStatement() {
        /* Get the UUID for this prepared statement */
        ModuleFactory moduleFactory = Monitor.getMonitor();
        if (moduleFactory != null) {
            UUIDFactory uuidFactory = moduleFactory.getUUIDFactory();
            UUIDValue = uuidFactory.createUUID();
            UUIDString = UUIDValue.toString();
        }
        spsAction = false;
    }

    /**
     */
    public GenericPreparedStatement(Statement st) {
        this();
        statement = st;
    }

    //
    // PreparedStatement interface
    //
    @Override
    public synchronized boolean upToDate() throws StandardException {
        return isUpToDate();
    }

    /**
     * Check whether this statement is up to date and its generated class is identical to the supplied class object.
     */
    @Override
    public synchronized boolean upToDate(GeneratedClass gc) {
        return (activationClass == gc) && isUpToDate();
    }

    /**
     * Unsynchronized helper method for {@link #upToDate()} and {@link
     * #upToDate(GeneratedClass)}. Checks whether this statement is up to date.
     *
     * @return {@code true} if this statement is up to date, {@code false} otherwise
     */
    private boolean isUpToDate() {
        return isValid && (activationClass != null) && !compilingStatement;
    }

    @Override
    public void rePrepare(LanguageConnectionContext lcc) throws StandardException {
        if (!upToDate()) {
            PreparedStatement ps = statement.prepare(lcc);

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(ps == this, "ps != this");
        }
    }

    /**
     * Get a new activation instance.
     *
     * @throws StandardException thrown if finished.
     */
    @Override
    public Activation getActivation(LanguageConnectionContext lcc, boolean scrollable) throws StandardException {
        Activation ac;
        synchronized (this) {
            GeneratedClass gc = getActivationClass();

            if (gc == null) {
                rePrepare(lcc);
                gc = getActivationClass();
            }

            ac = new GenericActivationHolder(lcc, gc, this, scrollable);

            inUseCount++;
        }
        // DERBY-2689. Close unused activations-- this method should be called
        // when I'm not holding a lock on a prepared statement to avoid
        // deadlock.
        lcc.closeUnusedActivations();

        Activation parentAct = null;
        StatementContext stmctx = lcc.getStatementContext();

        if (stmctx != null) {
            // If not null, parentAct represents one of 1) the activation of a
            // calling statement and this activation corresponds to a statement
            // inside a stored procedure or function, and 2) the activation of
            // a statement that performs a substatement, e.g. trigger body
            // execution.
            parentAct = stmctx.getActivation();
        }

        ac.setParentActivation(parentAct);

        return ac;
    }

    @Override
    public ResultSet executeSubStatement(LanguageConnectionContext lcc, boolean rollbackParentContext, long timeoutMillis)
            throws StandardException {
        Activation parent = lcc.getLastActivation();
        Activation a = getActivation(lcc, false);
        a.setSingleExecution();
        lcc.setupSubStatementSessionContext(parent);
        return executeStmt(a, rollbackParentContext, timeoutMillis);
    }

    @Override
    public ResultSet executeSubStatement(Activation parent,
                                         Activation activation,
                                         boolean rollbackParentContext,
                                         long timeoutMillis) throws StandardException {
        parent.getLanguageConnectionContext().setupSubStatementSessionContext(parent);
        activation.setSubStatement(true);
        return executeStmt(activation, rollbackParentContext, timeoutMillis);
    }

    @Override
    public ResultSet execute(Activation activation, long timeoutMillis) throws StandardException {
        return executeStmt(activation, false, timeoutMillis);
    }

    /**
     * The guts of execution.
     *
     * @param rollbackParentContext True if 1) the statement context is
     *                              NOT a top-level context, AND 2) in the event of a statement-level
     *                              exception, the parent context needs to be rolled back, too.
     * @param timeoutMillis         timeout value in milliseconds.
     * @param activation            the activation to run.
     * @return the result set to be pawed through
     */
    private ResultSet executeStmt(Activation activation, boolean rollbackParentContext, long timeoutMillis) throws StandardException {
        boolean needToClearSavePoint = false;

        if (activation == null || activation.getPreparedStatement() != this) {
            throw StandardException.newException(SQLState.LANG_WRONG_ACTIVATION, "execute");
        }

        recompileOutOfDatePlan:
        while (true) {
            // verify the activation is for me--somehow.  NOTE: This is
            // different from the above check for whether the activation is
            // associated with the right PreparedStatement - it's conceivable
            // that someone could construct an activation of the wrong type
            // that points to the right PreparedStatement.
            //
            //SanityManager.ASSERT(activation instanceof activationClass, "executing wrong activation");

            /* This is where we set and clear savepoints around each individual
             * statement which needs one.  We don't set savepoints for cursors because
             * they're not needed and they wouldn't work in a read only database.
             * We can't set savepoints for commit/rollback because they'll get
             * blown away before we try to clear them.
             */

            LanguageConnectionContext lccToUse = activation.getLanguageConnectionContext();

            ParameterValueSet pvs = activation.getParameterValueSet();

            /* put it in try block to unlock the PS in any case
             */
            if (!spsAction) {
                // only re-prepare if this isn't an SPS for a trigger-action;
                // if it _is_ an SPS for a trigger action, then we can't just
                // do a regular prepare because the statement might contain
                // internal SQL that isn't allowed in other statements (such as a
                // static method call to get the trigger context for retrieval
                // of "new row" or "old row" values).  So in that case we
                // skip the call to 'rePrepare' and if the statement is out
                // of date, we'll get a NEEDS_COMPILE exception when we try
                // to execute.  That exception will be caught by the executeSPS()
                // method of the GenericTriggerExecutor class, and at that time
                // the SPS action will be recompiled correctly.
                rePrepare(lccToUse);
            }

            StatementContext statementContext = lccToUse.pushStatementContext(
                    isAtomic, updateMode == CursorNode.READ_ONLY, getSource(), pvs, rollbackParentContext, timeoutMillis);

            statementContext.setActivation(activation);

            if (needsSavepoint()) {
                /* Mark this position in the log so that a statement
                * rollback will undo any changes.
                */
                statementContext.setSavePoint();
                needToClearSavePoint = true;
            }

            if (executionConstants != null) {
                lccToUse.validateStmtExecution(executionConstants);
            }

            ResultSet resultSet = null;
            try {

                resultSet = activation.execute();

                resultSet.open();
            } catch (StandardException se) {
                /* Can't handle recompiling SPS action recompile here */
                if (!se.getMessageId().equals(SQLState.LANG_STATEMENT_NEEDS_RECOMPILE) || spsAction) {
                    if (se instanceof ResubmitDistributedException)
                        statementContext.cleanupOnError(se);
                    throw se;
                }
                statementContext.cleanupOnError(se);
                continue;

            }


            if (needToClearSavePoint) {
                /* We're done with our updates */
                statementContext.clearSavePoint();
            }

            lccToUse.popStatementContext(statementContext, null);

            if (activation.isSingleExecution() && resultSet.isClosed()) {
                // if the result set is 'done', i.e. not openable,
                // then we can also release the activation.
                // Note that a result set with output parameters
                // or rows to return is explicitly finished
                // by the user.
                activation.close();
            }

            return resultSet;

        }
    }

    @Override
    public ResultDescription getResultDescription() {
        return resultDesc;
    }

    @Override
    public DataTypeDescriptor[] getParameterTypes() {
        return paramTypeDescriptors;
    }

    @Override
    public String getSource() {
        return (sourceTxt != null) ?
                sourceTxt :
                (statement == null) ? "null" : statement.getSource();
    }

    @Override
    public String getSessionPropertyValues() {
        return (statement == null) ? "null" : statement.getSessionPropertyValues();
    }
    @Override
    public void setSource(String text) {
        sourceTxt = text;
    }

    @Override
    public String getSourceTxt() {
        return sourceTxt;
    }

    public final void setSPSName(String name) {
        spsName = name;
    }

    @Override
    public String getSPSName() {
        return spsName;
    }


    /**
     * Get the total compile time for the associated query in milliseconds.
     * Compile time can be divided into parse, bind, optimize and generate times.
     *
     * @return long        The total compile time for the associated query in milliseconds.
     */
    @Override
    public long getCompileTimeInMillis() {
        return compileTime;
    }

    /**
     * Get the parse time for the associated query in milliseconds.
     *
     * @return long        The parse time for the associated query in milliseconds.
     */
    @Override
    public long getParseTimeInMillis() {
        return parseTime;
    }

    /**
     * Get the bind time for the associated query in milliseconds.
     *
     * @return long        The bind time for the associated query in milliseconds.
     */
    @Override
    public long getBindTimeInMillis() {
        return bindTime;
    }

    /**
     * Get the optimize time for the associated query in milliseconds.
     *
     * @return long        The optimize time for the associated query in milliseconds.
     */
    public long getOptimizeTimeInMillis() {
        return optimizeTime;
    }

    /**
     * Get the generate time for the associated query in milliseconds.
     *
     * @return long        The generate time for the associated query in milliseconds.
     */
    @Override
    public long getGenerateTimeInMillis() {
        return generateTime;
    }

    /**
     * Get the timestamp for the beginning of compilation
     *
     * @return Timestamp    The timestamp for the beginning of compilation.
     */
    @Override
    public Timestamp getBeginCompileTimestamp() {
        return beginCompileTimestamp;
    }

    /**
     * Get the timestamp for the end of compilation
     *
     * @return Timestamp    The timestamp for the end of compilation.
     */
    @Override
    public Timestamp getEndCompileTimestamp() {
        return endCompileTimestamp;
    }

    public void setCompileTimeWarnings(SQLWarning warnings) {
        addWarning(warnings);
    }

    public void addWarning(SQLWarning warning) {
        if (this.warnings == null)
            this.warnings = warning;
        else
            this.warnings.setNextWarning(warning);
    }

    public void clearWarnings() {
        this.warnings = null;
    }

    @Override
    public final SQLWarning getCompileTimeWarnings() {
        return warnings;
    }

    /**
     * Set the compile time for this prepared statement.
     *
     * @param compileTime The compile time
     */
    protected void setCompileTimeMillis(long parseTime, long bindTime,
                                        long optimizeTime,
                                        long generateTime,
                                        long compileTime,
                                        Timestamp beginCompileTimestamp,
                                        Timestamp endCompileTimestamp) {
        this.parseTime = parseTime;
        this.bindTime = bindTime;
        this.optimizeTime = optimizeTime;
        this.generateTime = generateTime;
        this.compileTime = compileTime;
        this.beginCompileTimestamp = beginCompileTimestamp;
        this.endCompileTimestamp = endCompileTimestamp;
    }


    /**
     * Finish marks a statement as totally unusable.
     */
    @Override
    public void finish(LanguageConnectionContext lcc) {
        synchronized (this) {
            inUseCount--;
        }
    }

    /**
     * Set the Execution constants. This routine is called as we Prepare the
     * statement.
     *
     * @param constantAction The big structure enclosing the Execution constants.
     */
    public final void setConstantAction(ConstantAction constantAction) {
        executionConstants = constantAction;
    }


    /**
     * Get the Execution constants. This routine is called at Execution time.
     *
     * @return ConstantAction    The big structure enclosing the Execution constants.
     */
    @Override
    public final ConstantAction getConstantAction() {
        return executionConstants;
    }

    /**
     * Set the saved objects. Called when compilation completes.
     *
     * @param objects The objects to save from compilation
     */
    public final void setSavedObjects(Object[] objects) {
        savedObjects = objects;
    }

    /**
     * Get the specified saved object.
     *
     * @param objectNum The object to get.
     * @return the requested saved object.
     */
    @Override
    public final Object getSavedObject(int objectNum) {
        if (SanityManager.DEBUG) {
            if (!(objectNum >= 0 && objectNum < savedObjects.length))
                SanityManager.THROWASSERT(
                        "request for savedObject entry " + objectNum + " invalid; " +
                                "savedObjects has " + savedObjects.length + " entries");
        }
        return savedObjects[objectNum];
    }

    /**
     * Get the saved objects.
     *
     * @return all the saved objects
     */
    @Override
    public final Object[] getSavedObjects() {
        return savedObjects;
    }

    //
    // Dependent interface
    //

    /**
     * Check that all of the dependent's dependencies are valid.
     *
     * @return true if the dependent is currently valid
     */
    @Override
    public boolean isValid() {
        return isValid;
    }

    /**
     * set this prepared statement to be valid, currently used by GenericTriggerExecutor.
     */
    @Override
    public void setValid() {
        isValid = true;
    }

    /**
     * Indicate this prepared statement is an SPS action, currently used by GenericTriggerExecutor.
     */
    @Override
    public void setSPSAction() {
        spsAction = true;
    }

    /**
     * Prepare to mark the dependent as invalid (due to at least one of
     * its dependencies being invalid).
     *
     * @param action The action causing the invalidation
     * @param p      the provider
     * @throws StandardException thrown if unable to make it invalid
     */
    @Override
    public void prepareToInvalidate(Provider p, int action, LanguageConnectionContext lcc) throws StandardException {

        /*
            this statement can have other open result sets
            if another one is closing without any problems.

            It is not a problem to create an index when there is an open
            result set, since it doesn't invalidate the access path that was
            chosen for the result set.
        */
        switch (action) {
            case DependencyManager.CHANGED_CURSOR:
            case DependencyManager.CREATE_INDEX:
                // Used by activations only:
            case DependencyManager.RECHECK_PRIVILEGES:
                return;
        }

        /* Verify that there are no activations with open result sets
         * on this prepared statement.
         */
        lcc.verifyNoOpenResultSets(this, p, action);
    }

    /**
     * Mark the dependent as invalid (due to at least one of
     * its dependencies being invalid).
     *
     * @param action The action causing the invalidation
     * @throws StandardException Standard Derby error policy.
     */
    @Override
    public void makeInvalid(int action, LanguageConnectionContext lcc) throws StandardException {

        switch (action) {
            case DependencyManager.RECHECK_PRIVILEGES:
                return;
        }

        synchronized (this) {

            if (compilingStatement) {
                // Since the statement is in the process of being compiled,
                // and at the end of the compilation it will set isValid to
                // true and overwrite whatever we set it to here, set another
                // flag to indicate that an invalidation was requested. A
                // re-compilation will be triggered if this flag is set, but
                // not until the current compilation is done.
                invalidatedWhileCompiling = true;
                return;
            }

            // make ourseleves invalid
            isValid = false;

            // block compiles while we are invalidating
            compilingStatement = true;
        }

        try {

            DependencyManager dm = lcc.getDataDictionary().getDependencyManager();

            /* Clear out the old dependencies on this statement as we
             * will build the new set during the reprepare in makeValid().
             */
            dm.clearDependencies(lcc, this);

            /*
            ** If we are invalidating an EXECUTE STATEMENT because of a stale
            ** plan, we also need to invalidate the stored prepared statement.
            */
            switch (action) {
                case DependencyManager.INTERNAL_RECOMPILE_REQUEST:
                case DependencyManager.DROP_TABLE:
                case DependencyManager.TRUNCATE_TABLE:
                case DependencyManager.ALTER_TABLE:
                case DependencyManager.CHANGED_CURSOR: {
                /*
                ** Get the DataDictionary, so we can get the descriptor for
                ** the SPP to invalidate it.
                */
                    DataDictionary dd = lcc.getDataDictionary();
                    SPSDescriptor spsd = null;
                    if (execStmtName != null) {
                        SchemaDescriptor sd = dd.getSchemaDescriptor(execSchemaName, lcc.getTransactionCompile(), true);
                        spsd = dd.getSPSDescriptor(execStmtName, sd);
                        spsd.makeInvalid(action, lcc);
                    }
                    // Remove from cache
                    DataDictionaryCache ddCache = dd.getDataDictionaryCache();
                    if (spsd != null) {
                        ddCache.storedPreparedStatementCacheRemove(spsd);
                    }
                    if (this.statement instanceof GenericStatement) {
                        ddCache.statementCacheRemove(((GenericStatement)this.statement));
                    }
                    break;
                }
            }
        } finally {
            synchronized (this) {
                compilingStatement = false;
                notifyAll();
            }
        }
    }

    /**
     * Is this dependent persistent?  A stored dependency will be required
     * if both the dependent and provider are persistent.
     *
     * @return boolean        Whether or not this dependent is persistent.
     */
    @Override
    public boolean isPersistent() {
        /* Non-stored prepared statements are not persistent */
        return false;
    }

    //
    // Dependable interface
    //

    /**
     * @return the stored form of this Dependable
     */
    @Override
    public DependableFinder getDependableFinder() {
        return null;
    }

    /**
     * Return the name of this Dependable.  (Useful for errors.)
     *
     * @return String    The name of this Dependable..
     */
    @Override
    public String getObjectName() {
        return UUIDString;
    }

    /**
     * Get the Dependable's UUID String.
     *
     * @return String    The Dependable's UUID String.
     */
    @Override
    public UUID getObjectID() {
        return UUIDValue;
    }

    /**
     * Get the Dependable's class type.
     *
     * @return String        Classname that this Dependable belongs to.
     */
    @Override
    public String getClassType() {
        return Dependable.PREPARED_STATEMENT;
    }

    /**
     * Return true if the query node for this statement references SESSION schema
     * tables/views.
     * This method gets called at the very beginning of the compile phase of any statement.
     * If the statement which needs to be compiled is already found in cache, then there is
     * no need to compile it again except the case when the statement is referencing SESSION
     * schema objects. There is a small window where such a statement might get cached
     * temporarily (a statement referencing SESSION schema object will be removed from the
     * cache after the bind phase is over because that is when we know for sure that the
     * statement is referencing SESSION schema objects.)
     *
     * @return true if references SESSION schema tables, else false
     */
    @Override
    public boolean referencesSessionSchema() {
        return referencesSessionSchema;
    }

    /**
     * Return true if the QueryTreeNode references SESSION schema tables/views.
     * The return value is also saved in the local field because it will be
     * used by referencesSessionSchema() method.
     * This method gets called when the statement is not found in cache and
     * hence it is getting compiled.
     * At the beginning of compilation for any statement, first we check if
     * the statement's plan already exist in the cache. If not, then we add
     * the statement to the cache and continue with the parsing and binding.
     * At the end of the binding, this method gets called to see if the
     * QueryTreeNode references a SESSION schema object. If it does, then
     * we want to remove it from the cache, since any statements referencing
     * SESSION schema objects should never get cached.
     *
     * @return true if references SESSION schema tables/views, else false
     */
    public boolean referencesSessionSchema(StatementNode qt) throws StandardException {
        //If the query references a SESSION schema table (temporary or permanent), then
        // mark so in this statement
        referencesSessionSchema = qt.referencesSessionSchema();
        return (referencesSessionSchema);
    }

    //
    // class interface
    //

    /**
     * Makes the prepared statement valid, assigning
     * values for its query tree, generated class,
     * and associated information.
     *
     * @param qt the query tree for this statement
     * @throws StandardException thrown on failure.
     */
    public void completeCompile(StatementNode qt) throws StandardException {
        //if (finished)
        //    throw StandardException.newException(SQLState.LANG_STATEMENT_CLOSED, "completeCompile()");

        paramTypeDescriptors = qt.getParameterTypes();

        // erase cursor info in case statement text changed
        if (targetTable != null) {
            targetTable = null;
            updateMode = 0;
            updateColumns = null;
            targetColumns = null;
        }

        // get the result description (null for non-cursor statements)
        // would we want to reuse an old resultDesc?
        // or do we need to always replace in case this was select *?
        resultDesc = qt.makeResultDescription();

        // would look at resultDesc.getStatementType() but it
        // doesn't call out cursors as such, so we check
        // the root node type instead.

        if (resultDesc != null) {
            /*
                For cursors, we carry around some extra information.
             */
            CursorInfo cursorInfo = (CursorInfo) qt.getCursorInfo();
            if (cursorInfo != null) {
                targetTable = cursorInfo.targetTable;
                targetColumns = cursorInfo.targetColumns;
                updateColumns = cursorInfo.updateColumns;
                updateMode = cursorInfo.updateMode;
            }
        }
        isValid = true;
    }

    @Override
    public GeneratedClass getActivationClass() throws StandardException {
        return activationClass;
    }

    public void setActivationClass(GeneratedClass ac) {
        activationClass = ac;
    }

    //
    // ExecPreparedStatement
    //

    /**
     * the update mode of the cursor
     *
     * @return The update mode of the cursor
     */
    @Override
    public int getUpdateMode() {
        return updateMode;
    }

    /**
     * the target table of the cursor
     *
     * @return target table of the cursor
     */
    @Override
    public ExecCursorTableReference getTargetTable() {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(targetTable != null, "Not a cursor, no target table");
        }
        return targetTable;
    }

    /**
     * the target columns of the cursor as a result column list
     *
     * @return target columns of the cursor as a result column list
     */
    @Override
    public ResultColumnDescriptor[] getTargetColumns() {
        return targetColumns;
    }

    /**
     * the update columns of the cursor as a update column list
     *
     * @return update columns of the cursor as a array of strings
     */
    @Override
    public String[] getUpdateColumns() {
        return updateColumns;
    }

    /**
     * Return the cursor info in a single chunk.  Used by StrorablePreparedStatement
     */
    @Override
    public Object getCursorInfo() {
        return new CursorInfo(updateMode, targetTable, targetColumns, updateColumns);
    }

    public void setCursorInfo(CursorInfo cursorInfo) {
        if (cursorInfo != null) {
            updateMode = cursorInfo.updateMode;
            targetTable = cursorInfo.targetTable;
            targetColumns = cursorInfo.targetColumns;
            updateColumns = cursorInfo.updateColumns;
        }
    }


    //
    // class implementation
    //

    /**
     * Get the byte code saver for this statement.
     * Overridden for StorablePreparedStatement.  We
     * don't want to save anything
     *
     * @return a byte code saver (null for us)
     */
    ByteArray getByteCodeSaver() {
        return null;
    }

    /**
     * Does this statement need a savepoint?
     */
    @Override
    public boolean needsSavepoint() {
        return needsSavepoint;
    }

    /**
     * Set the stmts 'needsSavepoint' state.  Used
     * by an SPS to convey whether the underlying stmt
     * needs a savepoint or not.
     *
     * @param needsSavepoint true if this statement needs a savepoint.
     */
    public void setNeedsSavepoint(boolean needsSavepoint) {
        this.needsSavepoint = needsSavepoint;
    }

    /**
     * Set the stmts 'isAtomic' state.
     *
     * @param isAtomic true if this statement must be atomic (i.e. it is not ok to do a commit/rollback in the middle)
     */
    public void setIsAtomic(boolean isAtomic) {
        this.isAtomic = isAtomic;
    }

    /**
     * Returns whether or not this Statement requires should
     * behave atomically -- i.e. whether a user is permitted
     * to do a commit/rollback during the execution of this
     * statement.
     *
     * @return boolean    Whether or not this Statement is atomic
     */
    @Override
    public boolean isAtomic() {
        return isAtomic;
    }

    /**
     * Set the name of the statement and schema for an "execute statement"command.
     */
    public void setExecuteStatementNameAndSchema(String execStmtName, String execSchemaName) {
        this.execStmtName = execStmtName;
        this.execSchemaName = execSchemaName;
    }

    /**
     * Get a new prepared statement that is a shallow copy of the current one.
     *
     * @return a new prepared statement
     */
    @Override
    public ExecPreparedStatement getClone() throws StandardException {
        GenericPreparedStatement clone = new GenericPreparedStatement(statement);
        shallowClone(clone);
        return clone;
    }

    /* Clone logic shared with sub classes */
    protected void shallowClone(GenericPreparedStatement clone) throws StandardException {
        clone.activationClass = getActivationClass();
        clone.resultDesc = resultDesc;
        clone.paramTypeDescriptors = paramTypeDescriptors;
        clone.executionConstants = executionConstants;
        clone.UUIDString = UUIDString;
        clone.UUIDValue = UUIDValue;
        clone.savedObjects = savedObjects;
        clone.execStmtName = execStmtName;
        clone.execSchemaName = execSchemaName;
        clone.isAtomic = isAtomic;
        clone.sourceTxt = sourceTxt;
        clone.targetTable = targetTable;
        clone.targetColumns = targetColumns;
        clone.updateColumns = updateColumns;
        clone.updateMode = updateMode;
        clone.needsSavepoint = needsSavepoint;
    }

    @Override
    public String toString() {
        return getObjectName();
    }

    public boolean isStorable() {
        return false;
    }

    public void setRequiredPermissionsList(List requiredPermissionsList) {
        this.requiredPermissionsList = requiredPermissionsList;
    }

    @Override
    public List getRequiredPermissionsList() {
        return requiredPermissionsList;
    }

    @Override
    public final long getVersionCounter() {
        return versionCounter;
    }

    public final void incrementVersionCounter() {
        ++versionCounter;
    }

    @Override
    public void setAutoTraced(boolean autoTraced) {
        this.isAutoTraced = autoTraced;
    }

    @Override
    public boolean isAutoTraced() {
        return isAutoTraced;
    }

    @Override
    public boolean hasXPlainTableOrProcedure() {
        return hasXPlainTableOrProcedure;
    }

    @Override
    public void setXPlainTableOrProcedure(boolean val) {
        hasXPlainTableOrProcedure = val;
    }


    public DataSetProcessorType datasetProcessorType() {
        return datasetProcessorType;
    }

    public void setDatasetProcessorType(DataSetProcessorType datasetProcessorType) {
        this.datasetProcessorType = datasetProcessorType;
    }
}
