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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;
import	com.splicemachine.db.catalog.Dependable;
import	com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.execute.HasIncrement;
import com.splicemachine.db.iapi.sql.Row;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.ConnectionContext;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.loader.GeneratedByteCode;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.SQLSessionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.CursorActivation;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.sql.execute.ResultSetFactory;
import com.splicemachine.db.iapi.sql.execute.TemporaryRowHolder;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.StringDataValue;

/**
 * BaseActivation
 * provides the fundamental support we expect all activations to have.
 * Doesn't actually implement any of the activation interface,
 * expects the subclasses to do that.
 */
public abstract class BaseActivation implements CursorActivation, GeneratedByteCode {
//    private static final Logger LOG = Logger.getLogger(BaseActivation.class);
	private	LanguageConnectionContext	lcc;
	protected ContextManager			cm;
	protected DataValueFactory dvf;
    public boolean ignoreSequence = false;
	protected boolean materialized = false;

	protected ExecPreparedStatement preStmt;
	protected ResultSet resultSet;
	protected ResultDescription resultDescription;
	protected boolean closed;
	private String cursorName;
	
	protected int numSubqueries;
    protected boolean useSpark = false;

	private boolean singleExecution;

	// This flag is declared volatile to ensure it is 
	// visible when it has been modified by the finalizer thread.
	private volatile boolean inUse;

	private java.sql.ResultSet targetVTI;
	private SQLWarning warnings;

	private GeneratedClass gc;	// my Generated class object.

	private static final long MAX_SQRT = (long) Math.sqrt(Long.MAX_VALUE);

	// When the row count exceeds this number, we should recompile if
	// the difference in row counts is greater than 10%.  If it's less
	// than this number, we use an entirely different technique to check
	// for recompilation.  See comments below, in informOfRowCount()
	private static final int TEN_PERCENT_THRESHOLD = 400;

	/* Performance optimization for update/delete - only
	 * open heap ConglomerateController once when doing
	 * index row to base row on search
	 */
	private ConglomerateController  updateHeapCC;
	private ScanController			indexSC;
	private long					indexConglomerateNumber = -1;

	private TableDescriptor ddlTableDescriptor;

	private int maxRows = -1;
	private boolean			forCreateTable;

	private boolean			scrollable;

  	private boolean resultSetHoldability;

	//beetle 3865: updateable cursor using index.  A way of communication
	//between cursor activation and update activation.
	private CursorResultSet forUpdateIndexScan;

	//Following three are used for JDBC3.0 auto-generated keys feature.
	//autoGeneratedKeysResultSetMode will be set true if at the time of statement execution,
	//either Statement.RETURN_GENERATED_KEYS was passed or an array of (column positions or
	//column names) was passed
	private boolean autoGeneratedKeysResultSetMode;
	private int[] autoGeneratedKeysColumnIndexes ;
	private String[] autoGeneratedKeysColumnNames ;

	/**
	 * By setting isValid to false, we can force a new activation to be used
	 * even if the prepared statement is still valid. This is used when
	 * modifying the current role for a session, which may (if the statement
	 * relies on privileges obtained via the current role) require rechecking
	 * of privileges. The checking normally only happens the first time the
	 * prepared statement is used in a session, when the activation is
	 * constructed. Forcing creation of a new activation achieves the purpose
	 * of getting the check performed over again and is cheaper than
	 * invalidating the prepared statement itself. Also, the latter would
	 * impact other sessions, forcing them to recreate their activations.
	 */
	private boolean isValid;

	/**
	 * For dependency tracking
	 */
	protected String UUIDString;

	/**
	 * For dependency tracking
	 */
	protected UUID   UUIDValue;

	/**
	 * The 'parentActivation' of an activation of a statement executing in
	 * the root connection is null.
	 *
	 * A non-null 'parentActivation' represents the activation of the calling
	 * statement (if we are in a nested connection of a stored routine), or the
	 * activation of the parent statement (if we are executing a substatement)
	 *
	 * 'parentActivation' is set when this activation is created (@see
	 * GenericPreparedStatement#getActivation) based on the top of the
	 * dynamic call stack of execution, which is tracked by
	 * StatementContext. The nested SQL session context is initialized
	 * by code generated for the call, after parameters are evaluated
	 * or just substatement execution starts.
	 * @see com.splicemachine.db.impl.sql.GenericPreparedStatement#executeSubStatement
	 *
	 */
	private Activation parentActivation;

	/**
	 * The SQL session context to be used inside a nested connection in a
	 * stored routine or in a substatement. In the latter case, it is an alias
	 * to the superstatement's session context.
	 */
	private SQLSessionContext sqlSessionContextForChildren;

	//Following is the position of the session table names list in savedObjects in compiler context
	//This is updated to be the correct value at cursor generate time if the cursor references any session table names.
	//If the cursor does not reference any session table names, this will stay negative
	protected int indexOfSessionTableNamesInSavedObjects = -1;

	// WARNING: these fields are accessed by code generated in the 
	// ExpressionClassBuilder: don't change them unless you 
	// make the appropriate changes there.
	protected ExecRow[] row;
	protected ParameterValueSet pvs;

    protected ExecRow scanStartOverride;
    protected ExecRow scanStopOverride;
	protected int[] scanKeys;

	private long numRowsSeen = 0l;
	//
	// constructors
	//

    public boolean ignoreSequence() {
        return ignoreSequence;
    }

    public void setIgnoreSequence(boolean ignoreSequence) {
        this.ignoreSequence = ignoreSequence;
    }

    protected BaseActivation()
	{
		super();
	}

		public long getRowsSeen() {
				return numRowsSeen;
		}

		public void addRowsSeen(long rowsSeen) {
			this.numRowsSeen+=rowsSeen;
		}

		public final void initFromContext(Context context)
		throws StandardException {

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(context!=null, "NULL context passed to BaseActivation.initFromContext");
		}
		this.cm = context.getContextManager();

		lcc = (LanguageConnectionContext) cm.getContext(LanguageConnectionContext.CONTEXT_ID);

		dvf = lcc.getDataValueFactory();
		
		if (SanityManager.DEBUG) {
			if (lcc == null)
				SanityManager.THROWASSERT("lcc is null in activation type " + getClass());
		}

		// mark in use
		inUse = true;
		
		// add this activation to the pool for the connection.
		lcc.addActivation(this);

		isValid = true;

		/* Get the UUID for this activation */
		UUIDFactory uuidFactory =
			Monitor.getMonitor().getUUIDFactory();

		UUIDValue = uuidFactory.createUUID();
		UUIDString = UUIDValue.toString();
	}


	//
	// Activation interface
	//

	public final ExecPreparedStatement getPreparedStatement() {
		return preStmt;
	}

	public ConstantAction getConstantAction() {
		return preStmt.getConstantAction();
	}


	public final void checkStatementValidity() throws StandardException {

		if (preStmt == null || preStmt.upToDate(gc))
			return;

		StandardException se = StandardException.newException(SQLState.LANG_STATEMENT_NEEDS_RECOMPILE);
		se.setReport(StandardException.REPORT_NEVER);
		throw se;
	}

	/**
		Link this activation with its PreparedStatement.
		It can be called with null to break the link with the
		PreparedStatement.

	*/
	public final void setupActivation(ExecPreparedStatement ps, boolean scrollable) 
	throws StandardException {
		preStmt = ps;
				
		if (ps != null) {
			// get the result set description
   			resultDescription = ps.getResultDescription();
			this.scrollable = scrollable;
			
			// Initialize the parameter set to have allocated
			// DataValueDescriptor objects for each parameter.
			if (pvs != null && pvs.getParameterCount() != 0)
				pvs.initialize(ps.getParameterTypes());

		} else {
			resultDescription = null;
			this.scrollable = false;
		}
	}

	public ResultSet getResultSet() {
		return resultSet;
	}

	/**
		Get the saved RowLocation.

		@param itemNumber	The saved item number.

		@return	A RowLocation template for the conglomerate
	 */
	public RowLocation getRowLocationTemplate(int itemNumber)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(itemNumber >= 0,
				"itemNumber expected to be >= 0");
			if (! (getPreparedStatement().getSavedObject(itemNumber) instanceof RowLocation))
			{
				SanityManager.THROWASSERT(
					"getPreparedStatement().getSavedObject(itemNumber) expected to be " +
					"instance of RowLocation, not " +
					getPreparedStatement().getSavedObject(itemNumber).getClass().getName() +
					", query is " + getPreparedStatement().getSource());
			}
        }
        RowLocation rl = (RowLocation)
                getPreparedStatement().getSavedObject(itemNumber);
        /* We have to return a clone of the saved RowLocation due
         * to the shared cache of SPSs.
         */
        Object rlClone = rl.cloneValue(false);
        if (SanityManager.DEBUG) {
            if (! (rlClone instanceof RowLocation))
			{
				SanityManager.THROWASSERT(
                    "rl.getClone() expected to be " +
                    "instance of RowLocation, not " +
                    rlClone.getClass().getName() + ", query is " +
                    getPreparedStatement().getSource());
			}
		}
        return (RowLocation)rlClone;
	}

	/*
	 */
	public ResultDescription getResultDescription() {
		if (SanityManager.DEBUG)
	    	SanityManager.ASSERT(resultDescription != null, "Must have a result description");
	   	    return resultDescription;
	}

	/**
		This is a partial implementation of reset.
		Subclasses will want to reset information
		they are aware of, such as parameters.
		<p>
		All subclasses must call super.reset() and
		then do their cleanup.
		<p>
		The execute call must set the resultSet field
		to be the resultSet that it has returned.

		@exception StandardException on error
	 */
	public void reset() throws StandardException
	{
		if (resultSet != null) 
			resultSet.close();
		
		updateHeapCC = null;
		// REMIND: do we need to get them to stop input as well?

		if (!isSingleExecution())
			clearWarnings();
	}

	/**
		Closing an activation marks it as unusable. Any other
		requests made on it will fail.  An activation should be
		marked closed when it is expected to not be used any longer,
		i.e. when the connection for it is closed, or it has suffered some
		sort of severe error.

		This should also remove it from the language connection context.

		@exception StandardException on error
	 */
	public final void close() throws StandardException 
	{
		if (! closed) {	
			
			// markUnused();

			// we call reset so that if the actual type of "this"
			// is a subclass of BaseActivation, its cleanup will
			// also happen -- reset in the actual type is called,
			// not reset in BaseActivation.  Subclass reset's
			// are supposed to call super.reset() as well.
			reset(); // get everything related to executing released

			if (resultSet != null)
			{
				// Finish the resultSet, it will never be used again.
				resultSet.finish();
				resultSet = null;
			}

			closed = true;

			LanguageConnectionContext lcc = getLanguageConnectionContext();

            // Remove all the dependencies this activation has. It won't need
            // them after it's closed, so let's free up the memory in the
            // dependency manager. (DERBY-4571)
            DependencyManager dm =
                    lcc.getDataDictionary().getDependencyManager();
            dm.clearDependencies(lcc, this);

			lcc.removeActivation(this);
			if (preStmt != null) {
				preStmt.finish(lcc);
				preStmt = null;
			}

			try {
				closeActivationAction();
			} catch (Throwable e) {
				throw StandardException.plainWrapException(e);
			}

		}
		
	}

	/**
		A generated class can create its own closeActivationAction
		method to invoke special logic when the activation is closed.
	*/
	protected void closeActivationAction() throws Exception {
		// no code to be added here as generated code
		// will not call super.closeActivationAction()
	}

	/**
		Find out if the activation closed or not.
		@return true if the prepared statement has been closed.
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
		Set this Activation for a single execution.

		@see Activation#setSingleExecution
	*/
	public void setSingleExecution() {
		singleExecution = true;
	}

	/**
		Returns true if this Activation is only going to be used for
		one execution.

		@see Activation#isSingleExecution
	*/
	public boolean isSingleExecution() {
		return singleExecution;
	}

	/**
		Get the number of subqueries in the entire query.
		@return int	 The number of subqueries in the entire query.
	 */
	public int getNumSubqueries() {
		return numSubqueries;
	}

    public boolean useSpark() {
        return useSpark;
    }

	/**
	 * @see Activation#isCursorActivation
	 */
	public boolean isCursorActivation()
	{
		return false;
	}

	//
	// GeneratedByteCode interface
	//

	public final void setGC(GeneratedClass gc) {
		this.gc = gc;
	}

	public final GeneratedClass getGC() {

		if (SanityManager.DEBUG) {
			if (gc == null)
				SanityManager.THROWASSERT("move code requiring GC to postConstructor() method!!");
		}
		return gc;
	}

	public final GeneratedMethod getMethod(String methodName) throws StandardException {

		return getGC().getMethod(methodName);
	}
	public Object e0() throws StandardException { return null; } 
	public Object e1() throws StandardException { return null; }
	public Object e2() throws StandardException { return null; }
	public Object e3() throws StandardException { return null; }
	public Object e4() throws StandardException { return null; } 
	public Object e5() throws StandardException { return null; }
	public Object e6() throws StandardException { return null; }
	public Object e7() throws StandardException { return null; }
	public Object e8() throws StandardException { return null; } 
	public Object e9() throws StandardException { return null; }

    public String e0ToString() throws StandardException { return null; }
    public String e1ToString() throws StandardException { return null; }
    public String e2ToString() throws StandardException { return null; }
    public String e3ToString() throws StandardException { return null; }
    public String e4ToString() throws StandardException { return null; }
    public String e5ToString() throws StandardException { return null; }
    public String e6ToString() throws StandardException { return null; }
    public String e7ToString() throws StandardException { return null; }
    public String e8ToString() throws StandardException { return null; }
    public String e9ToString() throws StandardException { return null; }
	//
	// class interface
	//

	/**
	 * Temporary tables can be declared with ON COMMIT DELETE ROWS. But if the table has a held curosr open at
	 * commit time, data should not be deleted from the table. This method, (gets called at commit time) checks if this
	 * activation held cursor and if so, does that cursor reference the passed temp table name.
	 *
	 * @return	true if this activation has held cursor and if it references the passed temp table name
	 */
	public boolean checkIfThisActivationHasHoldCursor(String tableName)
	{
		if (!inUse)
			return false;

		if (resultSetHoldability == false) //if this activation is not held over commit, do not need to worry about it
			return false;

		if (indexOfSessionTableNamesInSavedObjects == -1) //if this activation does not refer to session schema tables, do not need to worry about it
			return false;

		/* is there an open result set? */
		if ((resultSet != null) && !resultSet.isClosed() && resultSet.returnsRows())
		{
			//If we came here, it means this activation is held over commit and it reference session table names
			//Now let's check if it referneces the passed temporary table name which has ON COMMIT DELETE ROWS defined on it.
			return ((ArrayList)getPreparedStatement().getSavedObject(indexOfSessionTableNamesInSavedObjects)).contains(tableName);
		}

		return false;
	}

	/**
	   remember the cursor name
	 */

	public void	setCursorName(String cursorName)
	{
		if (isCursorActivation())
			this.cursorName = cursorName;
	}


	/**
	  get the cursor name.  For something that isn't
	  a cursor, this is used as a string name of the
	  result set for messages from things like the
	  dependency manager.
	  <p>
	  Activations that do support cursors will override
	  this.	
	*/
	public String getCursorName() {

		return isCursorActivation() ? cursorName : null;
	}

	public void setResultSetHoldability(boolean resultSetHoldability)
	{
		this.resultSetHoldability = resultSetHoldability;
	}

	public boolean getResultSetHoldability()
	{
		return resultSetHoldability;
	}

	/** @see Activation#setAutoGeneratedKeysResultsetInfo */
	public void setAutoGeneratedKeysResultsetInfo(int[] columnIndexes, String[] columnNames)
	{
		autoGeneratedKeysResultSetMode = true;
		autoGeneratedKeysColumnIndexes = columnIndexes;
		autoGeneratedKeysColumnNames = columnNames;
	}

	/** @see Activation#getAutoGeneratedKeysResultsetMode */
	public boolean getAutoGeneratedKeysResultsetMode()
	{
		return autoGeneratedKeysResultSetMode;
	}

	/** @see Activation#getAutoGeneratedKeysColumnIndexes */
	public int[] getAutoGeneratedKeysColumnIndexes()
	{
		return autoGeneratedKeysColumnIndexes;
	}

	/** @see Activation#getAutoGeneratedKeysColumnNames */
	public String[] getAutoGeneratedKeysColumnNames()
	{
		return autoGeneratedKeysColumnNames;
	}

	//
	// class implementation
	//


	/**
		Used in the execute method of activations for
		generating the result sets that they concatenate together.
	 */
	public final ResultSetFactory getResultSetFactory() throws StandardException {
		return getExecutionFactory().getResultSetFactory();
	}

	/**
		Used in activations for generating rows.
	 */
	public final ExecutionFactory getExecutionFactory() {
		return getLanguageConnectionContext().
            getLanguageConnectionFactory().getExecutionFactory();
	}


	/**
		Used in CurrentOfResultSet to get to the target result set
		for a cursor. Overridden by activations generated for
		updatable cursors.  Those activations capture the target
		result set in a field in their execute() method, and then
		return the value of that field in their version of this method.

		@return null.
	 */
	public CursorResultSet getTargetResultSet() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("Must be overridden to be used.");
		return null;
	}

	/**
	 * Called by generated code to compute the next autoincrement value.
	 * 
	 * @return The next autoincrement value which should be inserted.
	 * returns the correct number datatype.
	 */
	protected DataValueDescriptor 
		getSetAutoincrementValue(int columnPosition, long increment)
	       throws StandardException
	{
		return ((HasIncrement)resultSet).increment(columnPosition, increment);

	}

	/**
	 * Called by generated code to get the next number in an ANSI/ISO sequence
     * and advance the sequence. Raises an exception if the sequence was declared
     * NO CYCLE and its range is exhausted.
	 *
     * @param sequenceUUIDstring The string value of the sequence's UUID
     * @param typeFormatID The format id of the data type to be returned. E.g., StoredFormatIds.SQL_INTEGER_ID.
     *
	 * @return The next number in the sequence
	 */
	protected NumberDataValue getCurrentValueAndAdvance
        ( String sequenceUUIDstring, int typeFormatID )
	       throws StandardException
	{
//        ContextManager contextManager = ContextService.getFactory().getCurrentContextManager();
        try {
            cm.setActiveThread();
            cm.pushContext(lcc);
            cm.pushContext(lcc.getLanguageConnectionFactory().getExecutionFactory().newExecutionContext(cm));
            ContextService.getFactory().setCurrentContextManager(cm);

            NumberDataValue ndv = (NumberDataValue) getDataValueFactory().getNull(typeFormatID, StringDataValue.COLLATION_TYPE_UCS_BASIC);
            lcc.getDataDictionary().getCurrentValueAndAdvance(sequenceUUIDstring, ndv,
					true); // Hard Coded Batch since single lookups are too slow...
            return ndv;
        }
        finally {
            ContextService.getFactory().resetCurrentContextManager(cm);
        }
    }

	/**
		Used in CurrentOfResultSet to get to the cursor result set
		for a cursor.  Overridden by activations generated for
		updatable cursors.  Those activations capture the cursor
		result set in a field in their execute() method, and then
		return the value of that field in their version of this method.

		@return null
	 */
	public CursorResultSet getCursorResultSet() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("Must be overridden to be used.");
		return null;
	}

	/**
		Various activation methods need to disallow their
		invocation if the activation is closed. This lets them
		check and throw without generating alot of code.
		<p>
		The code to write to generate the call to this is approximately:
		<verbatim>
			// jf is a JavaFactory
			CallableExpression ce = jf.newMethodCall(
				jf.thisExpression(),
				BaseActivation.CLASS_NAME,
				"throwIfClosed",
				"void",
				acb.exprArray(jf.newStringLiteral(...some literal here...)));

			//mb is a MethodBuilder
			mb.addStatement(jf.newStatement(ce));
		</verbatim>
		The java code to write to call this is:
		<verbatim>
			this.throwIfClosed(...some literal here...);
		</verbatim>
		In both cases, "...some literal here..." gets replaced with
		an expression of type String that evaluates to the name
		of the operation that is being checked, like "execute" or
		"reset".

		@exception StandardException thrown if closed
	 */
	public void throwIfClosed(String op) throws StandardException {
		if (closed)
			throw StandardException.newException(SQLState.LANG_ACTIVATION_CLOSED, op);
	}

	/**
	 * Set a column position in an array of column positions.
	 *
	 * @param columnPositions	The array of column positions
	 * @param positionToSet		The place to put the column position
	 * @param column			The column position
	 */
	public static void setColumnPosition(
							int[] columnPositions,
							int positionToSet,
							int column) {
//		if (LOG.isTraceEnabled())
//			LOG.trace(String.format("setColumnPosition: columnPositions=%s, positionToSet=%d, column=%d",Arrays.toString(columnPositions),positionToSet, column));
		columnPositions[positionToSet] = column;
	}

	/**
	 * Allocate an array of qualifiers and initialize in Qualifier[][]
	 *
	 * @param qualifiers	The array of Qualifier arrays.
	 * @param position		The position in the array to set
	 * @param length		The array length of the qualifier array to allocate.
	 */
	public static void allocateQualArray(
    Qualifier[][]   qualifiers,
    int             position,
    int             length)
	{
        qualifiers[position] = new Qualifier[length];
	}


	/**
	 * Set a Qualifier in a 2 dimensional array of Qualifiers.
     *
     * Set a single Qualifier into one slot of a 2 dimensional array of 
     * Qualifiers.  @see Qualifier for detailed description of layout of
     * the 2-d array.
	 *
	 * @param qualifiers	The array of Qualifiers
	 * @param qualifier		The Qualifier
	 * @param position_1    The Nth array index into qualifiers[N][M]
	 * @param position_2    The Nth array index into qualifiers[N][M]
	 */
	public static void setQualifier(
    Qualifier[][]   qualifiers,
    Qualifier	    qualifier,
    int			    position_1,
    int             position_2)
	{
		qualifiers[position_1][position_2] = qualifier;
	}

	/**
	 * Reinitialize all Qualifiers in an array of Qualifiers.
	 *
	 * @param qualifiers	The array of Qualifiers
	 */
	public static void reinitializeQualifiers(Qualifier[][] qualifiers)
	{
		if (qualifiers != null)
		{
            for (int term = 0; term < qualifiers.length; term++)
            {
                for (int i = 0; i < qualifiers[term].length; i++)
                {
                    qualifiers[term][i].reinitialize();
                }
            }
		}
	}

	/**
	 * Mark the activation as unused.  
	 */
	public final void markUnused()
	{
		if(isInUse()) {
			inUse = false;
			lcc.notifyUnusedActivation();
		}
	}

	/**
	 * Is the activation in use?
	 *
	 * @return true/false
	 */
	public final boolean isInUse()
	{
		return inUse;
	}

	/**
	  @see com.splicemachine.db.iapi.sql.Activation#addWarning
	  */
	public void addWarning(SQLWarning w)
	{
		if (warnings == null)
			warnings = w;
		else
			warnings.setNextWarning(w);
	}

	/**
	  @see com.splicemachine.db.iapi.sql.Activation#getWarnings
	  */
	public SQLWarning getWarnings()
	{
		return warnings;
	}

	/**
	  @see com.splicemachine.db.iapi.sql.Activation#clearWarnings
	  */
	public void clearWarnings()
	{
		warnings = null;
	}

	/**
	 * @exception StandardException on error
	 */
	protected static void nullToPrimitiveTest(DataValueDescriptor dvd, String primitiveType)
		throws StandardException
	{
		if (dvd==null || dvd.isNull())
		{
			throw StandardException.newException(SQLState.LANG_NULL_TO_PRIMITIVE_PARAMETER, primitiveType);
		}
	}


	/**
	 * The subclass calls this method when it begins an execution.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void startExecution() throws StandardException {

	}

	/**
	 * @see Activation#getHeapConglomerateController
	 */
	public ConglomerateController getHeapConglomerateController()
	{
		return updateHeapCC;
	}


	/**
	 * @see Activation#setHeapConglomerateController
	 */
	public void setHeapConglomerateController(ConglomerateController updateHeapCC)
	{
		this.updateHeapCC = updateHeapCC;
	}

	/**
	 * @see Activation#clearHeapConglomerateController
	 */
	public void clearHeapConglomerateController()
	{
		updateHeapCC = null;
	}

	/**
	 * @see Activation#getIndexScanController
	 */
	public ScanController getIndexScanController()
	{
		return indexSC;
	}

	/**
	 * @see Activation#setIndexScanController
	 */
	public void setIndexScanController(ScanController indexSC)
	{
		this.indexSC = indexSC;
	}

	/**
	 * @see Activation#getIndexConglomerateNumber
	 */
	public long getIndexConglomerateNumber()
	{
		return indexConglomerateNumber;
	}

	/**
	 * @see Activation#setIndexConglomerateNumber
	 */
	public void setIndexConglomerateNumber(long indexConglomerateNumber)
	{
		this.indexConglomerateNumber = indexConglomerateNumber;
	}

	/**
	 * @see Activation#clearIndexScanInfo
	 */
	public void clearIndexScanInfo()
	{
		indexSC = null;
		indexConglomerateNumber = -1;
	}

	/**
	 * @see Activation#setForCreateTable()
	 */
	public void setForCreateTable()
	{
		forCreateTable = true;
	}

	/**
	 * @see Activation#getForCreateTable()
	 */
	public boolean getForCreateTable()
	{
		return forCreateTable;
	}

	/**
	 * @see Activation#setDDLTableDescriptor
	 */
	public void setDDLTableDescriptor(TableDescriptor td)
	{
		ddlTableDescriptor = td;
	}

	/**
	 * @see Activation#getDDLTableDescriptor
	 */
	public TableDescriptor getDDLTableDescriptor()
	{
		return ddlTableDescriptor;
	}

	/**
	 * @see Activation#setMaxRows
	 */
	public void setMaxRows(int maxRows)
	{
		this.maxRows = maxRows;
	}

	/**
	 * @see Activation#getMaxRows
	 */
	public int getMaxRows()
	{
		return maxRows;
	}

	public void setTargetVTI(java.sql.ResultSet targetVTI)
	{
		this.targetVTI = targetVTI;
	}

	public java.sql.ResultSet getTargetVTI()
	{
		return targetVTI;
	}

	/*
	** These accessor methods are provided by the sub-class to help figure
	** out whether to check row counts during this execution.
	*/
	abstract protected int getExecutionCount();

	abstract protected void setExecutionCount(int newValue); 

	/*
	** These accessor methods are provided by the sub-class to help figure
	** out whether the row count for a particular result set has changed
	** enough to force recompilation.
	*/
	abstract protected Vector getRowCountCheckVector();

	abstract protected void setRowCountCheckVector(Vector newValue);

	/*
	** These accessor methods are provided by the sub-class to remember the
	** value of the stale plan check interval property, so that we only
	** have to query the database properties once (there is heavyweight
	** synchronization around the database properties).
	*/
	abstract protected int getStalePlanCheckInterval();

	abstract protected void setStalePlanCheckInterval(int newValue);

	public final boolean getScrollable() {
		return scrollable;
	}

	protected final void setParameterValueSet(int paramCount, boolean hasReturnParam) {

		pvs = lcc.getLanguageFactory().newParameterValueSet(
			lcc.getLanguageConnectionFactory().getClassFactory().getClassInspector(),
			paramCount, hasReturnParam);
		}
	
	/**
	 * This method can help reduce the amount of generated code by changing
	 * instances of this.pvs.getParameter(position) to this.getParameter(position) 
	 * @param position
	 * @throws StandardException
	 */
	protected final DataValueDescriptor getParameter(int position) throws StandardException { 
		return pvs.getParameter(position); 
		} 
	
	/**
	 return the parameters.
	 */
	public ParameterValueSet	getParameterValueSet() 
	{ 
		if (pvs == null)
			setParameterValueSet(0, false); 
		return pvs; 
	}

	// how do we do/do we want any sanity checking for
	// the number of parameters expected?
	public void	setParameters(ParameterValueSet parameterValues, DataTypeDescriptor[] parameterTypes) throws StandardException
	{
		if (!isClosed())
		{

			if (this.pvs == null || parameterTypes == null) {
				pvs = parameterValues;
				return;

			}

			DataTypeDescriptor[]	newParamTypes = preStmt.getParameterTypes();

			/*
			** If there are old parameters but not new ones,
			** they aren't compatible.
			*/
			boolean match = false;
			if (newParamTypes != null) {

				if (newParamTypes.length == parameterTypes.length) {

					/* Check each parameter */
					match = true;
					for (int i = 0; i < parameterTypes.length; i++)
					{
						DataTypeDescriptor	oldType = parameterTypes[i];
						DataTypeDescriptor	newType	= newParamTypes[i];

						if (!oldType.isExactTypeAndLengthMatch(newType)) {
							match = false;
							break;
						}
						/*
						** We could probably get away without checking nullability,
						** since parameters are always nullable.
						*/
						if (oldType.isNullable() != newType.isNullable()) {
							match = false;
							break;
						}
					}
				}

			}

			if (!match)
				throw StandardException.newException(SQLState.LANG_OBSOLETE_PARAMETERS);


			parameterValues.transferDataValues(pvs);

		}
		else if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("isClosed() is expected to return false");
		}
	}

	/**
	 	Throw an exception if any parameters are uninitialized.

		@exception StandardException	Thrown if any parameters
												are unitialized
	 */

	public void throwIfMissingParms() throws StandardException
	{
		if (pvs != null && !pvs.allAreSet())
		{
			throw StandardException.newException(SQLState.LANG_MISSING_PARMS);
		}
	}

	/**
	 * Remember the row for the specified ResultSet.
	 */
	public void setCurrentRow(ExecRow currentRow, int resultSetNumber) {
//		if (LOG.isTraceEnabled())
//			LOG.trace(String.format("setCurrentRow: currentRow=%s, resultSetNumber=%d",currentRow,resultSetNumber));
		if (row != null) {
			row[resultSetNumber] = currentRow;
		}
	}

	/**
	 * Clear the current row for the specified ResultSet.
	 */
	public void clearCurrentRow(int resultSetNumber) {
//		if (LOG.isTraceEnabled())
//			LOG.trace(String.format("clearCurrentRow: resultSetNumber=%d",resultSetNumber));
		if (SanityManager.DEBUG)
		{
			if (row != null)
			{
				if (!(resultSetNumber >=0 && resultSetNumber < row.length))
				{
					SanityManager.THROWASSERT("resultSetNumber = " + resultSetNumber +
								 ", expected to be between 0 and " + row.length);
				}
			}
		}
		if (row != null)
		{
			row[resultSetNumber] = null;
		}
	}

	/**
	 * Get the current row at the given index.
	 */
	public Row getCurrentRow(int resultSetNumber) {
//		if (LOG.isTraceEnabled())
//			LOG.trace(String.format("getCurrentRow: resultSetNumber=%d",resultSetNumber));
        return row[resultSetNumber];
	}

	/**
	 * Return the current SQL session context for all immediately
	 * nested connections stemming from the call or function
	 * invocation of the statement corresponding to this activation.
     * <p/>
     * Substatements (e.g. used in rs.updateRow), inherit the SQL session
     * context via its parent activation.
	 * @see com.splicemachine.db.iapi.sql.Activation#getSQLSessionContextForChildren
	 */
	public SQLSessionContext getSQLSessionContextForChildren() {
        SQLSessionContext sessionContext = this.sqlSessionContextForChildren;
        if (sessionContext == null) {
            // if child session context not existent here, try parent.
			sessionContext = setupSQLSessionContextForChildren(false);
        }
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT
				(sessionContext != null,
				 "Expected sqlSessionContextForChildren to be non-null");
		}

		return sessionContext;
	}

	/**
	 * @see com.splicemachine.db.iapi.sql.Activation#setupSQLSessionContextForChildren
	 */
	public SQLSessionContext setupSQLSessionContextForChildren(boolean push) {

		if (push) {
			// Nested connection, so need to push a new context: SQL 2003,
			// 4.37.1: "An SQL-session is associated with an
			// SQL-connection.
			sqlSessionContextForChildren = lcc.createSQLSessionContext();
		} else {
			// Substatement, so use current one
			if (parentActivation != null) {
				// The parent statement performing the substatement is
				// itself inside a nested connection (stored routine)
				sqlSessionContextForChildren =
					parentActivation.getSQLSessionContextForChildren();
			} else {
				// The parent statement performing the substatement is on
				// top level
				sqlSessionContextForChildren =
					lcc.getTopLevelSQLSessionContext();
			}
		}

		return sqlSessionContextForChildren;
	}

	/**
	 * This activation is created in a dynamic call context or a substatement
	 * execution context, make note of its parent statements activation (a).
	 *
	 * @param a The caller's or superstatement's activation
	 */
	public void setParentActivation(Activation a) {
		parentActivation = a;
	}

	/**
	 * Get the activation of the calling statement or parent statement.
	 *
	 * @return The parent's activation
	 */
	public Activation getParentActivation() {
		return parentActivation;
	}


	protected final DataValueDescriptor getColumnFromRow(int rsNumber, int colId)
		throws StandardException {
//		if (LOG.isTraceEnabled())
//			LOG.trace(String.format("getColumnFromRow: resultSetNumber=%d, colId=%d",rsNumber, colId));

        if (row[rsNumber] == null) {
            /* This actually happens. NoPutResultSetImpl.clearOrderableCache
             * attempts to prefetch invariant values into a cache. This fails
             * in some deeply nested joins. See Beetle 4736 and 4880.*/

            /*
             * Update: DERBY-4798 shows a query for which we get an NPE unless
             * this escape is in place (once removed by DERBY-3097, but
             * reintroduced by DERBY-4798 until we understand how we can get
             * rid of this anomaly). Thus, for now,
             * OuterJoinTest#testDerby_4798_NPE will provoke an NPE if this
             * code is removed.
             */
//    		if (LOG.isTraceEnabled())
//    			LOG.trace("getColumnFromRow (deeply nested join issue");
            return null;
        }

        return row[rsNumber].getColumn(colId);
	}

    /**
     * Check that a positioned statement is executing against a cursor
     * from the same PreparedStatement (plan) that the positioned
     * statement was original compiled against.
     * 
     * Only called from generated code for positioned UPDATE and DELETE
     * statements. See CurrentOfNode.
     * 
     * @param cursorName Name of the cursor
     * @param psName Object name of the PreparedStatement.
     * @throws StandardException
     */
	protected void checkPositionedStatement(String cursorName, String psName)
		throws StandardException {

		ExecPreparedStatement ps = getPreparedStatement();
		if (ps == null)
			return;
			
		LanguageConnectionContext lcc = getLanguageConnectionContext();

		CursorActivation cursorActivation = lcc.lookupCursorActivation(cursorName);

		if (cursorActivation != null)
		{
			// check we are compiled against the correct cursor
			if (!psName.equals(cursorActivation.getPreparedStatement().getObjectName())) {

				// our prepared statement is now invalid since there
				// exists another cursor with the same name but a different
				// statement.
				ps.makeInvalid(DependencyManager.CHANGED_CURSOR, lcc);
			}
		}
	}

	/* This method is used to materialize a resultset if can actually fit in the memory
	 * specified by "maxMemoryPerTable" system property.  It converts the result set into
	 * union(union(union...(union(row, row), row), ...row), row).  It returns this
	 * in-memory converted resultset, or the original result set if not converted.
	 * See beetle 4373 for details.
	 *
	 * Optimization implemented as part of Beetle: 4373 can cause severe stack overflow
	 * problems. See JIRA entry DERBY-634. With default MAX_MEMORY_PER_TABLE of 1MG, it is
	 * possible that this optimization could attempt to cache upto 250K rows as nested
	 * union results. At runtime, this would cause stack overflow.
	 *
	 * As Jeff mentioned in DERBY-634, right way to optimize original problem would have been
	 * to address subquery materialization during optimization phase, through hash joins.
	 * Recent Army's optimizer work through DEBRY-781 and related work introduced a way to
	 * materialize subquery results correctly and needs to be extended to cover this case.
	 * While his optimization needs to be made more generic and stable, I propose to avoid
	 * this regression by limiting size of the materialized resultset created here to be
	 * less than MAX_MEMORY_PER_TABLE and MAX_DYNAMIC_MATERIALIZED_ROWS.
	 *
	 *	@param	rs	input result set
	 *	@return	materialized resultset, or original rs if it can't be materialized
	 */
	public NoPutResultSet materializeResultSetIfPossible(NoPutResultSet rs, int resultSetNumber)
		throws StandardException
	{
		int maxMemoryPerTable = getLanguageConnectionContext().getOptimizerFactory().getMaxMemoryPerTable();
		if(maxMemoryPerTable<=0)
			return rs;

        // since we create a new result set after row was created, resize row to make sure it can
        // accommodate CacheResultSet
		return getResultSetFactory().getCachedResultSet(this, rs, resultSetNumber);
	}



	//WARNING : this field name is referred in the DeleteNode generate routines.
	protected CursorResultSet[] raParentResultSets;


	// maintain hash table of parent result set vector
	// a table can have more than one parent source.
	protected Hashtable<String, Vector<TemporaryRowHolder>> parentResultSets;

	public void setParentResultSet(TemporaryRowHolder rs, String resultSetId)
	{
		Vector<TemporaryRowHolder> rsVector;
		if(parentResultSets == null)
			parentResultSets = new Hashtable<>();
		rsVector = parentResultSets.get(resultSetId);
		if(rsVector == null)
		{
			rsVector = new Vector<>();
			rsVector.addElement(rs);
		}else
		{
			rsVector.addElement(rs);
		}
		parentResultSets.put(resultSetId , rsVector);
	}

	/**
	 * get the reference to parent table ResultSets, that will be needed by the 
	 * referential action dependent table scans.
	 */
	public Vector getParentResultSet(String resultSetId)
	{
		return (Vector) parentResultSets.get(resultSetId);
	}

	public Hashtable getParentResultSets()
	{
		return parentResultSets;
	}

	/**
	 ** prepared statement use the same activation for
	 ** multiple execution. For each excution we create new
	 ** set of temporary resultsets, we should clear this hash table.
	 ** otherwise we will refer to the released resources.
	 */
	public void clearParentResultSets()
	{
		if(parentResultSets != null)
			parentResultSets.clear();
	}

	/**
	 * beetle 3865: updateable cursor using index.  A way of communication
	 * between cursor activation and update activation.
	 */
	public void setForUpdateIndexScan(CursorResultSet forUpdateIndexScan)
	{
		this.forUpdateIndexScan = forUpdateIndexScan;
	}

	public CursorResultSet getForUpdateIndexScan()
	{
		return forUpdateIndexScan;
	}

	private java.util.Calendar cal;
	/**
		Return a calendar for use by this activation.
		Calendar objects are not thread safe, the one returned
		is purely for use by this activation and it is assumed
		that is it single threded through the single active
		thread in a connection model.
	*/
	protected java.util.Calendar getCalendar() {
		if (cal == null)
			cal = new java.util.GregorianCalendar();
		return cal;

	}


	/*
	** Code originally in the parent class BaseExpressionActivation
	*/
	/**
	    Get the language connection factory associated with this connection
	  */
	public final LanguageConnectionContext	getLanguageConnectionContext()
	{
		return	lcc;
	}

	public final TransactionController getTransactionController()
	{
		return lcc.getTransactionExecute();
	}
			
	/**
	 * Get the Current ContextManager.
	 *
	 * @return Current ContextManager
	 */
	public ContextManager getContextManager()
	{
		return cm;
	}

	/**
		Used by activations to generate data values.  Most DML statements
		will use this method.  Possibly some DDL statements will, as well.
	 */
	public DataValueFactory getDataValueFactory() {
		return dvf;
	}

	/**
	 * Used to get a proxy for the current connection.
	 *
	 * @exception SQLException		Thrown on failure to get connection
	 */
	public Connection getCurrentConnection() throws SQLException {

		ConnectionContext cc = 
			(ConnectionContext) getContextManager().getContext(ConnectionContext.CONTEXT_ID);

		return cc.getNestedConnection(true);
	}	

	/**
		Real implementations of this method are provided by a generated class.
	*/
	public java.sql.ResultSet[][] getDynamicResults() {
		return null;
	}
	/**
		Real implementations of this method are provided by a generated class.
	*/
	public int getMaxDynamicResults() {
		return 0;
	}

    /**
     * Compute the DB2 compatible length of a value.
     *
     * @param value
     * @param constantLength The length, if it is a constant modulo null/not null. -1 if the length is not constant
     * @param reUse If non-null then re-use this as a container for the length
     *
     * @return the DB2 compatible length, set to null if value is null.
     */
    public NumberDataValue getDB2Length( DataValueDescriptor value,
                                         int constantLength,
                                         NumberDataValue reUse)
        throws StandardException
    {
        if( reUse == null)
            reUse = getDataValueFactory().getNullInteger( null);
        if( value.isNull())
            reUse.setToNull();
        else
        {
            if( constantLength >= 0)
                reUse.setValue( constantLength);
            else
            {
                reUse.setValue(value.getLength());
            }
        }
        return reUse;
    } // end of getDB2Length


	/* Dependable interface implementation */

	/**
	 * @see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder()
	{
	    return null;
	}


	/**
	 * @see Dependable#getObjectName
	 */
	public String getObjectName()
	{
		return UUIDString;
	}


	/**
	 * @see Dependable#getObjectID
	 */
	public UUID getObjectID()
	{
		return UUIDValue;
	}


	/**
	 * @see Dependable#getClassType
	 */
	public String getClassType()
	{
		return Dependable.ACTIVATION;
	}


	/**
	 * @see Dependable#isPersistent
	 */
	public boolean isPersistent()
	{
		/* activations are not persistent */
		return false;
	}


	/* Dependent interface implementation */

	/**
	 * @see com.splicemachine.db.iapi.sql.depend.Dependent#isValid
	 */
	public boolean isValid() {
		return isValid;
	}

	/**
	 * @see com.splicemachine.db.iapi.sql.depend.Dependent#makeInvalid
	 */
	public void makeInvalid(int action,
							LanguageConnectionContext lcc)
			throws StandardException {

		switch (action) {
		case DependencyManager.RECHECK_PRIVILEGES:
			// Make ourselves invalid.
			isValid = false;
			/* Clear out the old dependencies on this activation as we
			 * will die shortly.
			 */
			DependencyManager dm =
				lcc.getDataDictionary().getDependencyManager();
			dm.clearDependencies(lcc, this);

			break;
		case DependencyManager.REVOKE_ROLE:
			// Used by persistent objects (views, triggers, constraints)
			break;
		case DependencyManager.INTERNAL_RECOMPILE_REQUEST:
			// Used to recompile prepared statements
			break;
		default:
			if (SanityManager.DEBUG) {
				SanityManager.DEBUG_PRINT("BaseActivation", "action=" + action);
				SanityManager.NOTREACHED();
			}
		}

	}

	/**
	 * @see com.splicemachine.db.iapi.sql.depend.Dependent#prepareToInvalidate
	 */
	public void prepareToInvalidate(Provider p, int action,
							 LanguageConnectionContext lcc)
			throws StandardException {
	}

    public void setScanStartOverride(ExecRow scanStartOverride) {
        this.scanStartOverride = scanStartOverride;
    }

    public ExecRow getScanStartOverride() {
        return scanStartOverride;
    }

    public void setScanStopOverride(ExecRow scanStopOverride) {
        this.scanStopOverride = scanStopOverride;
    }

    public ExecRow getScanStopOverride() {
        return scanStopOverride;
    }

	public boolean isMaterialized() {
		return materialized;
	}

    public int[] getScanKeys() {
        return scanKeys;
    }

    public void setScanKeys(int[] keys) {
        scanKeys = keys;
    }

	public boolean willRunInSpark() {
		return useSpark() || getLanguageConnectionContext().getDataSetProcessorType()
				.equals(CompilerContext.DataSetProcessorType.FORCED_SPARK);
	}
}
