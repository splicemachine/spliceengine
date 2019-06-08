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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.context.Context;
import com.splicemachine.db.iapi.services.compiler.JavaFactory;
import com.splicemachine.db.iapi.services.loader.ClassFactory;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.dictionary.*;

import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.depend.ProviderList;

import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.store.access.SortCostController;
import com.splicemachine.db.impl.sql.compile.subquery.aggregate.AggregateSubqueryFlatteningVisitor;
import com.splicemachine.system.SimpleSparkVersion;
import com.splicemachine.system.SparkVersion;

import java.util.List;
import java.util.Vector;
import java.sql.SQLWarning;

/**
 * CompilerContext stores the parser and type id factory to be used by
 * the compiler.  Stack compiler contexts when a new, local parser is needed
 * (if calling the compiler recursively from within the compiler,
 * for example).
 * CompilerContext objects are private to a LanguageConnectionContext.
 *
 *
 * History:
 *	5/22/97 Moved getExternalInterfaceFactory() to LanguageConnectionContext
 *			because it had to be used at execution. - Jeff
 */
public interface CompilerContext extends Context
{
	/////////////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTANTS
	//
	/////////////////////////////////////////////////////////////////////////////////////


    enum DataSetProcessorType {
        DEFAULT_CONTROL, // Default Value
        FORCED_CONTROL, // Hinted to use control
        SPARK, // Scans Large enough for Spark
        FORCED_SPARK // Hinted to use Spark
    }

    enum NativeSparkModeType {
        SYSTEM, // Use the system-level setting.
        ON,     // Process the operation using UnSafeRows if the source operation produces them.
        OFF,    // Process the operation using Derby rows.
        FORCED  // Convert the source operation's rows into UnSafeRows,
	        // then process the current operation using UnSafeRows, if possible.
    }

	/**
	 * this is the ID we expect compiler contexts
	 * to be stored into a context manager under.
	 */


	String CONTEXT_ID = "CompilerContext";

	// bit masks for query fragments which are potentially unreliable. these are used
	// by setReliability() and checkReliability().

	int			DATETIME_ILLEGAL			=	0x00000001;
	// NOTE: getCurrentConnection() is currently legal everywhere
	int			CURRENT_CONNECTION_ILLEGAL	=	0x00000002;
	int			FUNCTION_CALL_ILLEGAL		=	0x00000004;
	int			UNNAMED_PARAMETER_ILLEGAL	=	0x00000008;
	int			DIAGNOSTICS_ILLEGAL			=	0x00000010;
	int			SUBQUERY_ILLEGAL			=	0x00000020;
	int			USER_ILLEGAL				=	0x00000040;
	int			COLUMN_REFERENCE_ILLEGAL	=	0x00000080;
	int			IGNORE_MISSING_CLASSES		=	0x00000100;
	int			SCHEMA_ILLEGAL				=	0x00000200;
	int			INTERNAL_SQL_ILLEGAL		=	0x00000400;
	
	/**
	 * Calling procedures that modify sql data from before triggers is illegal. 
	 * 
	 */
	int			MODIFIES_SQL_DATA_PROCEDURE_ILLEGAL	=	0x00000800;

	int			NON_DETERMINISTIC_ILLEGAL		=	0x00001000;
	int			SQL_IN_ROUTINES_ILLEGAL		=	0x00002000;

	int			NEXT_VALUE_FOR_ILLEGAL		=	0x00004000;

	/** Standard SQL is legal */
	int			SQL_LEGAL					=	(INTERNAL_SQL_ILLEGAL);

	/** Any SQL we support is legal */
	int			INTERNAL_SQL_LEGAL			=	0;

	int			CHECK_CONSTRAINT		= (
		                                                                    DATETIME_ILLEGAL |
																		    UNNAMED_PARAMETER_ILLEGAL |
																		    DIAGNOSTICS_ILLEGAL |
																		    SUBQUERY_ILLEGAL |
																			USER_ILLEGAL |
																			SCHEMA_ILLEGAL |
																			INTERNAL_SQL_ILLEGAL |
                                                                            NEXT_VALUE_FOR_ILLEGAL
																		  );

	int			DEFAULT_RESTRICTION		= (
		                                                                    SUBQUERY_ILLEGAL |
																			UNNAMED_PARAMETER_ILLEGAL |
																			COLUMN_REFERENCE_ILLEGAL |
																			INTERNAL_SQL_ILLEGAL
																			);

	int			GENERATION_CLAUSE_RESTRICTION		= (
		                                                                    CHECK_CONSTRAINT |
																			NON_DETERMINISTIC_ILLEGAL |
                                                                            SQL_IN_ROUTINES_ILLEGAL |
                                                                            NEXT_VALUE_FOR_ILLEGAL
																			);

	int			WHERE_CLAUSE_RESTRICTION		= NEXT_VALUE_FOR_ILLEGAL;
	int			HAVING_CLAUSE_RESTRICTION		= NEXT_VALUE_FOR_ILLEGAL;
	int			ON_CLAUSE_RESTRICTION		= NEXT_VALUE_FOR_ILLEGAL;
	int			AGGREGATE_RESTRICTION		= NEXT_VALUE_FOR_ILLEGAL;
	int			CONDITIONAL_RESTRICTION		= NEXT_VALUE_FOR_ILLEGAL;
	int			GROUP_BY_RESTRICTION		= NEXT_VALUE_FOR_ILLEGAL;
	int         DEFAULT_MAX_MULTICOLUMN_PROBE_VALUES              = 10000;
	boolean     DEFAULT_MULTICOLUMN_INLIST_PROBE_ON_SPARK_ENABLED = false;
	boolean     DEFAULT_CONVERT_MULTICOLUMN_DNF_PREDICATES_TO_INLIST = true;
	boolean     DEFAULT_DISABLE_PREDICATE_SIMPLIFICATION = false;
	SparkVersion DEFAULT_SPLICE_SPARK_VERSION = new SimpleSparkVersion("2.2.0");
	NativeSparkModeType DEFAULT_SPLICE_NATIVE_SPARK_AGGREGATION_MODE = NativeSparkModeType.SYSTEM;
	boolean     DEFAULT_SPLICE_ALLOW_OVERFLOW_SENSITIVE_NATIVE_SPARK_EXPRESSIONS = true;

	/////////////////////////////////////////////////////////////////////////////////////
	//
	//	BEHAVIOR
	//
	/////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Get the Parser from this CompilerContext.
	 *	 *
	 * @return	The parser associated with this CompilerContext
	 *
	 */

	Parser getParser();

	/**
	 * Get the NodeFactory from this CompilerContext.
	 *
	 * @return	The NodeFactory associated with this CompilerContext
	 *
	 */

	NodeFactory getNodeFactory();

	/**
	 * Get the TypeCompilerFactory from this CompilerContext.
	 *
	 * @return	The TypeCompilerFactory associated with this CompilerContext
	 *
	 */
	TypeCompilerFactory getTypeCompilerFactory();

	/**
		Return the class factory to use in this compilation.
	*/
	ClassFactory getClassFactory();

	/**
	 * Get the JavaFactory from this CompilerContext.
	 *
	 * @return	The JavaFactory associated with this CompilerContext
	 *
	 */

	JavaFactory getJavaFactory();

	/**
	 * Get the current next column number (for generated column names)
	 * from this CompilerContext.
	 *
	 * @return int	The next column number for the current statement.
	 *
	 */

	int getNextColumnNumber();

	/**
	  *	Reset compiler context (as for instance, when we recycle a context for
	  *	use by another compilation.
	  */
	void	resetContext();

	/**
	 * Get the current next table number from this CompilerContext.
	 *
	 * @return int	The next table number for the current statement.
	 *
	 */

	int getNextTableNumber();

	/**
	 * Get the number of tables in the current statement from this CompilerContext.
	 *
	 * @return int	The number of tables in the current statement.
	 *
	 */

	int getNumTables();

    /**
     * Utility method for Subquery Flattening.
     * @see AggregateSubqueryFlatteningVisitor#visit(Visitable)
     *
     * @param num
     */
	void setNumTables(int num);

	/**
	 * Some where subqueries can be converted to fromSubquery, so the number of tables could increase
	 * during preprocess of optimization. maximalPossibleTableCount takes the where Subqueries into
	 * considration as potentially the maximal possible table count.
	 * @return
	 */
	int getMaximalPossibleTableCount();

	void setMaximalPossibleTableCount(int num);

	/**
	 * Get the current next subquery number from this CompilerContext.
	 *
	 * @return int	The next subquery number for the current statement.
	 *
	 */

	int getNextSubqueryNumber();

	/**
	 * Get the number of subquerys in the current statement from this CompilerContext.
	 *
	 * @return int	The number of subquerys in the current statement.
	 *
	 */

	int getNumSubquerys();

	/**
	 * Get the current next ResultSet number from this CompilerContext.
	 *
	 * @return int	The next ResultSet number for the current statement.
	 *
	 */

	int getNextResultSetNumber();

	/**
	 * Reset the next ResultSet number from this CompilerContext.
	 */

	void resetNextResultSetNumber();

	/**
	 * Get the number of Results in the current statement from this CompilerContext.
	 *
	 * @return The number of ResultSets in the current statement.
	 *
	 */

	int getNumResultSets();

	/**
	 * Get a unique Class name from this CompilerContext.
	 * Ensures it is globally unique for this JVM.
	 *
	 * @return String	A unique-enough class name.
	 *
	 */

	String getUniqueClassName();

	/**
	 * Set the current dependent from this CompilerContext.
	 * This should be called at the start of a compile to
	 * register who has the dependencies needed for the compilation.
	 *
	 * @param d	The Dependent currently being compiled.
	 *
	 */

	void setCurrentDependent(Dependent d);

		Dependent getCurrentDependent();

	/**
	 * Get the current auxiliary provider list from this CompilerContext.
	 *
	 * @return	The current AuxiliaryProviderList.
	 *
	 */

	ProviderList getCurrentAuxiliaryProviderList();

	/**
	 * Set the current auxiliary provider list for this CompilerContext.
	 *
	 * @param apl	The new current AuxiliaryProviderList.
	 *
	 */

	void setCurrentAuxiliaryProviderList(ProviderList apl);

	/**
	 * Add a dependency for the current dependent.
	 *
	 * @param p	The Provider of the dependency.
	 * @exception StandardException thrown on failure.
	 *
	 */
	void createDependency(Provider p) throws StandardException;

	/**
	 * Add a dependency between two objects.
	 *
	 * @param d	The Dependent object.
	 * @param p	The Provider of the dependency.
	 * @exception StandardException thrown on failure.
	 *
	 */
	void createDependency(Dependent d, Provider p) throws StandardException;

	/**
	 * Add an object to the pool that is created at compile time
	 * and used at execution time.  Use the integer to reference it
	 * in execution constructs.  Execution code will have to generate:
	 *	<pre>
	 *	(#objectType) (this.getPreparedStatement().getSavedObject(#int))
	 *  <\pre>
	 *
	 * @param o object to add to the pool of saved objects
	 * @return the entry # for the object
	 */
	int	addSavedObject(Object o);

	/**
	 *	Get the saved object pool (for putting into the prepared statement).
	 *  This turns it into its storable form, an array of objects.
	 *
	 * @return the saved object pool.
	 */
	Object[] getSavedObjects(); 

	/**
	 *	Set the saved object pool (for putting into the prepared statement).
	 *
	 * @param objs	 The new saved objects
	 */
	void setSavedObjects(Object[] objs);

	/**
	 * Set the in use state for the compiler context.
	 *
	 * @param inUse	 The new inUse state for the compiler context.
	 */
	void setInUse(boolean inUse);

	/**
	 * Return the in use state for the compiler context.
	 *
	 * @return boolean	The in use state for the compiler context.
	 */
	boolean getInUse();

	/**
	 * Mark this CompilerContext as the first on the stack, so we can avoid
	 * continually popping and pushing a CompilerContext.
	 */
	void firstOnStack();

	/**
	 * Is this the first CompilerContext on the stack?
	 */
	boolean isFirstOnStack();

	/**
	 * Sets which kind of query fragments are NOT allowed. Basically,
	 * these are fragments which return unstable results. CHECK CONSTRAINTS
	 * and CREATE PUBLICATION want to forbid certain kinds of fragments.
	 *
	 * @param reliability	bitmask of types of query fragments to be forbidden
	 *						see the reliability bitmasks above
	 *
	 */
	void	setReliability(int reliability);

	/**
	 * Return the reliability requirements of this clause. See setReliability()
	 * for a definition of clause reliability.
	 *
	 * @return a bitmask of which types of query fragments are to be forbidden
	 */
	int getReliability();

	/**
	 * Get the compilation schema descriptor for this compilation context.
	   Will be null if no default schema lookups have occured. Ie.
	   the statement is independent of the current schema.
	 * 
	 * @return the compilation schema descirptor
	 */
	SchemaDescriptor getCompilationSchema();

	/**
	 * Set the compilation schema descriptor for this compilation context.
	 *
	 * @param newDefault compilation schema
	 * 
	 * @return the previous compilation schema descirptor
	 */
	SchemaDescriptor setCompilationSchema(SchemaDescriptor newDefault);

	/**
	 * Push a default schema to use when compiling.
	 * <p>
	 * Sometimes, we need to temporarily change the default schema, for example
	 * when recompiling a view, since the execution time default schema may
	 * differ from the required default schema when the view was defined.
	 * Another case is when compiling generated columns which reference
	 * unqualified user functions.
	 * </p>
	 * @param sd schema to use
	 */
	void pushCompilationSchema(SchemaDescriptor sd);


	/**
	 * Pop the default schema to use when compiling.
	 */
	void popCompilationSchema();

	/**
	 * Get a StoreCostController for the given conglomerate.
	 *
	 * @param conglomerateDescriptor	The conglomerate for which to get a StoreCostController.
	 * @param skipStats do not fetch the stats from dictionary if true
	 * @param defaultRowCount it only take effect when skipStats is true, and forces the fake stats' rowcount to be the specified value
	 *
	 * @return	The appropriate StoreCostController.
	 *
	 * @exception StandardException		Thrown on error
	 */
	StoreCostController getStoreCostController(TableDescriptor td, ConglomerateDescriptor conglomerateDescriptor, boolean skipStats, long defaultRowCount) throws StandardException;

	/**
	 * Get a SortCostController.
	 *
	 * @exception StandardException		Thrown on error
	 */
	SortCostController getSortCostController() throws StandardException;

	/**
	 * Set the parameter list.
	 *
	 * @param parameterList	The parameter list.
	 */
	void setParameterList(Vector parameterList);

	/**
	 * Get the parameter list.
	 *
	 * @return	The parameter list.
	 */
	Vector getParameterList();

	/**
	 * If callable statement uses ? = form
	 */
	void setReturnParameterFlag();

	/**
	 * Is the callable statement uses ? for return parameter.
	 *
	 * @return	true if ? = call else false
	 */
	boolean getReturnParameterFlag();

	/**
	 * Get the array of DataTypeDescriptor representing the types of
	 * the ? parameters.
	 *
	 * @return	The parameter descriptors
	 */

	DataTypeDescriptor[] getParameterTypes();

	/**
	 * Get the cursor info stored in the context.
	 *
	 * @return the cursor info
	 */
	Object getCursorInfo();
	
	/**
	 * Set params
	 *
	 * @param cursorInfo the cursor info
	 */
	void setCursorInfo(Object cursorInfo);

	/**
	 * Set the isolation level for the scans in this query.
	 *
	 * @param isolationLevel	The isolation level to use.
	 */
	void setScanIsolationLevel(int isolationLevel);

	/**
	 * Get the isolation level for the scans in this query.
	 *
	 * @return	The isolation level for the scans in this query.
	 */
	int getScanIsolationLevel();

	/**
	 * Get the next equivalence class for equijoin clauses.
	 *
	 * @return The next equivalence class for equijoin clauses.
	 */
	int getNextEquivalenceClass();

	/**
		Add a compile time warning.
	*/
	void addWarning(SQLWarning warning);

	/**
		Get the chain of compile time warnings.
	*/
	SQLWarning getWarnings();

	/**
	 * Sets the current privilege type context and pushes the previous on onto a stack.
	 * Column and table nodes do not know how they are
	 * being used. Higher level nodes in the query tree do not know what is being
	 * referenced. Keeping the context allows the two to come together.
	 *
	 * @param privType One of the privilege types in 
	 *						com.splicemachine.db.iapi.sql.conn.Authorizer.
	 */
	void pushCurrentPrivType(int privType);
	
	void popCurrentPrivType();
    
	/**
	 * Add a column privilege to the list of used column privileges.
	 *
	 * @param column
	 */
	void addRequiredColumnPriv(ColumnDescriptor column);

	/**
	 * Add a table or view privilege to the list of used table privileges.
	 *
	 * @param table
	 */
	void addRequiredTablePriv(TableDescriptor table);

	/**
	 * Add a schema privilege to the list of used privileges.
	 *
	 * @param schema	Schema name of the object that is being accessed
	 * @param aid		Requested authorizationId for new schema
	 * @param privType	CREATE_SCHEMA_PRIV, MODIFY_SCHEMA_PRIV or DROP_SCHEMA_PRIV
	 */
	void addRequiredSchemaPriv(String schema, String aid, int privType);

	void addRequiredAccessSchemaPriv(UUID uuid);

	/**
	 * Add a routine execute privilege to the list of used routine privileges.
	 *
	 * @param routine
	 */
	void addRequiredRoutinePriv(AliasDescriptor routine);

	/**
	 * Add a usage privilege to the list of required privileges.
	 *
	 * @param usableObject
	 */
	void addRequiredUsagePriv(PrivilegedSQLObject usableObject);

	/**
	 * Add a required role privilege to the list of privileges.
	 *
	 * @see CompilerContext#addRequiredRolePriv
	 */
	void addRequiredRolePriv(String roleName, int privType);

	/**
	 * @return The list of required privileges.
	 */
	List getRequiredPermissionsList();
    
	/**
	 * Add a sequence descriptor to the list of referenced sequences.
	 */
	void addReferencedSequence(SequenceDescriptor sd);

	/**
	 * Report whether the given sequence has been referenced already.
	 */
	boolean isReferenced(SequenceDescriptor sd);

    void setDataSetProcessorType(DataSetProcessorType type);

    DataSetProcessorType getDataSetProcessorType();

	boolean skipStats(int tableNumber);

	Vector<Integer> getSkipStatsTableList();

	public boolean getSelectivityEstimationIncludingSkewedDefault();

	public void setSelectivityEstimationIncludingSkewedDefault(boolean onOff);

	public boolean isProjectionPruningEnabled();

	public void setProjectionPruningEnabled(boolean onOff);
	
	public int getMaxMulticolumnProbeValues();
	
	public void setMaxMulticolumnProbeValues(int newValue);
	
	public void setMulticolumnInlistProbeOnSparkEnabled(boolean newValue);
	
	public boolean getMulticolumnInlistProbeOnSparkEnabled();
	
	public void setConvertMultiColumnDNFPredicatesToInList(boolean newValue);
	
	public boolean getConvertMultiColumnDNFPredicatesToInList();

	public void setDisablePredicateSimplification(boolean newValue);

	public boolean getDisablePredicateSimplification();

	public void setSparkVersion(SparkVersion newValue);

	public SparkVersion getSparkVersion();

	public boolean isSparkVersionInitialized();

	public void setNativeSparkAggregationMode(CompilerContext.NativeSparkModeType newValue);

	public CompilerContext.NativeSparkModeType getNativeSparkAggregationMode();

	public void setAllowOverflowSensitiveNativeSparkExpressions(boolean newValue);

	public boolean getAllowOverflowSensitiveNativeSparkExpressions();
}
