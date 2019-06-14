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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.ProviderInfo;
import com.splicemachine.db.iapi.sql.depend.ProviderList;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.catalog.UUID;

import java.util.Collections;

/**
 * A CreateViewNode is the root of a QueryTree that represents a CREATE VIEW
 * statement.
 *
 */

public class CreateViewNode extends DDLStatementNode
{
	ResultColumnList	resultColumns;
	ResultSetNode		queryExpression;
	String				qeText;
	int					checkOption;
	ProviderInfo[]		providerInfos;
	ColumnInfo[]		colInfos;
	private OrderByList orderByList;
    private ValueNode   offset;
    private ValueNode   fetchFirst;
    private boolean hasJDBClimitClause; // true if using JDBC limit/offset escape syntax
	private boolean isRecursive;

	/**
	 * Initializer for a CreateViewNode
	 *
	 * @param newObjectName		The name of the table to be created
	 * @param resultColumns		The column list from the view definition, 
	 *							if specified
	 * @param queryExpression	The query expression for the view
	 * @param checkOption		The type of WITH CHECK OPTION that was specified
	 *							(NONE for now)
	 * @param qeText			The text for the queryExpression
	 * @param orderCols         ORDER BY list
     * @param offset            OFFSET if any, or null
     * @param fetchFirst        FETCH FIRST if any, or null
	 * @param hasJDBClimitClause True if the offset/fetchFirst clauses come from JDBC limit/offset escape syntax
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(Object newObjectName,
				   Object resultColumns,
				   Object	 queryExpression,
				   Object checkOption,
				   Object qeText,
                   Object orderCols,
                   Object offset,
                   Object fetchFirst,
                   Object hasJDBClimitClause,
                   Object isRecursive)
		throws StandardException
	{
		initAndCheck(newObjectName);
		this.resultColumns = (ResultColumnList) resultColumns;
		this.queryExpression = (ResultSetNode) queryExpression;
		this.checkOption = (Integer) checkOption;
		this.qeText = ((String) qeText).trim();
		this.orderByList = (OrderByList)orderCols;
        this.offset = (ValueNode)offset;
        this.fetchFirst = (ValueNode)fetchFirst;
        this.hasJDBClimitClause = hasJDBClimitClause != null && (Boolean) hasJDBClimitClause;
        this.isRecursive = isRecursive != null && (Boolean) isRecursive;

		implicitCreateSchema = true;
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return super.toString() +
				"checkOption: " + checkOption + "\n" +
				"qeText: " + qeText + "\n";
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "CREATE VIEW";
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (resultColumns != null)
			{
				printLabel(depth, "resultColumns: ");
				resultColumns.treePrint(depth + 1);
			}

			printLabel(depth, "queryExpression: ");
			queryExpression.treePrint(depth + 1);
		}
	}

	// accessors

	public	int				getCheckOption() { return checkOption; }

	public	ProviderInfo[]	getProviderInfo() { return providerInfos; }

	public	ColumnInfo[]	getColumnInfo() { return colInfos; }

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this CreateViewNode.  This means doing any static error
	 * checking that can be done before actually creating the table.
	 * For example, verifying that the ResultColumnList does not
	 * contain any duplicate column names.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindStatement() throws StandardException
	{
		CompilerContext				cc = getCompilerContext();
		DataDictionary				dataDictionary = getDataDictionary();
		ResultColumnList			qeRCL;
		String						duplicateColName;

		// bind the query expression
		if (isRecursive) {
			providerInfos = bindRecursiveViewDefinition
					( dataDictionary, cc,
							getNodeFactory(),
							getContextManager()
					);
		} else {
			providerInfos = bindViewDefinition
					(dataDictionary, cc,
							getNodeFactory(),
							queryExpression,
							getContextManager()
					);
		}

		qeRCL = queryExpression.getResultColumns();

		/* If there is an RCL for the view definition then
		 * copy the names to the queryExpression's RCL after verifying
		 * that they both have the same size.
		 */
		if (resultColumns != null)
		{
			if (resultColumns.size() != qeRCL.visibleSize())
			{
				throw StandardException.newException(SQLState.LANG_VIEW_DEFINITION_R_C_L_MISMATCH,
								getFullName());
			}
			qeRCL.copyResultColumnNames(resultColumns);
		}

		/* Check to make sure the queryExpression's RCL has unique names. If target column
		 * names not specified, raise error if there are any un-named columns to match DB2
		 */
		duplicateColName = qeRCL.verifyUniqueNames(resultColumns == null);
		if (duplicateColName != null)
		{
			throw StandardException.newException(SQLState.LANG_DUPLICATE_COLUMN_NAME_CREATE_VIEW, duplicateColName);
		}

		/* Only 5000 columns allowed per view */
		if (queryExpression.getResultColumns().size() > Limits.DB2_MAX_COLUMNS_IN_VIEW)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_COLUMNS_IN_TABLE_OR_VIEW,
				String.valueOf(queryExpression.getResultColumns().size()),
				getRelativeName(),
				String.valueOf(Limits.DB2_MAX_COLUMNS_IN_VIEW));
		}

		// for each column, stuff system.column
		// System columns should only include visible columns DERBY-4230
		colInfos = new ColumnInfo[queryExpression.getResultColumns().visibleSize()];
		genColumnInfos(colInfos);
	}

	/**
	 * Bind the query expression for a view definition. 
	 *
	 * @param dataDictionary	The DataDictionary to use to look up
	 *				columns, tables, etc.
	 *
	 * @return	Array of providers that this view depends on.
	 *
	 * @exception StandardException		Thrown on error
	 */

	private ProviderInfo[] bindViewDefinition( DataDictionary 	dataDictionary,
											 CompilerContext	compilerContext,
											 NodeFactory		nodeFactory,
											 ResultSetNode		queryExpr,
											 ContextManager		cm)
		throws StandardException
	{
		FromList	fromList = (FromList) nodeFactory.getNode(
										C_NodeTypes.FROM_LIST,
										nodeFactory.doJoinOrderOptimization(),
										cm);

		ProviderList 	prevAPL = compilerContext.getCurrentAuxiliaryProviderList();
		ProviderList 	apl = new ProviderList();

		try {
			compilerContext.setCurrentAuxiliaryProviderList(apl);
			compilerContext.pushCurrentPrivType(Authorizer.SELECT_PRIV);

			/* Bind the tables in the queryExpression */
			queryExpr = queryExpr.bindNonVTITables(dataDictionary, fromList);
			queryExpr = queryExpr.bindVTITables(fromList);


			/* Bind the expressions under the resultSet */
			queryExpr.bindExpressions(fromList);

			//cannot define views on temporary tables
			if (queryExpr instanceof SelectNode)
			{
				//If attempting to reference a SESSION schema table (temporary or permanent) in the view, throw an exception
				if (queryExpr.referencesSessionSchema())
					throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);
                // check that no provider is a temp table (whether or not it's in SESSION schema)
                for (Provider provider : apl.values()) {
                    if (provider instanceof TableDescriptor && ! provider.isPersistent()) {
                        throw StandardException.newException(SQLState.LANG_TEMP_TABLES_CANNOT_BE_IN_VIEWS,
                                                             provider.getObjectName());
                    }
                }
			}

			// bind the query expression
			queryExpr.bindResultColumns(fromList);
			
			// rejects any untyped nulls in the RCL
			// e.g.:  CREATE VIEW v1 AS VALUES NULL
			queryExpr.bindUntypedNullsToResultColumns(null);
		}
		finally
		{
			compilerContext.popCurrentPrivType();
			compilerContext.setCurrentAuxiliaryProviderList(prevAPL);
		}

		DependencyManager 		dm = dataDictionary.getDependencyManager();
		ProviderInfo[]			providerInfos = dm.getPersistentProviderInfos(apl);
		// need to clear the column info in case the same table descriptor
		// is reused, eg., in multiple target only view definition
		dm.clearColumnInfoInProviders(apl);

		/* Verify that all underlying ResultSets reclaimed their FromList */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(fromList.size() == 0,
				"fromList.size() is expected to be 0, not " + fromList.size() +
				" on return from RS.bindExpressions()");
		}

		return providerInfos;
	}

	public void replaceSelfReferenceForRecursiveView(TableDescriptor td) throws StandardException {
		// set recursive view descriptor which contains the column type info.
		((UnionNode)queryExpression).setViewDescreiptor(td);
		ResultSetNode leftResultSetNode = ((UnionNode) queryExpression).getLeftResultSet();
		ResultSetNode rightResultSetNode = ((UnionNode) queryExpression).getRightResultSet();
		RecursiveViewReferenceVisitor recursiveViewReferenceVisitor =
				new RecursiveViewReferenceVisitor(getObjectName(), leftResultSetNode, queryExpression, td);

		leftResultSetNode.accept(recursiveViewReferenceVisitor);
		int numReferences = recursiveViewReferenceVisitor.getNumReferences();
		if (numReferences > 0) {
			throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR,
					"No recursive reference is allowed in the seed statement of a WITH RECURSIVE");
		}

		rightResultSetNode.accept(recursiveViewReferenceVisitor);
		numReferences = recursiveViewReferenceVisitor.getNumReferences();

		/* check for valid syntax: currently only one recursive reference is allowed */
		if (numReferences <= 0) {
			throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR,
					"No recursive reference found in WITH RECURSIVE");
		} else if (numReferences > 1) {
			throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR,
					"More than one recursive reference in WITH RECURSIVE is not supported!");
		}

		return;
	}

	private ProviderInfo[] bindRecursiveViewDefinition(DataDictionary 	dataDictionary,
													   CompilerContext	compilerContext,
											           NodeFactory		nodeFactory,
											           ContextManager	cm)
			throws StandardException
	{
		FromList	fromList = (FromList) nodeFactory.getNode(
				C_NodeTypes.FROM_LIST,
				nodeFactory.doJoinOrderOptimization(),
				cm);

		ProviderList 	prevAPL = compilerContext.getCurrentAuxiliaryProviderList();
		ProviderList 	apl = new ProviderList();

		try {
			compilerContext.setCurrentAuxiliaryProviderList(apl);
			compilerContext.pushCurrentPrivType(Authorizer.SELECT_PRIV);

			/* step 1: recursive with must be a UNION-AlL where the left branch is the
			   seed, and the right branch is the recursion part with a self-reference
			 */
			if (!(queryExpression instanceof UnionNode) ||  !((UnionNode)queryExpression).all)
				throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR,
						"WITH RECURSIVE requires UNION-ALL operation at the top level of the definition");

			/* step 2: replace the self-reference in the right branch with a SelfReferenceNode */
			replaceSelfReferenceForRecursiveView(null);

			/* step 3: bind the left of the union-all */
			((UnionNode) queryExpression).getLeftResultSet().bindNonVTITables(dataDictionary, fromList);
			((UnionNode) queryExpression).getLeftResultSet().bindVTITables(fromList);

			/* Bind the expressions under the resultSet */
			((UnionNode) queryExpression).getLeftResultSet().bindExpressions(fromList);

			// bind the query expression
			((UnionNode) queryExpression).getLeftResultSet().bindResultColumns(fromList);

			/* step 4: bind the right only, and the union node */
			((SetOperatorNode)queryExpression).bindNonVTITables(dataDictionary, fromList, true);
			((SetOperatorNode)queryExpression).bindVTITables(fromList, true);
			((SetOperatorNode)queryExpression).bindExpressions(fromList, true);
			((SetOperatorNode)queryExpression).bindResultColumns(fromList, true);

			/* step 5: more syntax check, no nested recursive view allowed */
			RecursiveViewSyntaxCheckVisitor syntaxCheckVisitor = new RecursiveViewSyntaxCheckVisitor();
			queryExpression.accept(syntaxCheckVisitor);

			//cannot define views on temporary tables
			//If attempting to reference a SESSION schema table (temporary or permanent) in the view, throw an exception
			if (queryExpression.referencesSessionSchema())
				throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);
			// check that no provider is a temp table (whether or not it's in SESSION schema)
			for (Provider provider : apl.values()) {
				if (provider instanceof TableDescriptor && ! provider.isPersistent()) {
					throw StandardException.newException(SQLState.LANG_TEMP_TABLES_CANNOT_BE_IN_VIEWS,
							provider.getObjectName());
				}
			}

			// rejects any untyped nulls in the RCL
			// e.g.:  CREATE VIEW v1 AS VALUES NULL
			queryExpression.bindUntypedNullsToResultColumns(null);
		}
		finally
		{
			compilerContext.popCurrentPrivType();
			compilerContext.setCurrentAuxiliaryProviderList(prevAPL);
		}

		DependencyManager 		dm = dataDictionary.getDependencyManager();
		ProviderInfo[]			providerInfos = dm.getPersistentProviderInfos(apl);
		// need to clear the column info in case the same table descriptor
		// is reused, eg., in multiple target only view definition
		dm.clearColumnInfoInProviders(apl);

		/* Verify that all underlying ResultSets reclaimed their FromList */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(fromList.size() == 0,
					"fromList.size() is expected to be 0, not " + fromList.size() +
							" on return from RS.bindExpressions()");
		}

		return providerInfos;
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesSessionSchema()
		throws StandardException
	{
		//If create view is part of create statement and the view references SESSION schema tables, then it will
		//get caught in the bind phase of the view and exception will be thrown by the view bind. 
		return (queryExpression.referencesSessionSchema());
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		/* RESOLVE - need to build up dependendencies and store them away through
		 * the constant action.
		 */
		return	getGenericConstantActionFactory().getCreateViewConstantAction(getSchemaDescriptor().getSchemaName(),
											  getRelativeName(),
											  TableDescriptor.VIEW_TYPE,
											  qeText,
											  checkOption,
											  colInfos,
											  providerInfos,
											  (UUID)null); 	// compilation schema, filled
															// in when we create the view
	}

	/**
	 * Fill in the ColumnInfo[] for this create view.
	 * 
	 * @param colInfos	The ColumnInfo[] to be filled in.
	 */
	private void genColumnInfos(ColumnInfo[] colInfos)
	{
		ResultColumnList rcl = 	queryExpression.getResultColumns();

		for (int index = 0; index < colInfos.length; index++)
		{
			ResultColumn rc = (ResultColumn) rcl.elementAt(index);
			// The colInfo array has been initialized to be of length 
			// visibleSize() (DERBY-4230).  This code assumes that all the visible
			// columns are at the beginning of the rcl. Throw an assertion 
			// if we hit a generated column in what we think is the visible
			// range.
			if (SanityManager.DEBUG) {
				if (rc.isGenerated)
					SanityManager.THROWASSERT("Encountered generated column in expected visible range at rcl[" + index +"]");
			}
			//RESOLVEAUTOINCREMENT
			colInfos[index] = new ColumnInfo(rc.getName(),
											 rc.getType(),
											 null,
											 null,
											 null,
											 null,
											 null,
											 ColumnInfo.CREATE,
											 0, 0, 0,-1);
		}
	}

	/*
	 * class interface
	 */

	/**
	  *	Get the parsed query expression (the SELECT statement).
	  *
	  *	@return	the parsed query expression.
	  */
	ResultSetNode	getParsedQueryExpression() { return queryExpression; }


	/*
	 * These methods are used by execution
	 * to get information for storing into
	 * the system catalogs.
	 */


	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 */
    @Override
	public void acceptChildren(Visitor v) throws StandardException {
		super.acceptChildren(v);

		if (queryExpression != null)
		{
			queryExpression = (ResultSetNode)queryExpression.accept(v, this);
		}
	}

    public OrderByList getOrderByList() {
        return orderByList;
    }

    public ValueNode getOffset() {
        return offset;
    }

    public ValueNode getFetchFirst() {
        return fetchFirst;
    }
    
    public boolean hasJDBClimitClause() { return hasJDBClimitClause; }

	public TableDescriptor createDynamicView() throws StandardException {
		DataDictionary dd = this.getDataDictionary();
		SchemaDescriptor sd  = this.getSchemaDescriptor();
		LanguageConnectionContext lcc = this.getLanguageConnectionContext();
		TransactionController tc = lcc.getTransactionExecute();
		TableDescriptor existingDescriptor = dd.getTableDescriptor(getRelativeName(), sd, tc);
		if (existingDescriptor != null) {
			throw StandardException.newException(com.splicemachine.db.shared.common.reference.SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
					existingDescriptor.getDescriptorType(),
					existingDescriptor.getDescriptorName(),
					sd.getDescriptorType(),
					sd.getDescriptorName());
		}

		/* Create a new table descriptor.
		 * (Pass in row locking, even though meaningless for views.)
		 */
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
		TableDescriptor td = ddg.newTableDescriptor(getRelativeName(),sd,TableDescriptor.WITH_TYPE,TableDescriptor.ROW_LOCK_GRANULARITY,-1,null,null,null,null,null, null,false,false);
		UUID toid = td.getUUID();

		// No Need to add since this will be dynamic!!!
//		dd.addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc);
//		toid = td.getUUID();

		// for each column, stuff system.column
		ColumnDescriptor[] cdlArray = new ColumnDescriptor[colInfos.length];
		int index = 1;
		for (int ix = 0; ix < colInfos.length; ix++) {
			index++;
			ColumnDescriptor columnDescriptor = new ColumnDescriptor(
					colInfos[ix].name,
					index,
					index,
					colInfos[ix].dataType,
					colInfos[ix].defaultValue,
					colInfos[ix].defaultInfo,
					td,
					(UUID) null,
					colInfos[ix].autoincStart,
					colInfos[ix].autoincInc,
					index
			);
			cdlArray[ix] = columnDescriptor;
		}
		// Do not add to dictionary since it is dynamic!!
//		dd.addDescriptorArray(cdlArray, td,DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

		// add columns to the column descriptor list.
		ColumnDescriptorList cdl = td.getColumnDescriptorList();
		Collections.addAll(cdl, cdlArray);

		String createViewString;
		if (isRecursive)
			createViewString = "create recursive view ";
		else
			createViewString = "create view ";

		ViewDescriptor vd = ddg.newViewDescriptor(toid, getRelativeName(), createViewString + getRelativeName() + " " + qeText, checkOption, sd.getUUID());
		td.setViewDescriptor(vd);
		return td;
	}

	public boolean isRecursive() {
    	return isRecursive;
	}
}
