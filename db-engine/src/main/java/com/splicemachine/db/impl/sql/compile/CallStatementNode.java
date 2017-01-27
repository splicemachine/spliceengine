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

package com.splicemachine.db.impl.sql.compile;

import java.lang.reflect.Modifier;

import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import java.util.TreeSet;

/**
 * An CallStatementNode represents a CALL <procedure> statement.
 * It is the top node of the query tree for that statement.
 * A procedure call is very simple.
 * 
 * CALL [<schema>.]<procedure>(<args>)
 * 
 * <args> are either constants or parameter markers.
 * This implementation assumes that no subqueries or aggregates
 * can be in the argument list.
 * 
 * A procedure is always represented by a MethodCallNode.
 *
 */
public class CallStatementNode extends DMLStatementNode {
	/**
	 * The method call for the Java procedure. Guaranteed to be
	 * a JavaToSQLValueNode wrapping a MethodCallNode by checks
	 * in the parser.
	 */
	private JavaToSQLValueNode	methodCall;
    private static TreeSet<String> xplainTraceProcedures;

    static {
        xplainTraceProcedures = new TreeSet<String>();
        xplainTraceProcedures.add("SQLCAMESSAGE");
    }

	/**
	 * Initializer for a CallStatementNode.
	 *
	 * @param methodCall		The expression to "call"
	 */

	public void init(Object methodCall)
	{
		super.init(null);
		this.methodCall = (JavaToSQLValueNode) methodCall;
		this.methodCall.getJavaValueNode().markForCallStatement();
	}


	public String statementToString()
	{
		return "CALL";
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

			if (methodCall != null)
			{
				printLabel(depth, "methodCall: ");
				methodCall.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Bind this UpdateNode.  This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc.
	 * <p>
	 * Binding an update will also massage the tree so that
	 * the ResultSetNode has a single column, the RID.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public void bindStatement() throws StandardException {
		DataDictionary dd = getDataDictionary();

		if (SanityManager.DEBUG)
			SanityManager.ASSERT((dd != null), "Failed to get data dictionary");

		getCompilerContext().pushCurrentPrivType(getPrivType());
		methodCall = (JavaToSQLValueNode) methodCall.bindExpression(
							(FromList) getNodeFactory().getNode(
								C_NodeTypes.FROM_LIST,
								getNodeFactory().doJoinOrderOptimization(),
								getContextManager()), 
							null,
							null);

		// Disallow creation of BEFORE triggers which contain calls to 
		// procedures that modify SQL data. 
  		checkReliability();

		getCompilerContext().popCurrentPrivType();
	}

	/**
	 * Optimize a DML statement (which is the only type of statement that
	 * should need optimizing, I think). This method over-rides the one
	 * in QueryTreeNode.
	 *
	 * This method takes a bound tree, and returns an optimized tree.
	 * It annotates the bound tree rather than creating an entirely
	 * new tree.
	 *
	 * Throws an exception if the tree is not bound, or if the binding
	 * is out of date.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void optimizeStatement() throws StandardException
	{
		DataDictionary dd = getDataDictionary();

		if (SanityManager.DEBUG)
		SanityManager.ASSERT((dd != null), "Failed to get data dictionary");

		/* Preprocess the method call tree */
		methodCall = (JavaToSQLValueNode) methodCall.preprocess(
								getCompilerContext().getNumTables(),
								(FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager()),
								(SubqueryList) null,
								(PredicateList) null);

        JavaValueNode valueNode = methodCall.getJavaValueNode();
        if (valueNode instanceof MethodCallNode) {
            MethodCallNode callNode = (MethodCallNode) valueNode;
            String methodName = callNode.getMethodName();
            if (xplainTraceProcedures.contains(methodName)) {
                // turn off explain trace for xplain procedures
                getLanguageConnectionContext().getStatementContext().setXPlainTableOrProcedure(true);
            }
        }
	}

	/**
	 * Code generation for CallStatementNode.
	 * The generated code will contain:
	 *		o  A generated void method for the user's method call.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	The method for the execute() method to be built
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		JavaValueNode		methodCallBody;

		/* generate the parameters */
		generateParameterValueSet(acb);

		/* 
		 * Skip over the JavaToSQLValueNode and call generate() for the JavaValueNode.
		 * (This skips over generated code which is unnecessary since we are throwing
		 * away any return value and which won't work with void methods.)
		 * generates:
		 *     <methodCall.generate(acb)>;
		 * and adds it to userExprFun
		 */
		methodCallBody = methodCall.getJavaValueNode();

		/*
		** Tell the method call that its return value (if any) will be
		** discarded.  This is so it doesn't generate the ?: operator
		** that would return null if the receiver is null.  This is
		** important because the ?: operator cannot be made into a statement.
		*/
		methodCallBody.markReturnValueDiscarded();

		// this sets up the method
		// generates:
		// 	void userExprFun {
		//     method_call(<args>);
		//  }
		//
		//  An expression function is used to avoid reflection.
		//  Since the arguments to a procedure are simple, this
		// will be the only expression function and so it will
		// be executed directly as e0.
		MethodBuilder userExprFun = acb.newGeneratedFun("void", Modifier.PUBLIC);
		userExprFun.addThrownException("java.lang.Exception");
		methodCallBody.generate(acb, userExprFun);
		userExprFun.endStatement();
		userExprFun.methodReturn();
		userExprFun.complete();
		acb.pushGetResultSetFactoryExpression(mb);
		acb.pushMethodReference(mb, userExprFun); // first arg
		acb.pushThisAsActivation(mb); // arg 2
		int args = 2;
        if (methodCallBody instanceof MethodCallNode) {
            // These are the real java class and method name of the stored procedure,
            // not the activation. For example, if this is an import action, the class
            // would be HdfsImport and the method would be IMPORT_DATA.
            // These are for reference only, not to be used in any reflection.
            mb.push(((MethodCallNode)methodCallBody).getJavaClassName());
            mb.push(((MethodCallNode)methodCallBody).getMethodName());
            args = 4;
        }
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getCallStatementResultSet", ClassName.ResultSet, args);
	}

	public ResultDescription makeResultDescription()
	{
		return null;
	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 */
    @Override
	public void acceptChildren(Visitor v) throws StandardException {
		super.acceptChildren(v);

		if (methodCall != null)
		{
			methodCall = (JavaToSQLValueNode) methodCall.accept(v, this);
		}
	}

	/**
	 * Set default privilege of EXECUTE for this node. 
	 */
	int getPrivType()
	{
		return Authorizer.EXECUTE_PRIV;
	}
	
	/**
	 * This method checks if the called procedure allows modification of SQL 
	 * data. If yes, it cannot be compiled if the reliability is 
	 * <code>CompilerContext.MODIFIES_SQL_DATA_PROCEDURE_ILLEGAL</code>. This 
	 * reliability is set for BEFORE triggers in the create trigger node. This 
	 * check thus disallows creation of BEFORE triggers which contain calls to 
	 * procedures that modify SQL data in the trigger action statement.  
	 * 
	 * @throws StandardException
	 */
	private void checkReliability() throws StandardException {
		if(getSQLAllowedInProcedure() == RoutineAliasInfo.MODIFIES_SQL_DATA &&
				getCompilerContext().getReliability() == CompilerContext.MODIFIES_SQL_DATA_PROCEDURE_ILLEGAL) 
			throw StandardException.newException(SQLState.LANG_UNSUPPORTED_TRIGGER_PROC);
	}
	
	/**
	 * This method checks the SQL allowed by the called procedure. This method 
	 * should be called only after the procedure has been resolved.
	 * 
	 * @return	SQL allowed by the procedure
	 */
	private short getSQLAllowedInProcedure() {
		RoutineAliasInfo routineInfo = ((MethodCallNode)methodCall.getJavaValueNode()).routineInfo;
		
		// If this method is called before the routine has been resolved, routineInfo will be null 
		if (SanityManager.DEBUG)
			SanityManager.ASSERT((routineInfo != null), "Failed to get routineInfo");

		return routineInfo.getSQLAllowed();
	}
}
