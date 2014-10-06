/*
   Derby - Class org.apache.derby.impl.sql.compile.WindowFunctionNode

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.compile;

import java.util.Vector;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.types.DataTypeDescriptor;


/**
 * Superclass of all window functions.
 */
public abstract class WindowFunctionNode extends AggregateNode {

    private WindowNode window; // definition or reference

    /**
     * Initializer. AggregateNode override.
     *
     * @param arg1 UnaryNode operand (to satisfy AggregateNode init())
     * @param arg2 The function's definition class
     * @param arg3 function name
     * @param arg4 isDistinct
     * @param arg5 The window definition or reference
     * @throws StandardException
     */
    public void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) throws StandardException {
        super.init(arg1, arg2, arg3, arg4);
        this.window = (WindowNode)arg5;
    }

    /**
     * Initializer. Wrapped Aggregate override.
     * @param arg1 The window definition or reference
     * @param arg2 <code>null</code> - only for method disambiguation.
     * @throws StandardException
     */
    public void init(Object arg1, Object arg2) throws StandardException {
        this.window = (WindowNode)arg1;
    }

    public abstract String getName();

    /**
     * ValueNode override.
     * @see ValueNode#isConstantExpression
     */
    @Override
    public boolean isConstantExpression()
    {
        return false;
    }

    /**
     * ValueNode override.
     * @see ValueNode#isConstantExpression
     */
    @Override
    public boolean constantExpression(PredicateList whereClause)
    {
        // Without this, an ORDER by on ROW_NUMBER could get optimised away
        // if there is a restriction, e.g.
        //
        // SELECT -ABS(i) a, ROW_NUMBER() OVER () c FROM t
        //     WHERE i > 1 ORDER BY c DESC
        return false;
    }

    /**
     * Code somewhere is dependent on this for AggregateNodes.
     * @return <code>true</code>. Yes, a window function is a window function.
     */
    @Override
    public boolean isWindowFunction() {
        return true;
    }

    /**
     * @return window associated with this window function
     */
    public WindowNode getWindow() {
        return window;
    }

    /**
     * get this window functions operands. Scalar functions will
     * return only one in array.
     * @return one or more function operands.
     */
    public abstract ValueNode[] getOperands();

    /**
     * Set window associated with this window function call.
     * @param wdn window definition
     */
    public void setWindow(WindowDefinitionNode wdn) {
        this.window = wdn;
    }

    /**
     * @return if name matches a defined window (in windows), return the
     * definition of that window, else null.
     */
    private WindowDefinitionNode definedWindow(WindowList windows,
                                               String name) {
        for (int i=0; i < windows.size(); i++) {
            WindowDefinitionNode wdn =
                (WindowDefinitionNode)windows.elementAt(i);

            if (wdn.getName().equals(name)) {
                return wdn;
            }
        }
        return null;
    }

    /**
     * QueryTreeNode override.
     * @see QueryTreeNode#printSubNodes
     */

    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG)
        {
            super.printSubNodes(depth);

            printLabel(depth, "window: ");
            window.treePrint(depth + 1);
        }
    }

    @Override
    public ResultColumn getNewExpressionResultColumn(DataDictionary dd) throws StandardException {
        return super.getNewExpressionResultColumn(dd);
    }

    public ResultColumn[] getNewExpressionResultColumns(DataDictionary dd) throws StandardException {
        OrderByList orderByList = this.window.getOrderByList();
        // return value intended for scalar aggregate function
        if (isScalarAggregate() || orderByList == null) {
            ResultColumn[] resultColumns = new ResultColumn[1];
            resultColumns[0] = getNewExpressionResultColumn(dd);
            return resultColumns;
        }
        // return value intended for ranking function
        ResultColumn[] resultColumns = new ResultColumn[orderByList.size()];
        for (int i=0; i<orderByList.size(); i++) {
            ValueNode node = orderByList.getOrderByColumn(i).getColumnExpression();

            resultColumns[i] = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##WindowExpression",
                node,
                getContextManager());
        }
        return resultColumns;
    }

    public boolean isScalarAggregate() {
        return true;
    }

    /**
     * Get the null result expression column.
     *
     * @return the value node
     *
     * @exception StandardException on error
     */
    public ValueNode    getNewNullResultExpression()
        throws StandardException
    {
        //
        // Create a result column with the aggregate operand
        // it.
        //
        return getNullNode(getTypeServices());
    }

    /**
     * TODO
     * Replace window function calls in the expression tree with a
     * ColumnReference to that window function, append the aggregate to the
     * supplied RCL (assumed to be from the child ResultSetNode) and return the
     * ColumnReference.
     *
     * @param rc
     * @param tableNumber The tableNumber for the new ColumnReference
     * @param nestingLevel this node's nesting level
     * @param newResultColumn
     * @return ValueNode    The (potentially) modified tree.
     * @throws StandardException
     */
    public ValueNode replaceCallWithColumnReference(ResultColumn rc,
                                                    int tableNumber,
                                                    int nestingLevel,
                                                    ResultColumn newResultColumn) throws StandardException {
         /*
          * This call is idempotent.  Do the right thing if we have already
          * replaced ourselves.
          */
        if (generatedRef == null) {
            generatedRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##SQLColWinGen" + newResultColumn.getName(),
                this,
                getContextManager());
            generatedRC.markGenerated();

            // Parse time.
            //
            generatedRef = (ColumnReference) getNodeFactory().getNode(
                C_NodeTypes.COLUMN_REFERENCE,
                "##CR -> " + generatedRC.getName(),
                null,
                getContextManager());

            generatedRef.setSource(generatedRC);
            generatedRef.setNestingLevel(nestingLevel);
            generatedRef.setSourceLevel(0);

            if (tableNumber != -1) {
                generatedRef.setTableNumber(tableNumber);
            }

            // Mark the ColumnReference as being generated to replace a call to
            // a window function
            generatedRef.markGeneratedToReplaceWindowFunctionCall();

            generatedRef.setSource(newResultColumn);
            rc.setExpression(generatedRef);
        }

        return generatedRef;
    }

    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    Vector aggregateVector) throws StandardException {
        DataDictionary dd = getDataDictionary();
        DataTypeDescriptor dts = null;
        ClassFactory cf = getClassFactory();
        classInspector = cf.getClassInspector();
        if (userAggregateName != null) {
            userAggregateName.bind(dd);
            AliasDescriptor ad = resolveAggregate
                (
                    dd,
                    getSchemaDescriptor(userAggregateName.getSchemaName(), true),
                    userAggregateName.getTableName()
                );
            if (ad == null) {
                throw StandardException.newException
                    (
                        SQLState.LANG_OBJECT_NOT_FOUND,
                        AliasDescriptor.getAliasType(AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR),
                        userAggregateName.getTableName()
                    );
            }

            setUserDefinedAggregate(new UserAggregateDefinition(ad));
            aggregateName = ad.getJavaClassName();
        }

        instantiateAggDef();
        // if this is a user-defined aggregate
        if (isUserDefinedAggregate()) {
            AliasDescriptor ad = ((UserAggregateDefinition) uad).getAliasDescriptor();

            // set up dependency on the user-defined aggregate and compile a check for USAGE
            // priv if needed
            getCompilerContext().createDependency(ad);

            if (isPrivilegeCollectionRequired()) {
                getCompilerContext().addRequiredUsagePriv(ad);
            }
        }
         /* Add ourselves to the aggregateVector before we do anything else */
        aggregateVector.add(this);

        CompilerContext cc = getCompilerContext();

        // operand being null means a count(*)
        if (operand != null) {
            int previousReliability = orReliability(CompilerContext.AGGREGATE_RESTRICTION);

            // overridden to bind all function operands
            bindOperand(fromList, subqueryList, aggregateVector);
            cc.setReliability(previousReliability);

              /*
              ** Make sure that we don't have an aggregate
              ** IMMEDIATELY below us.  Don't search below
              ** any ResultSetNodes.
              */
            HasNodeVisitor visitor = new HasNodeVisitor(this.getClass(), ResultSetNode.class);
            operand.accept(visitor);
            if (visitor.hasNode()) {
                throw StandardException.newException
                    (
                        SQLState.LANG_USER_AGGREGATE_CONTAINS_AGGREGATE,
                        getSQLName()
                    );
            }

            // Also forbid any window function inside an aggregate unless in
            // subquery, cf. SQL 2003, section 10.9, SR 7 a).
            SelectNode.checkNoWindowFunctions(operand, aggregateName);

              /*
              ** Check the type of the operand.  Make sure that the user
              ** defined aggregate can handle the operand datatype.
              */
            dts = operand.getTypeServices();

              /* Convert count(nonNullableColumn) to count(*)	*/
            if (uad instanceof CountAggregateDefinition &&
                !dts.isNullable()) {
                setOperator(aggregateName);
                setMethodName(aggregateName);
            }

              /*
              ** If we have a distinct, then the value expression
              ** MUST implement Orderable because we are going
              ** to process it using it as part of a sort.
              */
            if (distinct) {
                // FIXME: won't need distinct - always false for window function
                  /*
                  ** For now, we check to see if orderable() returns
                  ** true for this type.  In the future we may need
                  ** to check to see if the type implements Orderable
                  **
                  */
                if (!operand.getTypeId().orderable(cf)) {
                    throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION,
                                                         dts.getTypeId().getSQLTypeName());
                }
            }

              /*
              ** Don't allow an untyped null
              */
            if (operand instanceof UntypedNullConstantNode) {
                throw StandardException.newException
                    (SQLState.LANG_USER_AGGREGATE_BAD_TYPE_NULL, getSQLName());

            }
        }

      /*
      ** Ask the aggregate definition whether it can handle
       ** the input datatype.
       */
        aggregatorClassName = new StringBuffer();
        DataTypeDescriptor resultType = uad.getAggregator(dts, aggregatorClassName);

        if (resultType == null) {
            throw StandardException.newException(SQLState.LANG_USER_AGGREGATE_BAD_TYPE,
                                                 getSQLName(),
                                                 operand.getTypeId().getSQLTypeName());

        }
        // For user-defined aggregates, the input operand may need to be
        // coerced to the expected input type of the aggregator.
        if (isUserDefinedAggregate()) {
            ValueNode castNode = ((UserAggregateDefinition) uad).castInputValue
                (operand, getContextManager());
            if (castNode != null) {
                operand = castNode.bindExpression(fromList, subqueryList, aggregateVector);
            }
        }

        checkAggregatorClassName(aggregatorClassName.toString());

        setType(resultType);

        return this;
    }

    /**
     * Bind the operand for this unary operator.
     * Binding the operator may change the operand node.
     * Sub-classes bindExpression() methods need to call this
     * method to bind the operand.
     */
    @Override
    protected void bindOperand(FromList fromList, SubqueryList subqueryList, Vector aggregateVector)
        throws StandardException {
        for (ValueNode node : getOperands()) {
            node = node.bindExpression(fromList, subqueryList, aggregateVector);

            if (node.requiresTypeFromContext()) {
                bindParameter();
                // If not bound yet then just return.
                // The node type will be set by either
                // this class' bindExpression() or a by
                // a node that contains this expression.
                if (node.getTypeServices() == null)
                    return;
            }

		/* If the operand is not a built-in type, then generate a bound conversion
         * tree to a built-in type.
		 */
            if (!(node instanceof UntypedNullConstantNode) &&
                node.getTypeId().userType()) {
                node = node.genSQLJavaSQLTree();
            }
        }
    }
}
