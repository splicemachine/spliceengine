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

package com.splicemachine.db.impl.sql.compile;

import java.util.List;
import java.util.Vector;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;


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
    public boolean isConstantExpression() {
        return false;
    }

    /**
     * ValueNode override.
     * @see ValueNode#isConstantExpression
     */
    @Override
    public boolean constantExpression(PredicateList whereClause) {
        // Without this, an ORDER by on ROW_NUMBER could get optimised away
        // if there is a restriction, e.g.
        //
        // SELECT -ABS(i) a, ROW_NUMBER() OVER () c FROM t
        //     WHERE i > 1 ORDER BY c DESC
        return false;
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        ValueNode[] operands = getOperands();
        ValueNode[] visitedNodes = new ValueNode[operands.length];
        int i=0;
        for (ValueNode overNode : operands) {
            if (overNode != null) {
                visitedNodes[i++] = (ValueNode) overNode.accept(v);
            }
        }
        setOperands(visitedNodes);
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

    /**
     * Get an ResultColumn array of all operand expressions. Called by WindowResultSetNode
     * during building of function tuple ([result, operand [, operand, ...], function]).
     *
     * @return the array of ResultColumns each pointing to a ColumnReference to
     * an operand of this operator.
     * @throws StandardException
     */
    public ResultColumn[] getNewExpressionResultColumns() throws StandardException {
        ValueNode[] operands = getOperands();
        ResultColumn[] resultColumns = new ResultColumn[operands.length];
        int i = 0;
        for (ValueNode node : operands) {
            if (node == null) {
                node = getNewNullResultExpression();
            }
            ValueNode lower = node;
            if (node instanceof  ColumnReference && ! ((ColumnReference)node).getGeneratedToReplaceAggregate()) {
                // If "node" is a ColumnReference, it is a reference to the operand and
                // lives at this level in the tree. We will need the underlying expression
                // reference from the node below because our function will reference the
                // "output" of the node below as its input.
                // If it's not a ColumnReference, we may be just above a FromBaseTable or
                // it may be a "direct" input value such as numeric for, say, count(). In
                // that case, we leave it "as is".
                lower = ((ColumnReference)node).getSource().getExpression();
                if (lower instanceof VirtualColumnNode) {
                    // If the "lower" expression is a VCN, create a CR pointing to its RC
                    // add to the plan tree
                    ResultColumn targetRC = ((VirtualColumnNode)lower).getSourceColumn();

                    ColumnReference tmpColumnRef = (ColumnReference) getNodeFactory().getNode(
                        C_NodeTypes.COLUMN_REFERENCE,
                        targetRC.getName(),
                        null,
                        getContextManager());
                    tmpColumnRef.setSource(targetRC);
                    tmpColumnRef.setNestingLevel(0);
                    tmpColumnRef.setSourceLevel(0);

                    lower = tmpColumnRef;
                }
            }
            resultColumns[i++] = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##WindowOperand",
                lower,
                getContextManager());
        }
        return resultColumns;
    }

    /**
     * Default behavior is to return <code>true</code>. That is so that
     * existing aggregate functions can operate "as-is".<br/>
     * Subclasses that operate on more than one operand - like ranking
     * functions - should return <code>false</code>.
     * @return whether this function operates on more than one operand.
     */
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
     * Replace window function calls in the expression tree with a
     * ColumnReference to that window function.  Use the supplied
     * <code>newResultColumn</code> ResultColumn as the source for the new CR.
     *
     * @param rc the RC with which to set the new CR as its expression (point
     *           it to the new CR node).
     * @param tableNumber The tableNumber for the new ColumnReference
     * @param nestingLevel this node's nesting level
     * @param newResultColumn the source RC for the new CR
     * @return the newly generated CR.
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
                "##SQLColWinGen_" + newResultColumn.getName(),
                this,
                getContextManager());
            generatedRC.markGenerated();

            // Parse time.
            //
            generatedRef = (ColumnReference) getNodeFactory().getNode(
                C_NodeTypes.COLUMN_REFERENCE,
                generatedRC.getName(),
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

    /**
     * We have to override this method because we subclass AggregateNode. Window functions,
     * can have more than one operand, unlike AggregateNode which subclasses UnaryOperatorNode,
     * and one-operand behavior is buried deep in AggregateNode's hierarchy.
     * <p/>
     * It may seem more appropriate to create a new NaryOperatorNode and subclass that, but
     * that would require a lot more integration with db plan tree manipulation and subclassing
     * AggregateNode allows us to wrap and use existing aggregate operators as window functions.
     * <p/>
     * <b>NOTE</b>: Although this override, which is almost a complete copy of UnaryOperatorNode#bindExpression()
     * seems to work (because we've also overridden UnaryOperatorNode#bindOperand() - see below),
     * there are references in this method to <code>operand</code>, which is the superclass'
     * reference to this operator's <b>first</b> operand - I didn't want to accumulate
     * "previousReliability", etc. This may need resolution later, esp if/when we allow user-defined
     * window functions.
     *
     * @param fromList            The query's FROM list
     * @param subqueryList        The subquery list being built as we find SubqueryNodes
     * @param aggregateVector    The aggregate list being built as we find AggregateNodes.
     *                           <b>NOTE</b>, we <b>don't</b> add ourselves to this vector.
     *                            We want to bind the wrapped aggregate (and operand, etc)
     *                            but we don't want to show up in this list as an aggregate.
     *                            The list will be handed to GroupByNode, which we don't want
     *                            doing the work.  Window function code will handle the window
     *                            function aggregates
     *
     * @return this bound function.
     * @throws StandardException for unenumerable reasons
     * @see org.apache.derby.impl.sql.compile.UnaryOperatorNode#bindExpression(FromList, SubqueryList, java.util.Vector)
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
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
         /* DO NOT Add ourselves to the aggregateVector before we do anything else */
//        aggregateVector.add(this);

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
     * We override this method because, unlike UnaryOperatorNode, window functions can
     * have more than one operand (n-ary operator). But, because we have to subclass
     * AggregateNode to fit in to the mold, we have to bind all our operands.<br/>
     * This is called from {@link #bindExpression(FromList, SubqueryList, java.util.Vector)}
     * above.
     * @see org.apache.derby.impl.sql.compile.UnaryOperatorNode#bindOperand(FromList, SubqueryList, java.util.Vector)
     */
    @Override
    protected void bindOperand(FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregateVector)
        throws StandardException {
        // bind all operands - aggregate operands will be added to aggregateVector
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

    protected abstract void setOperands(ValueNode[] operands) throws StandardException;
}
