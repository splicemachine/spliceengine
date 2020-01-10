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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Superclass of all window functions.
 */
public abstract class WindowFunctionNode extends AggregateNode {

    private WindowNode window; // definition or reference
    private boolean ignoreNulls;

    /**
     * Initializer. AggregateNode override.
     *
     * @param arg1 UnaryNode operand (to satisfy AggregateNode init())
     * @param arg2 The function's definition class
     * @param arg3 function name
     * @param arg4 isDistinct
     * @param arg5 The window definition or reference
     * @param arg6 Respect or ignore nulls in function arguments
     * @throws StandardException
     */
    public void init(Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) throws StandardException {
        super.init(arg1, arg2, arg3, arg4);
        this.window = (WindowNode)arg5;
        this.ignoreNulls = (boolean) arg6;
    }

    // TODO: JC- remove if "initialize*()" works
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
        // this will visit function operand
        super.acceptChildren(v);

        // this will visit all cols in over clause, some of which may be operands (i.e., ranking functions).
        for (OrderedColumn oc : getWindow().getOverColumns()) {
            if (oc.getColumnExpression() != null) {
                ValueNode visited = (ValueNode) oc.getColumnExpression().accept(v, this);
                oc.setColumnExpression(visited);
            }
        }
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
    public abstract List<ValueNode> getOperands();

    public abstract void replaceOperand(ValueNode oldVal, ValueNode newVal);

    /**
     * Set window associated with this window function call.
     * @param wdn window definition
     */
    public void setWindow(WindowDefinitionNode wdn) {
        this.window = wdn;
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
     * @param tableNumber The tableNumber for the new ColumnReference
     * @param nestingLevel this node's nesting level
     * @return the newly generated CR.
     * @throws StandardException
     */
    public ValueNode replaceCallWithColumnReference(int tableNumber,
                                                    int nestingLevel) throws StandardException {
         /*
          * This call is idempotent.  Do the right thing if we have already
          * replaced ourselves.
          */
        if (generatedRef == null) {
            generatedRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##SQLColWinGen_" + getSQLName(),
                this,
                getContextManager());
            generatedRC.markGenerated();

            // The generated column reference. It's returned from this method but is also maintained
            // by this aggregate node
            generatedRef = (ColumnReference) getNodeFactory().getNode(
                C_NodeTypes.COLUMN_REFERENCE,
                generatedRC.getName(),
                null,
                getContextManager());

            generatedRef.setSource(generatedRC);
            generatedRef.setNestingLevel(nestingLevel);
            generatedRef.setSourceLevel(0);

            if (tableNumber >= 0) {
                generatedRef.setTableNumber(tableNumber);
            }

            // Mark the ColumnReference as being generated to replace a call to
            // a window function
            generatedRef.markGeneratedToReplaceWindowFunctionCall();
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
     * @see com.splicemachine.db.impl.sql.compile.UnaryOperatorNode#bindExpression(FromList, SubqueryList, java.util.List)
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
            // TODO: JC - this.getClass() gives the subclass vvv. shouldn't this be WindowFunctionNode.class?
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

        // Add all the aggregates in the over clause to the agg list so that GroupByNode will handle
        for (AggregateNode aggregateNode : getAggregatesInOverClause()) {
            aggregateNode.bindExpression(fromList, subqueryList, aggregateVector);
        }

        return this;
    }

    private Collection<AggregateNode> getAggregatesInOverClause() {
        List<AggregateNode> aggs = new ArrayList<>();
        for (OrderedColumn oc : getWindow().getOverColumns()) {
            ValueNode exp = oc.getColumnExpression();
            if (exp instanceof AggregateNode) {
                aggs.add((AggregateNode)exp);
            }
        }
        return aggs;
    }

    /**
     * We override this method because, unlike UnaryOperatorNode, window functions can
     * have more than one operand (n-ary operator). But, because we have to subclass
     * AggregateNode to fit in to the mold, we have to bind all our operands.<br/>
     * This is called from {@link #bindExpression(FromList, SubqueryList, java.util.List)}
     * above.
     * @see com.splicemachine.db.impl.sql.compile.UnaryOperatorNode#bindOperand(FromList, SubqueryList, java.util.List)
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

    public boolean isIgnoreNulls() {
        return ignoreNulls;
    }

    /**
     * Override this method to provide window function specific generic set of arguments
     * to the Splice-side window function.
     * @return a map of argName -> argument in which a specific function is interested.
     */
    public FormatableHashtable getFunctionSpecificArgs() {
        return new FormatableHashtable();
    }
}
