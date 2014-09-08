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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;


/**
 * Superclass of any window function call.
 */
public abstract class WindowFunctionNode extends AggregateNode {

    private WindowNode window; // definition or reference

    /**
     * Initializer for a WindowFunctionNode
     * @param arg1 null (operand)
     * @param arg2 function mame (operator)
     * @param arg3 window node (definition or reference)
     */
    public void init(Object arg1, Object arg2, Object arg3) {
        super.init(arg1, arg2, null);
        this.window = (WindowNode)arg3;
    }

    public WindowNode getWindowNode() {
        return this.window;
    }

    /**
     * ValueNode override.
     * @see ValueNode#isConstantExpression
     */
    public boolean isConstantExpression()
    {
        return false;
    }

    /**
     * ValueNode override.
     * @see ValueNode#isConstantExpression
     */
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
     * @return window associated with this window function
     */
    public WindowNode getWindow() {
        return window;
    }


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
     * Get the generated ColumnReference to this window function after the
     * parent called replaceCallsWithColumnReferences().
     * <p/>
     * There are cases where this will not have been done because the tree has
     * been re-written to eliminate the window function, e.g. for this query:
     * <p/><pre>
     *     {@code SELECT * FROM t WHERE EXISTS
     *           (SELECT ROW_NUMBER() OVER () FROM t)}
     * </pre><p/>
     * in which case the top PRN of the subquery sitting over a
     * WindowResultSetNode just contains a RC which is boolean constant {@code
     * true}. This means that the replaceCallsWithColumnReferences will not
     * have been called for {@code this}, so the returned {@code generatedRef}
     * is null.
     *
     * @return the column reference
     */
    @Override
    public abstract ColumnReference getGeneratedRef();

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
                "##aggregate expression",
                node,
                getContextManager());
        }
        return resultColumns;
    }

    public abstract boolean isScalarAggregate();

    /**
     * Get the generated ResultColumn where this
     * aggregate now resides after a call to
     * replaceAggregatesWithColumnReference().
     *
     * @return the result column
     */
    @Override
    public abstract ResultColumn getGeneratedRC();

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
     * Get the aggregate node, if any, wrapped by this
     * window function
     *
     * @return the window aggregate function or <code>null</code>
     */
    public abstract AggregateNode getWrappedAggregate();
}
