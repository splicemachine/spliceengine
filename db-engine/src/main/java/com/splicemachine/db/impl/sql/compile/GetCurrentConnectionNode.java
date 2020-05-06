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

import com.splicemachine.db.iapi.sql.compile.CompilerContext;

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.classfile.VMOpcode;


import com.splicemachine.db.iapi.store.access.Qualifier;

import com.splicemachine.db.iapi.util.JBitSet;

import com.splicemachine.db.iapi.reference.ClassName;

import java.util.Collections;
import java.util.List;

/**
 * This node represents a unary getCurrentConnection operator
 * RESOLVE - parameter will always be null for now.  Someday
 * we may want to allow user to specify which of their connections
 * they want.  Assume that we will use a String.
 *
 */

public final class GetCurrentConnectionNode extends JavaValueNode
{
    /**
     * Constructor for a GetCurrentConnectionNode
     *
     */

    public GetCurrentConnectionNode()
    {
        /*
        ** The result type of getCurrentConnection is
        ** java.sql.Connection
        */

        setJavaTypeName("java.sql.Connection");
    }

    /**
     * Bind this operator
     *
     * @param fromList            The query's FROM list
     * @param subqueryList        The subquery list being built as we find SubqueryNodes
     * @param aggregateVector    The aggregate vector being built as we find AggregateNodes
     *
     * @exception StandardException        Thrown on error
     */
    @Override
    public JavaValueNode bindExpression(FromList fromList,
                                        SubqueryList subqueryList,
                                        List<AggregateNode> aggregateVector) throws StandardException {
        return this;
    }

    /**
     * Preprocess an expression tree.  We do a number of transformations
     * here (including subqueries, IN lists, LIKE and BETWEEN) plus
     * subquery flattening.
     * NOTE: This is done before the outer ResultSetNode is preprocessed.
     *
     * @param    numTables            Number of tables in the DML Statement
     * @param    outerFromList        FromList from outer query block
     * @param    outerSubqueryList    SubqueryList from outer query block
     * @param    outerPredicateList    PredicateList from outer query block
     *
     * @exception StandardException        Thrown on error
     */
    public void preprocess(int numTables,
                                FromList outerFromList,
                                SubqueryList outerSubqueryList,
                                PredicateList outerPredicateList)
                    throws StandardException
    {
    }

    /**
     * Categorize this predicate.  Initially, this means
     * building a bit map of the referenced tables for each predicate.
     * If the source of this ColumnReference (at the next underlying level)
     * is not a ColumnReference or a VirtualColumnNode then this predicate
     * will not be pushed down.
     *
     * For example, in:
     *        select * from (select 1 from s) a (x) where x = 1
     * we will not push down x = 1.
     * NOTE: It would be easy to handle the case of a constant, but if the
     * inner SELECT returns an arbitrary expression, then we would have to copy
     * that tree into the pushed predicate, and that tree could contain
     * subqueries and method calls.
     * RESOLVE - revisit this issue once we have views.
     *
     * @param referencedTabs    JBitSet with bit map of referenced FromTables
     * @param simplePredsOnly    Whether or not to consider method
     *                            calls, field references and conditional nodes
     *                            when building bit map
     *
     * @return boolean        Whether or not source.expression is a ColumnReference
     *                        or a VirtualColumnNode.
     */
    public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
    {
        return false;
    }

    /**
     * Remap all ColumnReferences in this tree to be clones of the
     * underlying expression.
     *
     * @return JavaValueNode            The remapped expression tree.
     *
     */
    public JavaValueNode remapColumnReferencesToExpressions()
    {
        return this;
    }

    /**
     * Bind a ? parameter operand of the char_length function.
     */

    void bindParameter()
    {
    }

    /**
     * Return the variant type for the underlying expression.
     * The variant type can be:
     *        VARIANT                - variant within a scan
     *                              (method calls and non-static field access)
     *        SCAN_INVARIANT        - invariant within a scan
     *                              (column references from outer tables)
     *        QUERY_INVARIANT        - invariant within the life of a query
     *                              (constant expressions)
     *
     * @return    The variant type for the underlying expression.
     */
    protected int getOrderableVariantType()
    {
        return Qualifier.QUERY_INVARIANT;
    }
    /**
     *
     * @see ConstantNode#generateExpression
     *
     * @param acb    The ExpressionClassBuilder for the class being built
     * @param mb    The method the code to place the code
     *
     * @exception StandardException        Thrown on error
     */
    public void generateExpression(ExpressionClassBuilder acb,
                                            MethodBuilder mb)
                                    throws StandardException
    {
        mb.pushThis();
        mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "getCurrentConnection", getJavaTypeName(), 0);
    }

    /**
        Check the reliability type of this java value.

        @exception StandardException        Thrown on error

        @see com.splicemachine.db.iapi.sql.compile.CompilerContext
    */
    public void checkReliability(ValueNode sqlNode)
        throws StandardException {
        sqlNode.checkReliability("getCurrentConnection()",
            CompilerContext.CURRENT_CONNECTION_ILLEGAL);
    }

    @Override
    public List<? extends QueryTreeNode> getChildren() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public QueryTreeNode getChild(int index) {
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        throw new UnsupportedOperationException("Not Implemented");
    }
}
