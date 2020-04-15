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

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Parser;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DefaultDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.catalog.types.DefaultInfoImpl;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.List;

/**
 * DefaultNode represents a column/parameter default.
 */
@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public  class DefaultNode extends ValueNode
{
    private String        columnName;
    private String        defaultText;
    private ValueNode    defaultTree;

    /**
     * Initializer for a column/parameter default.
     *
     * @param defaultTree            Query tree for default
     * @param defaultText    The text of the default.
     */
    public void init(
                    Object defaultTree,
                    Object defaultText)
    {
        this.defaultTree = (ValueNode) defaultTree;
        this.defaultText = (String) defaultText;
    }

    /**
     * Initializer for insert/update
     *
     */
    public void init(Object columnName)
    {
        this.columnName = (String) columnName;
    }

    /**
      * Get the text of the default.
      */
    public    String    getDefaultText()
    {
        return    defaultText;
    }

    /**
     * Get the query tree for the default.
     *
     * @return The query tree for the default.
     */
    ValueNode getDefaultTree()
    {
        return defaultTree;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return    This object as a String
     */

    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return "columnName: " + columnName + "\n" +
                "defaultText: " + defaultText + "\n" +
                super.toString();
        }
        else
        {
            return "";
        }
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth        The depth of this node in the tree
     */
    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG)
        {
            super.printSubNodes(depth);

            if (defaultTree != null) {
                printLabel(depth, "defaultTree:");
                defaultTree.treePrint(depth + 1);
            }
        }
    }


    /**
     * Bind this expression.  This means binding the sub-expressions,
     * as well as figuring out what the return type is for this expression.
     * In this case, there are no sub-expressions, and the return type
     * is already known, so this is just a stub.
     *
     * @param fromList        The FROM list for the query this
     *                expression is in, for binding columns.
     * @param subqueryList        The subquery list being built as we find SubqueryNodes
     * @param aggregateVector    The aggregate vector being built as we find AggregateNodes
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on failure
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        ColumnDescriptor cd;
        TableDescriptor td;

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(!fromList.isEmpty(),
                "fromList expected to be non-empty");
            if (! (fromList.elementAt(0) instanceof FromBaseTable))
            {
                SanityManager.THROWASSERT(
                    "fromList.elementAt(0) expected to be instanceof FromBaseTable, not " +
                    fromList.elementAt(0).getClass().getName());
            }

        }
        // Get the TableDescriptor for the target table
        td = ((FromBaseTable) fromList.elementAt(0)).getTableDescriptor();

        // Get the ColumnDescriptor for the column
        cd = td.getColumnDescriptor(columnName);
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(cd != null,
                "cd expected to be non-null");
        }

        /* If we have the default text, then parse and bind it and
         * return the tree.
         */
        DefaultInfoImpl defaultInfo = (DefaultInfoImpl) cd.getDefaultInfo();
        if (defaultInfo != null)
        {
            String        defaultText = defaultInfo.getDefaultText();
            ValueNode    defaultTree = parseDefault(defaultText, getLanguageConnectionContext(),
                                                   getCompilerContext());

            /* Query is dependent on the DefaultDescriptor */
            DefaultDescriptor defaultDescriptor = cd.getDefaultDescriptor(
                                                    getDataDictionary());
            getCompilerContext().createDependency(defaultDescriptor);

            return defaultTree.bindExpression(
                                    fromList,
                                    subqueryList,
                                    aggregateVector);
        }
        else
        {
            // Default is null
            return (ValueNode) getNodeFactory().getNode(
                                        C_NodeTypes.UNTYPED_NULL_CONSTANT_NODE,
                                        getContextManager());
        }
    }

    /**
      *    Parse a default and turn it into a query tree.
      *
      *    @param    defaultText            Text of Default.
      * @param    lcc                    LanguageConnectionContext
      * @param    cc                    CompilerContext
      *
      * @return    The parsed default as a query tree.
      *
      * @exception StandardException        Thrown on failure
      */
    public    static ValueNode    parseDefault
    (
        String                        defaultText,
        LanguageConnectionContext    lcc,
        CompilerContext                cc
    )
        throws StandardException
    {
        Parser                        p;
        ValueNode                    defaultTree;

        /* Get a Statement to pass to the parser */

        /* We're all set up to parse. We have to build a compilable SQL statement
         * before we can parse -  So, we goober up a VALUES defaultText.
         */
        String values = "VALUES " + defaultText;

        /*
        ** Get a new compiler context, so the parsing of the select statement
        ** doesn't mess up anything in the current context (it could clobber
        ** the ParameterValueSet, for example).
        */
        CompilerContext newCC = lcc.pushCompilerContext();

        p = newCC.getParser();


        /* Finally, we can call the parser */
        // Since this is always nested inside another SQL statement, so topLevel flag
        // should be false
        Visitable qt = p.parseStatement(values);
        if (SanityManager.DEBUG)
        {
            if (! (qt instanceof CursorNode))
            {
                SanityManager.THROWASSERT(
                    "qt expected to be instanceof CursorNode, not " +
                    qt.getClass().getName());
            }
            CursorNode cn = (CursorNode) qt;
            if (! (cn.getResultSetNode() instanceof RowResultSetNode))
            {
                SanityManager.THROWASSERT(
                    "cn.getResultSetNode() expected to be instanceof RowResultSetNode, not " +
                    cn.getResultSetNode().getClass().getName());
            }
        }

        defaultTree = ((ResultColumn)
                            ((CursorNode) qt).getResultSetNode().getResultColumns().elementAt(0)).
                                    getExpression();

        lcc.popCompilerContext(newCC);

        return    defaultTree;
    }

    /**
     * @exception StandardException        Thrown on failure
     */
    public void generateExpression(ExpressionClassBuilder acb,
                                            MethodBuilder mb)
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.THROWASSERT(
                "generateExpression not expected to be called");
        }
    }

    /**
     * @inheritDoc
     */
    protected boolean isEquivalent(ValueNode other)
    {
        return this == other;
    }

    public List<? extends QueryTreeNode> getChildren() {
        return Collections.singletonList(defaultTree);
    }

    @Override
    public QueryTreeNode getChild(int index) {
        assert index == 0;
        return defaultTree;
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        assert index == 0;
        defaultTree = (ValueNode) newValue;
    }
}
