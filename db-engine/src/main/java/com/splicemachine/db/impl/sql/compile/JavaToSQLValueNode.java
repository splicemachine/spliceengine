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

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;

/**
 * This node type converts a value from the Java domain to the SQL domain.
 */

@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public class JavaToSQLValueNode extends ValueNode
{
    JavaValueNode    javaNode;

    /**
     * Initializer for a JavaToSQLValueNode
     *
     * @param value        The Java value to convert to the SQL domain
     */
    public void init(Object value)
    {
        this.javaNode = (JavaValueNode) value;
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
     * @return        The modified expression
     *
     * @exception StandardException        Thrown on error
     */
    public ValueNode preprocess(int numTables,
                                FromList outerFromList,
                                SubqueryList outerSubqueryList,
                                PredicateList outerPredicateList)
                        throws StandardException
    {
        javaNode.preprocess(numTables,
                            outerFromList, outerSubqueryList,
                            outerPredicateList);

        return this;
    }

    /**
     * Do code generation for this conversion of a value from the Java to
     * the SQL domain.
     *
     * @param acb    The ExpressionClassBuilder for the class we're generating
     * @param mb the method  the expression will go into
     *
     * @exception StandardException        Thrown on error
     */

    public void generateExpression(ExpressionClassBuilder acb,
                                            MethodBuilder mb)
                                    throws StandardException
    {
        TypeId            resultType;
        String                        resultTypeName;

        /*
        ** Tell the Java node that it's value is being returned to the
        ** SQL domain.  This way, it knows whether the checking for a null
        ** receiver is to be done at the Java level or the SQL level.
        */
        javaNode.returnValueToSQLDomain();

        /* Generate the receiver, if any. */
        boolean hasReceiver = javaNode.generateReceiver(acb, mb);

        /*
        ** If the java expression has a receiver, we want to check whether
        ** it's null before evaluating the whole expression (to avoid
        ** a NullPointerException.
        */
        if (hasReceiver)
        {
            /*
            ** There is a receiver.  Generate a null SQL value to return
            ** in case the receiver is null.  First, create a field to hold
            ** the null SQL value.
            */
            String nullValueClass = getTypeCompiler().interfaceName();
            LocalField nullValueField =
                acb.newFieldDeclaration(Modifier.PRIVATE, nullValueClass);
            /*
            ** There is a receiver.  Generate the following to test
            ** for null:
            **
            **        (receiverExpression == null) ?
            */

            mb.conditionalIfNull();
            mb.getField(nullValueField);
            acb.generateNullWithExpress(mb, getTypeCompiler(),
                    getTypeServices(),null);


            /*
            ** We have now generated the expression to test, and the
            ** "true" side of the ?: operator.  Finish the "true" side
            ** so we can generate the "false" side.
            */
            mb.startElseCode();
        }

        TypeCompiler tc = getTypeCompiler();

        resultTypeName = tc.interfaceName();

        /* Allocate an object for re-use to hold the result of the conversion */
        LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, resultTypeName);

        /* Generate the expression for the Java value under us */
        javaNode.generateExpression(acb, mb);

        /* Generate the SQL value, which is always nullable */
        acb.generateDataValue(mb, tc,
                getTypeServices().getCollationType(), field);

        /*
        ** If there was a receiver, the return value will be the result
        ** of the ?: operator.
        */
        if (hasReceiver)
        {
            mb.completeConditional();
        }
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode for
     * how tree printing is supposed to work.
     *
     * @param depth        The depth of this node in the tree
     */

    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG)
        {
            super.printSubNodes(depth);

            printLabel(depth, "javaNode: ");
            javaNode.treePrint(depth + 1);
        }
    }

    /**
     * Get the JavaValueNode that lives under this JavaToSQLValueNode.
     *
     * @return    The JavaValueNode that lives under this node.
     */

    public JavaValueNode getJavaValueNode()
    {
        return javaNode;
    }

    // Is this a system function (defined in the SYSFUN schema)?
    public boolean isSystemFunction() {
        if (javaNode != null &&
            javaNode instanceof StaticMethodCallNode) {
            StaticMethodCallNode smc = (StaticMethodCallNode)javaNode;
            return smc.isSystemFunction();
        }
        return false;
    }

    /**
     * Bind this expression.  This means binding the sub-expressions,
     * as well as figuring out what the return type is for this expression.
     *
     * @param fromList        The FROM list for the query this
     *                expression is in, for binding columns.
     * @param subqueryList        The subquery list being built as we find
     *                            SubqueryNodes
     * @param aggregateVector    The aggregate vector being built as we find AggregateNodes
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode>    aggregateVector) throws StandardException {
        // method invocations are not allowed in ADD TABLE clauses.
        // And neither are field references.
        javaNode.checkReliability(this);

        /* Bind the expression under us */
        javaNode = javaNode.bindExpression(fromList, subqueryList, aggregateVector);
        if ( javaNode instanceof StaticMethodCallNode )
        {
            AggregateNode   agg = ((StaticMethodCallNode) javaNode).getResolvedAggregate();

            if ( agg != null )
            {
                 return agg.bindExpression( fromList, subqueryList, aggregateVector );
            }
        }

        DataTypeDescriptor dts = javaNode.getDataType();
        if (dts == null)
        {
            throw StandardException.newException(SQLState.LANG_NO_CORRESPONDING_S_Q_L_TYPE,
                javaNode.getJavaTypeName());
        }

        TypeDescriptor catalogType = dts.getCatalogType();

        if ( catalogType.isRowMultiSet() || (catalogType.getTypeName().equals( "java.sql.ResultSet" )) )
        {
            throw StandardException.newException(SQLState.LANG_TABLE_FUNCTION_NOT_ALLOWED);
        }
        
        setType(dts);

        // For functions returning string types we should set the collation to match the 
        // java method's schema DERBY-2972. This is propogated from 
        // RoutineAliasInfo to javaNode.
        if (dts.getTypeId().isStringTypeId()) {
            this.setCollationInfo(javaNode.getCollationType(),
                    StringDataValue.COLLATION_DERIVATION_IMPLICIT);
        }

        return this;
    }

    /**
     * Remap all ColumnReferences in this tree to be clones of the
     * underlying expression.
     *
     * @return ValueNode            The remapped expression tree.
     *
     * @exception StandardException            Thrown on error
     */
    public ValueNode remapColumnReferencesToExpressions()
        throws StandardException
    {
        javaNode = javaNode.remapColumnReferencesToExpressions();
        return this;
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
     *
     * @param referencedTabs    JBitSet with bit map of referenced FromTables
     * @param simplePredsOnly    Whether or not to consider method
     *                            calls, field references and conditional nodes
     *                            when building bit map
     *
     * @return boolean        Whether or not source.expression is a ColumnReference
     *                        or a VirtualColumnNode.
     *
     * @exception StandardException            Thrown on error
     */
    public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
        throws StandardException
    {
        return javaNode.categorize(referencedTabs, simplePredsOnly);
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
     * @exception StandardException    thrown on error
     */
    protected int getOrderableVariantType() throws StandardException
    {
        return javaNode.getOrderableVariantType();
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (javaNode != null) {
            javaNode = (JavaValueNode)javaNode.accept(v, this);
        }
    }
        
    /**
     * {@inheritDoc}
     */
    protected boolean isEquivalent(ValueNode o)
    {
        // anything in the java domain is not equiavlent.
        return false;//this == o;
    }

    public List<? extends QueryTreeNode> getChildren() {
        return Collections.singletonList(javaNode);
    }

    @Override
    public QueryTreeNode getChild(int index) {
        assert index == 0;
        return javaNode;
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        assert index == 0;
        javaNode = (JavaValueNode) newValue;
    }

    @Override
    public boolean isConstantOrParameterTreeNode() {
        return javaNode.isConstantOrParameterTreeNode();
    }
}
