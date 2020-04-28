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

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.store.access.Qualifier;

import java.util.Collections;
import java.util.List;

/**
 * ConstantNode holds literal constants as well as nulls.
 * <p>
 * A NULL from the parser may not yet know its type; that
 * must be set during binding, as it is for parameters.
 * <p>
 * the DataValueDescriptor methods want to throw exceptions
 * when they are of the wrong type, but to do that they
 * must check typeId when the value is null, rather than
 * the instanceof check they do for returning a valid value.
 * <p>
 * For code generation, we generate a static field.  Then we set the 
 * field be the proper constant expression (something like <code>
 * getDatavalueFactory().getCharDataValue("hello", ...)) </code>)
 * in the constructor of the generated method.  Ideally
 * we would have just 
 */
public abstract class ConstantNode extends ValueNode
{
    DataValueDescriptor    value;

    /*
    ** In case generateExpression() is called twice (something
    ** that probably wont happen but might), we will cache
    ** our generated expression and just return a reference
    ** to the field that holds our value (instead of having
    ** two fields holding the same constant).
    */

    /**
     * Initializer for non-numeric types
     *
     * @param typeId    The Type ID of the datatype
     * @param nullable    True means the constant is nullable
     * @param maximumWidth    The maximum number of bytes in the data value
     *
     * @exception StandardException
     */
    public void init(
            Object typeId,
            Object nullable,
            Object maximumWidth)
        throws StandardException
    {
        setType((TypeId) typeId,
                (Boolean) nullable,
                (Integer) maximumWidth);

    }

    /**
     * Constructor for untyped nodes, which contain little information
     *
     */
    ConstantNode()
    {
        super();
    }

    /**
     * Set the value in this ConstantNode.
     */
    void setValue(DataValueDescriptor value)
    {
        this.value = value;
    }

    /**
      * Get the value in this ConstantNode
      */
    public DataValueDescriptor    getValue()
    {
        return    value;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return    This object as a String
     */
    @Override
    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return "value: " + value + "\n" +
                super.toString();
        }
        else
        {
            return "";
        }
    }

    /**
     * Return whether or not this expression tree is cloneable.
     *
     * @return boolean    Whether or not this expression tree is cloneable.
     */
    public boolean isCloneable()
    {
        return true;
    }

    /**
     * Return a clone of this node.
     *
     * @return ValueNode    A clone of this node.
     *
     */
    public ValueNode getClone()
    {
        /* All constants can simply be reused */
        return this;
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
     * @exception StandardException        Thrown on error. Although this class
     * doesn't throw this exception, it's subclasses do and hence this method
     * signature here needs to have throws StandardException
     */
    @Override
    public ValueNode bindExpression(
            FromList fromList, SubqueryList subqueryList,
            List<AggregateNode> aggregateVector)
    throws StandardException
    {
        /*
        ** This has to be here for binding to work, but it doesn't
        ** have to do anything, because the datatypes of constant nodes
        ** are pre-generated by the parser.
        */
        return this;
    }

    /**
     * Return whether or not this expression tree represents a constant expression.
     *
     * @return    Whether or not this expression tree represents a constant expression.
     */
    public boolean isConstantExpression()
    {
        return true;
    }

    /** @see ValueNode#constantExpression */
    public boolean constantExpression(PredicateList whereClause)
    {
        return true;
    }

    /**
     * For a ConstantNode, we generate the equivalent literal value.
     * A null is generated as a Null value cast to the type of
     * the constant node.
     * The subtypes of ConstantNode generate literal expressions
     * for non-null values.
     *
     * @param acb    The ExpressionClassBuilder for the class being built
     * @param mb    The method the code to place the code
     *
     * @exception StandardException        Thrown on error
     */
    public void generateExpression
    (
        ExpressionClassBuilder    acb,
        MethodBuilder         mb
    ) throws StandardException
    {
        /* Are we generating a SQL null value? */
        if (isNull())
        {
            acb.generateNull(mb, getTypeCompiler(),
                    getTypeServices());
        }
        else
        {
            generateConstant(acb, mb);    // ask sub type to give a constant,
                                        // usually a literal like 'hello'

            acb.generateDataValue(mb, getTypeCompiler(),
                    getTypeServices().getCollationType(), (LocalField) null);
        }
    }

    /**
     * This generates the proper constant.  It is implemented
     * by every specific constant node (e.g. IntConstantNode).
     *
     * @param acb    The ExpressionClassBuilder for the class being built
     * @param mb    The method the code to place the code
     *
     * @exception StandardException        Thrown on error
     */
    abstract void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
        throws StandardException;

    /**
     * Return whether or not this node represents a typed null constant.
     *
     */
    boolean isNull()
    {
        return (value == null || value.isNull());
    }

    /**
     * Return the variant type for the underlying expression.
     * The variant type can be:
     *        VARIANT                - variant within a scan
     *                              (method calls and non-static field access)
     *        SCAN_INVARIANT        - invariant within a scan
     *                              (column references from outer tables)
     *        QUERY_INVARIANT        - invariant within the life of a query
     *        VARIANT                - immutable
     *
     * @return    The variant type for the underlying expression.
     */
    protected int getOrderableVariantType()
    {
        // Constants are constant for the life of the query
        return Qualifier.CONSTANT;
    }
        
    protected boolean isEquivalent(ValueNode o) throws StandardException
    {
        if (isSameNodeType(o)) {
            ConstantNode other = (ConstantNode)o;

            // value can be null which represents a SQL NULL value.
            return ( (other.getValue() == null && getValue() == null) ||
                     (other.getValue() != null &&
                             other.getValue().compare(getValue()) == 0) );
        }
        return false;
    }

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

    @Override
    public long nonZeroCardinality(long numberOfRows) throws StandardException {
        return 1;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean isConstantOrParameterTreeNode() {
        return true;
    }
}
