/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.List;

/**
 * This node represents an Array Initialization Node.  It supports arrays in the format
 * [1,1,1] and will set them in the corresponding row to a SQLArray
 *
 *
 */

@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public class ArrayConstantNode extends ValueNode {
    ValueNodeList    argumentsList; //this is the list of arguments to the function. We are interested in the first not-null argument

    /**
     * The generated method will generate code to call coalesce on
     * this non-parameter argument.
     */
    private int firstNonParameterNodeIdx = -1;

    /**
     * Initializer for a ArrayNode
     *
     * @param functionName    Tells if the function was called with name COALESCE or with name VALUE
     * @param argumentsList    The list of arguments to the coalesce/value function
     */
    public void init(Object argumentsList) throws StandardException {
        if (argumentsList instanceof TypeId) {
            this.setNullability(true);
        } else {
            this.argumentsList = (ValueNodeList) argumentsList;
        }
    }

    /**
     * Binding this expression means setting the result DataTypeServices.
     * In this case, the result type is based on the rules in the table listed earlier.
     *
     * @param fromList            The FROM list for the statement.
     * @param subqueryList        The subquery list being built as we find SubqueryNodes.
     * @param aggregateVector    The aggregate vector being built as we find AggregateNodes.
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode>    aggregateVector) throws StandardException {
        //bind all the arguments
        argumentsList.bindExpression(fromList, subqueryList, aggregateVector);
        // Set the type?
        setType(
                TypeId.getBuiltInTypeId(Types.ARRAY),
                argumentsList==null, // need to fix JL-TODO
                -1);
        //check if all the arguments are parameters. If yes, then throw an exception
        // Need to add exception TODO JL
        if (argumentsList.containsAllParameterNodes())
            throw StandardException.newException(SQLState.LANG_DB2_MULTINARY_FUNCTION_ALL_PARAMS, "ArrayConstantNode");

        int argumentsListSize = argumentsList.size();
        //find the first non-param argument. The generated method will generate code to call coalesce on this argument
        for (int index = 0; index < argumentsListSize; index++) {
            if (!(((ValueNode) argumentsList.elementAt(index)).requiresTypeFromContext())) {
                firstNonParameterNodeIdx = index;
                break;
            }
        }

        //make sure these arguments are compatible to each other before array formation can be allowed
        for (int index = 0; index < argumentsListSize; index++) {
            if (((ValueNode) argumentsList.elementAt(index)).requiresTypeFromContext()) //since we don't know the type of param, can't check for compatibility
                continue;
            argumentsList.compatible((ValueNode) argumentsList.elementAt(index));
        }

        dataTypeServices.setChildren(new DataTypeDescriptor[]{argumentsList.getTypeServices()});


        //set all the parameter types to the type of the result type
        for (int index = 0; index < argumentsListSize; index++) {
            if (((ValueNode) argumentsList.elementAt(index)).requiresTypeFromContext()) {
                ((ValueNode)argumentsList.elementAt(index)).setType(getTypeServices());
            }
        }


        return this;
    }

    /**
     * Do code generation for coalese/value
     *
     * @param acb    The ExpressionClassBuilder for the class we're generating
     * @param mb    The method the expression will go into
     *
     * @exception StandardException        Thrown on error
     */

    public void generateExpression(ExpressionClassBuilder acb,
                                   MethodBuilder mb)
            throws StandardException {

                /* Are we generating a SQL null value? */
        if (argumentsList == null) // bail out
        {
            acb.generateNull(mb, getTypeCompiler(),
                    getTypeServices());
            return;
        }

        int            argumentsListSize = argumentsList.size();
        String        receiverType = ClassName.DataValueDescriptor;
        String        argumentsListInterfaceType = ClassName.DataValueDescriptor + "[]";

        // Generate the code to build the array
        LocalField arrayField =
                acb.newFieldDeclaration(Modifier.PRIVATE, argumentsListInterfaceType);

        /* The array gets created in the constructor.
         * All constant elements in the array are initialized
         * in the constructor.
         */
        /* Assign the initializer to the DataValueDescriptor[] field */
        MethodBuilder cb = acb.getConstructor();
        cb.pushNewArray(ClassName.DataValueDescriptor, argumentsListSize);
        cb.setField(arrayField);

        /* Set the array elements that are constant */
        int numConstants = 0;
        MethodBuilder nonConstantMethod = null;
        MethodBuilder currentConstMethod = cb;
        for (int index = 0; index < argumentsListSize; index++) {
            MethodBuilder setArrayMethod;
            if (argumentsList.elementAt(index) instanceof ConstantNode) {
                numConstants++;

                /*if too many statements are added  to a  method,
                *size of method can hit  65k limit, which will
                *lead to the class format errors at load time.
                *To avoid this problem, when number of statements added
                *to a method is > 2048, remaing statements are added to  a new function
                *and called from the function which created the function.
                *See Beetle 5135 or 4293 for further details on this type of problem.
                */
                if(currentConstMethod.statementNumHitLimit(1)) {
                    MethodBuilder genConstantMethod = acb.newGeneratedFun("void", Modifier.PRIVATE);
                    currentConstMethod.pushThis();
                    currentConstMethod.callMethod(VMOpcode.INVOKEVIRTUAL,
                            (String) null,
                            genConstantMethod.getName(),
                            "void", 0);
                    //if it is a generate function, close the metod.
                    if(currentConstMethod != cb){
                        currentConstMethod.methodReturn();
                        currentConstMethod.complete();
                    }
                    currentConstMethod = genConstantMethod;
                }
                setArrayMethod = currentConstMethod;
            } else {
                if (nonConstantMethod == null)
                    nonConstantMethod = acb.newGeneratedFun("void", Modifier.PROTECTED);
                setArrayMethod = nonConstantMethod;

            }

            setArrayMethod.getField(arrayField);
            ((ValueNode) argumentsList.elementAt(index)).generateExpression(acb, setArrayMethod);
            setArrayMethod.upCast(receiverType);
            setArrayMethod.setArrayElement(index);
        }

        //if a generated function was created to reduce the size of the methods close the functions.
        if(currentConstMethod != cb){
            currentConstMethod.methodReturn();
            currentConstMethod.complete();
        }

        if (nonConstantMethod != null) {
            nonConstantMethod.methodReturn();
            nonConstantMethod.complete();
            mb.pushThis();
            mb.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, nonConstantMethod.getName(), "void", 0);
        }

        /*
        **  Call the method for coalesce/value function.
        **    First generate following
        **    <first non-param argument in the list>.method(<all the arguments>, <resultType>)
        **    Next, if we are dealing with result type that is variable length, then generate a call to setWidth.
        */

        // coalesce will be called on this non-parameter argument
        ((ValueNode) argumentsList.elementAt(firstNonParameterNodeIdx)).
                generateExpression(acb, mb);

        mb.upCast(ClassName.DataValueDescriptor);

        mb.getField(arrayField); // first arg to the coalesce function

        //Following is for the second arg. This arg will be used to pass the return value.
        //COALESCE method expects this to be initialized to NULL SQLxxx type object.
        LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, receiverType);
        acb.generateNull(mb, getTypeCompiler(),
                getTypeServices());
        mb.upCast(ClassName.DataValueDescriptor);
        mb.putField(field);

        mb.callMethod(VMOpcode.INVOKEINTERFACE, receiverType, "setArray", receiverType, 2);
        if (getTypeId().variableLength())//since result type is variable length, generate setWidth code.
        {
            boolean isNumber = getTypeId().isNumericTypeId();
            // to leave the DataValueDescriptor value on the stack, since setWidth is void
            mb.dup();

            mb.push(isNumber ? getTypeServices().getPrecision() : getTypeServices().getMaximumWidth());
            mb.push(getTypeServices().getScale());
            mb.push(true);
            mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.VariableSizeDataValue, "setWidth", "void", 3);
        }
    }

    /*
        print the non-node subfields
     */
    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return
                    "array: \n" +
                            "firstNonParameterNodeIdx: " + firstNonParameterNodeIdx + "\n" +
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

            printLabel(depth, "argumentsList: ");
            argumentsList.treePrint(depth + 1);
        }
    }


    /**
     * {@inheritDoc}
     */
    protected boolean isEquivalent(ValueNode o) throws StandardException
    {
        if (!isSameNodeType(o))
        {
            return false;
        }

        CoalesceFunctionNode other = (CoalesceFunctionNode)o;

        return argumentsList.isEquivalent(other.argumentsList);

    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (argumentsList != null)
            argumentsList = (ValueNodeList) argumentsList.accept(v, this);
    }

    /**
     * Categorize this predicate.
     *
     * @see ValueNode#categorize(JBitSet, boolean)
     */
    public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
            throws StandardException
    {
        return argumentsList.categorize(referencedTabs, simplePredsOnly);
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
     * @return                        The modified expression
     *
     * @exception StandardException        Thrown on error
     */
    public ValueNode preprocess(int numTables,
                                FromList outerFromList,
                                SubqueryList outerSubqueryList,
                                PredicateList outerPredicateList)
            throws StandardException
    {
        if (argumentsList != null)
            argumentsList.preprocess(
                    numTables,
                    outerFromList,
                    outerSubqueryList,
                    outerPredicateList);

        return this;
    }

    /**
     * Remap all the {@code ColumnReference}s in this tree to be clones of
     * the underlying expression.
     *
     * @return the remapped tree
     * @throws StandardException if an error occurs
     */
    public ValueNode remapColumnReferencesToExpressions()
            throws StandardException
    {
        argumentsList = argumentsList.remapColumnReferencesToExpressions();
        return this;
    }

    public List<? extends QueryTreeNode> getChildren() {

        return argumentsList.getNodes();
    }

    @Override
    public QueryTreeNode getChild(int index) {
        return argumentsList.elementAt(index);
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        argumentsList.setElementAt(newValue, index);
    }

    @Override
    public long nonZeroCardinality(long numberOfRows) throws StandardException {

        long c = 0;
        for (int i = 0; i < argumentsList.size(); ++i) {
            ValueNode v = (ValueNode) argumentsList.elementAt(i);
            c = Math.max(c, v.nonZeroCardinality(numberOfRows));
        }
        return c;
    }

    @Override
    public boolean isConstantOrParameterTreeNode() {
        if (argumentsList!= null && !argumentsList.containsOnlyConstantAndParamNodes())
            return false;
        return true;
    }
}


