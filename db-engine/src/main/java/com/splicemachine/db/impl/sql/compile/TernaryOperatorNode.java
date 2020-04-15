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
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.util.ReuseFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A TernaryOperatorNode represents a built-in ternary operators.
 * This covers  built-in functions like substr().
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 */

@SuppressFBWarnings(value="HE_INHERITS_EQUALS_USE_HASHCODE", justification="DB-9277")
public class TernaryOperatorNode extends OperatorNode
{
    String        operator;
    String        methodName;
    int            operatorType;
    ValueNode    receiver;

    ValueNode    leftOperand;
    ValueNode    rightOperand;

    String        resultInterfaceType;
    String        receiverInterfaceType;
    String        leftInterfaceType;
    String        rightInterfaceType;
    int            trimType;

    public static final int TRIM = 0;
    public static final int LOCATE = 1;
    public static final int SUBSTRING = 2;
    public static final int LIKE = 3;
    public static final int TIMESTAMPADD = 4;
    public static final int TIMESTAMPDIFF = 5;
    public static final int REPLACE = 6;
    public static final int RIGHT = 7;
    public static final int LEFT = 8;
    static final String[] TernaryOperators = {"trim", "LOCATE", "substring", "like", "TIMESTAMPADD", "TIMESTAMPDIFF", "replace", "right", "left"};
    static final String[] TernaryMethodNames = {"ansiTrim", "locate", "substring", "like", "timestampAdd", "timestampDiff", "replace", "right", "left"};

    static final String[] TernaryResultType = {
            ClassName.StringDataValue,
            ClassName.NumberDataValue,
            ClassName.ConcatableDataValue,
            ClassName.BooleanDataValue,
            ClassName.DateTimeDataValue,
            ClassName.NumberDataValue,
            ClassName.ConcatableDataValue,
            ClassName.StringDataValue,
            ClassName.StringDataValue
    };
    static final String[][] TernaryArgType = {
            {ClassName.StringDataValue, ClassName.StringDataValue, "java.lang.Integer"},
            {ClassName.ConcatableDataValue, ClassName.ConcatableDataValue, ClassName.NumberDataValue},
            {ClassName.ConcatableDataValue, ClassName.NumberDataValue, ClassName.NumberDataValue},
            {ClassName.DataValueDescriptor, ClassName.DataValueDescriptor, ClassName.DataValueDescriptor},
            {ClassName.DateTimeDataValue, "java.lang.Integer", ClassName.NumberDataValue}, // time.timestampadd( interval, count)
            {ClassName.DateTimeDataValue, "java.lang.Integer", ClassName.DateTimeDataValue},// time2.timestampDiff( interval, time1)
            {ClassName.ConcatableDataValue, ClassName.StringDataValue, ClassName.StringDataValue}, // replace{}
            {ClassName.StringDataValue, ClassName.NumberDataValue, ClassName.NumberDataValue}, // right
            {ClassName.StringDataValue, ClassName.NumberDataValue, ClassName.NumberDataValue} // left
    };

    /**
     * Initializer for a TernaryOperatorNode
     *
     * @param receiver        The receiver (eg, string being operated on in substr())
     * @param leftOperand    The left operand of the node
     * @param rightOperand    The right operand of the node
     * @param operatorType    The type of the operand
     */

    public void init(
                    Object receiver,
                    Object leftOperand,
                    Object rightOperand,
                    Object operatorType,
                    Object trimType)
    {
        this.receiver = (ValueNode) receiver;
        this.leftOperand = (ValueNode) leftOperand;
        this.rightOperand = (ValueNode) rightOperand;
        this.operatorType = (Integer) operatorType;
        this.operator = TernaryOperators[this.operatorType];
        this.methodName = TernaryMethodNames[this.operatorType];
        this.resultInterfaceType = TernaryResultType[this.operatorType];
        this.receiverInterfaceType = TernaryArgType[this.operatorType][0];
        this.leftInterfaceType = TernaryArgType[this.operatorType][1];
        this.rightInterfaceType = TernaryArgType[this.operatorType][2];
        if (trimType != null)
                this.trimType = (Integer) trimType;
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
            return "operator: " + operator + "\n" +
                "methodName: " + methodName + "\n" +
                "resultInterfaceType: " + resultInterfaceType + "\n" +
                "receiverInterfaceType: " + receiverInterfaceType + "\n" +
                "leftInterfaceType: " + leftInterfaceType + "\n" +
                "rightInterfaceType: " + rightInterfaceType + "\n" +
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

            if (receiver != null)
            {
                printLabel(depth, "receiver: ");
                receiver.treePrint(depth + 1);
            }

            if (leftOperand != null)
            {
                printLabel(depth, "leftOperand: ");
                leftOperand.treePrint(depth + 1);
            }

            if (rightOperand != null)
            {
                printLabel(depth, "rightOperand: ");
                rightOperand.treePrint(depth + 1);
            }
        }
    }

    /**
     * Bind this expression.  This means binding the sub-expressions,
     * as well as figuring out what the return type is for this expression.
     *
     * @param fromList        The FROM list for the query this
     *                expression is in, for binding columns.
     * @param subqueryList        The subquery list being built as we find SubqueryNodes
     * @param aggregateVector    The aggregate vector being built as we find AggregateNodes
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */

    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector)  throws StandardException {
        receiver = receiver.bindExpression(fromList, subqueryList,  aggregateVector);

        leftOperand = leftOperand.bindExpression(fromList, subqueryList, aggregateVector);

        if (rightOperand != null)
        {
            rightOperand = rightOperand.bindExpression(fromList, subqueryList,  aggregateVector);
        }
        if (operatorType == TRIM)
            trimBind();
        else if (operatorType == LOCATE)
            locateBind();
        else if (operatorType == SUBSTRING)
            substrBind();
        else if (operatorType == RIGHT)
            rightBind();
        else if (operatorType == LEFT)
            leftBind();
        else if (operatorType == TIMESTAMPADD)
            timestampAddBind();
        else if (operatorType == TIMESTAMPDIFF)
            timestampDiffBind();
        else if (operatorType == REPLACE)
            replaceBind();

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
        receiver = receiver.preprocess(numTables,
                                             outerFromList, outerSubqueryList,
                                             outerPredicateList);

        leftOperand = leftOperand.preprocess(numTables,
                                             outerFromList, outerSubqueryList,
                                             outerPredicateList);
        if (rightOperand != null)
        {
            rightOperand = rightOperand.preprocess(numTables,
                                                   outerFromList, outerSubqueryList,
                                                   outerPredicateList);
        }
        return this;
    }

    /**
     * Return the variant type for the underlying expression.
     * The variant type can be:
     *        VARIANT                - variant within a scan
     *                              (method calls and non-static field access)
     *        SCAN_INVARIANT        - invariant within a scan
     *                              (column references from outer tables)
     *        QUERY_INVARIANT        - invariant within the life of a query
     *        CONSTANT            - immutable
     *
     * @return    The variant type for the underlying expression.
     * @exception StandardException    thrown on error
     */
    protected int getOrderableVariantType() throws StandardException {
        if (operator != null && operator.equals("trim") && rightOperand == null)
            return Qualifier.VARIANT;
        int leftType = leftOperand.getOrderableVariantType();
        return rightOperand == null ?
                leftType : Math.min(leftType, rightOperand.getOrderableVariantType());
    }

    /**
     * Do code generation for this ternary operator.
     *
     * @param acb    The ExpressionClassBuilder for the class we're generating
     * @param mb    The method the expression will go into
     *
     *
     * @exception StandardException        Thrown on error
     */

    public void generateExpression(ExpressionClassBuilder acb,
                                            MethodBuilder mb)
        throws StandardException
    {
        int nargs = 0;
        String receiverType = null;

        /* Allocate an object for re-use to hold the result of the operator */
        LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, resultInterfaceType);

        receiver.generateExpression(acb, mb);
        if (operatorType == TRIM)
        {
            mb.push(trimType);
            leftOperand.generateExpression(acb, mb);
            mb.cast(leftInterfaceType);

            mb.getField(field);
            nargs = 3;
            receiverType = receiverInterfaceType;
        }
        else if (operatorType == LOCATE)
        {
            leftOperand.generateExpression(acb, mb);
            mb.upCast(leftInterfaceType);
            rightOperand.generateExpression(acb, mb);
            mb.upCast(rightInterfaceType);
            mb.getField(field);
            nargs = 3;
            receiverType = receiverInterfaceType;
        }
        else if (operatorType == SUBSTRING)
        {
            leftOperand.generateExpression(acb, mb);
            mb.upCast(leftInterfaceType);
            if (rightOperand != null)
            {
                rightOperand.generateExpression(acb, mb);
                mb.upCast(rightInterfaceType);
            }
            else
            {
                mb.pushNull(rightInterfaceType);
            }

            mb.getField(field); // third arg
            mb.push(receiver.getTypeServices().getMaximumWidth());
            mb.push(getTypeServices().getTypeId().getTypeFormatId() == StoredFormatIds.CHAR_TYPE_ID || getTypeServices().getTypeId().getTypeFormatId() == StoredFormatIds.BIT_TYPE_ID);
            nargs = 5;
            receiverType = receiverInterfaceType;
        }
        else if (operatorType == LEFT)
        {
            leftOperand.generateExpression(acb, mb);
            mb.upCast(leftInterfaceType);
            mb.getField(field);
            nargs = 2;
            receiverType = receiverInterfaceType;
        }
        else if (operatorType == RIGHT)
        {
            leftOperand.generateExpression(acb, mb);
            mb.upCast(leftInterfaceType);
            mb.getField(field);
            nargs = 2;
            receiverType = receiverInterfaceType;
        }
        else if (operatorType == TIMESTAMPADD || operatorType == TIMESTAMPDIFF)
        {
            Object intervalType = leftOperand.getConstantValueAsObject();
            if( SanityManager.DEBUG)
                SanityManager.ASSERT( intervalType != null && intervalType instanceof Integer,
                                      "Invalid interval type used for " + operator);
            mb.push((Integer) intervalType);
            rightOperand.generateExpression( acb, mb);
            mb.upCast( TernaryArgType[ operatorType][2]);
            acb.getCurrentDateExpression( mb);
            mb.getField(field);
            nargs = 4;
            receiverType = receiverInterfaceType;
        }
        else if (operatorType == REPLACE)
        {
            leftOperand.generateExpression(acb, mb);
            mb.upCast(leftInterfaceType);
            if (rightOperand != null)
            {
                rightOperand.generateExpression(acb, mb);
                mb.upCast(rightInterfaceType);
            }
            else
            {
                mb.pushNull(rightInterfaceType);
            }
            mb.getField(field);
            nargs = 3;
            receiverType = receiverInterfaceType;
        }

        mb.callMethod(VMOpcode.INVOKEINTERFACE, receiverType, methodName, resultInterfaceType, nargs);

        /*
        ** Store the result of the method call in the field, so we can re-use
        ** the object.
        */
//        mb.putField(field);
    }

    public String getOperator(){
        return operator;
    }

    /**
     * Set the leftOperand to the specified ValueNode
     *
     * @param newLeftOperand    The new leftOperand
     */
    public void setLeftOperand(ValueNode newLeftOperand)
    {
        leftOperand = newLeftOperand;
    }

    /**
     * Get the leftOperand
     *
     * @return The current leftOperand.
     */
    public ValueNode getLeftOperand()
    {
        return leftOperand;
    }

    public void setReceiver(ValueNode receiver) {
        this.receiver = receiver;
    }

    @Override
    public boolean checkCRLevel(int level){
        return leftOperand.checkCRLevel(level) || (rightOperand != null ? rightOperand.checkCRLevel(level) : false) || receiver.checkCRLevel(level);
    }


    /**
     * Set the rightOperand to the specified ValueNode
     *
     * @param newRightOperand    The new rightOperand
     */
    public void setRightOperand(ValueNode newRightOperand)
    {
        rightOperand = newRightOperand;
    }

    /**
     * Get the rightOperand
     *
     * @return The current rightOperand.
     */
    public ValueNode getRightOperand()
    {
        return rightOperand;
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
     * @exception StandardException            Thrown on error
     */
    public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
        throws StandardException
    {
        boolean pushable;
        pushable = receiver.categorize(referencedTabs, simplePredsOnly);
        pushable = (leftOperand.categorize(referencedTabs, simplePredsOnly) && pushable);
        if (rightOperand != null)
        {
            pushable = (rightOperand.categorize(referencedTabs, simplePredsOnly) && pushable);
        }
        return pushable;
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
        receiver = receiver.remapColumnReferencesToExpressions();
        leftOperand = leftOperand.remapColumnReferencesToExpressions();
        if (rightOperand != null)
        {
            rightOperand = rightOperand.remapColumnReferencesToExpressions();
        }
        return this;
    }

    /**
     * Return whether or not this expression tree represents a constant expression.
     *
     * @return    Whether or not this expression tree represents a constant expression.
     */
    public boolean isConstantExpression()
    {
        return (receiver.isConstantExpression() &&
                leftOperand.isConstantExpression() &&
                (rightOperand == null || rightOperand.isConstantExpression()));
    }

    /** @see ValueNode#constantExpression */
    public boolean constantExpression(PredicateList whereClause)
    {
        return (receiver.constantExpression(whereClause) &&
                leftOperand.constantExpression(whereClause) &&
                (rightOperand == null ||
                    rightOperand.constantExpression(whereClause)));
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (receiver != null) {
            receiver = (ValueNode)receiver.accept(v, this);
        }
        if (leftOperand != null) {
            leftOperand = (ValueNode)leftOperand.accept(v, this);
        }
        if (rightOperand != null) {
            rightOperand = (ValueNode)rightOperand.accept(v, this);
        }
    }
    /**
     * Bind trim expression.
     * The variable receiver is the string that needs to be trimmed.
     * The variable leftOperand is the character that needs to be trimmed from
     *     receiver.
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */

    private ValueNode trimBind()
            throws StandardException
    {
        TypeId    receiverType;
        TypeId    resultType = TypeId.getBuiltInTypeId(Types.VARCHAR);

        // handle parameters here

        /* Is there a ? parameter for the receiver? */
        if (receiver.requiresTypeFromContext())
        {
            /*
            ** According to the SQL standard, if trim has a ? receiver,
            ** its type is varchar with the implementation-defined maximum length
            ** for a varchar.
            */

            receiver.setType(getVarcharDescriptor());
            //check if this parameter can pick up it's collation from the
            //character that will be used for trimming. If not(meaning the
            //character to be trimmed is also a parameter), then it will take
            //it's collation from the compilation schema.
            if (!leftOperand.requiresTypeFromContext()) {
                receiver.setCollationInfo(leftOperand.getTypeServices());
            } else {
                receiver.setCollationUsingCompilationSchema();
            }
        }

        /* Is there a ? parameter on the left? */
        if (leftOperand.requiresTypeFromContext())
        {
            /* Set the left operand type to varchar. */
            leftOperand.setType(getVarcharDescriptor());
            //collation of ? operand should be picked up from the context.
            //By the time we come here, receiver will have correct collation
            //set on it and hence we can rely on it to get correct collation
            //for the ? for the character that needs to be used for trimming.
            leftOperand.setCollationInfo(receiver.getTypeServices());
        }

        bindToBuiltIn();

        /*
        ** Check the type of the receiver - this function is allowed only on
        ** string value types.
        */
        receiverType = receiver.getTypeId();
        if (receiverType.userType())
            throwBadType("trim", receiverType.getSQLTypeName());

        receiver = castArgToString(receiver);

        if (receiverType.getTypeFormatId() == StoredFormatIds.CLOB_TYPE_ID) {
        // special case for CLOBs: if we start with a CLOB, we have to get
        // a CLOB as a result (as opposed to a VARCHAR), because we can have a
        // CLOB that is beyond the max length of VARCHAR (ex. "clob(100k)").
        // This is okay because CLOBs, like VARCHARs, allow variable-length
        // values (which is a must for the trim to actually work).
            resultType = receiverType;
        }

        /*
        ** Check the type of the leftOperand (trimSet).
        ** The leftOperand should be a string value type.
        */
        TypeId    leftCTI;
        leftCTI = leftOperand.getTypeId();
        if (leftCTI.userType())
            throwBadType("trim", leftCTI.getSQLTypeName());

        leftOperand = castArgToString(leftOperand);

        /*
        ** The result type of trim is varchar.
        */
        setResultType(resultType);
        //Result of TRIM should pick up the collation of the character string
        //that is getting trimmed (which is variable receiver) because it has
        //correct collation set on it.
        setCollationInfo(receiver.getTypeServices());

        return this;
    }
    /*
    ** set result type for operator
    */
    private void setResultType(TypeId resultType) throws StandardException
    {
        setType(new DataTypeDescriptor(
                        resultType,
                        true,
                        receiver.getTypeServices().getMaximumWidth()
                    )
                );
    }
    /**
     * Bind locate operator
     * The variable leftOperand is the string which will be searched
     * The variable receiver is the search character that will be looked in the
     *     leftOperand variable.
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */

    public ValueNode locateBind() throws StandardException
    {
        TypeId    firstOperandType, secondOperandType, offsetType;

        /*
         * Is there a ? parameter for the first arg.  Copy the
         * left/firstOperand's.  If the left/firstOperand are both parameters,
         * both will be max length.
         */
        if( receiver.requiresTypeFromContext())
        {
            if( leftOperand.requiresTypeFromContext())
            {
                // we cannot tell whether it is StringType or BitType, so set to BitType as default
                receiver.setType(getVarBitDescriptor());
                //Since both receiver and leftOperands are parameters, use the
                //collation of compilation schema for receiver.
                receiver.setCollationUsingCompilationSchema();
            }
            else
            {
                if( leftOperand.getTypeId().isStringTypeId() || leftOperand.getTypeId().isBitTypeId())
                {
                    //Since the leftOperand is not a parameter, receiver will
                    //get it's collation from leftOperand through following
                    //setType method
                    receiver.setType(
                                     leftOperand.getTypeServices());
                }
            }
        }

        /*
         * Is there a ? parameter for the second arg.  Copy the receiver's.
         * If the receiver are both parameters, both will be max length.
         */
        if(leftOperand.requiresTypeFromContext())
        {
            if(receiver.requiresTypeFromContext())
            {
                // we cannot tell whether it is StringType or BitType, so set to BitType as default
                leftOperand.setType(getVarBitDescriptor());
            }
            else
            {
                if( receiver.getTypeId().isStringTypeId() || receiver.getTypeId().isBitTypeId())
                {
                    leftOperand.setType(
                                     receiver.getTypeServices());
                }
            }
            //collation of ? operand should be picked up from the context.
            //By the time we come here, receiver will have correct collation
            //set on it and hence we can rely on it to get correct collation
            //for this ?
            leftOperand.setCollationInfo(receiver.getTypeServices());
        }

        /*
         * Is there a ? paramter for the third arg.  It will be an int.
         */
        if( rightOperand.requiresTypeFromContext())
        {
            rightOperand.setType(
                new DataTypeDescriptor(TypeId.INTEGER_ID, true));
        }

        bindToBuiltIn();

        /*
        ** Check the type of the operand - this function is allowed only
        ** for: receiver = CHAR or CharBit
        **      firstOperand = CHAR or CharBit
        **      secondOperand = INT
        */
        secondOperandType = leftOperand.getTypeId();
        offsetType = rightOperand.getTypeId();
        firstOperandType = receiver.getTypeId();

        if (offsetType.getJDBCTypeId() != Types.INTEGER) {
            throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE,
                    "LOCATE", "FUNCTION");
        } else {
            if (!firstOperandType.isStringTypeId() && !firstOperandType.isBitTypeId() ||
                !secondOperandType.isStringTypeId() && !secondOperandType.isBitTypeId()) {
                throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE,
                        "LOCATE", "FUNCTION");
            }
        }
        // do implicit casting
        if (firstOperandType.isStringTypeId() && secondOperandType.isBitTypeId()) {
            // we do not support cast of Long varchar and CLOB
            if (firstOperandType.isClobTypeId() || firstOperandType.isLongVarcharTypeId()) {
                throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE,
                        "LOCATE", "FUNCTION");
            }
            receiver = castArgToVarBit(receiver);
        } else if (firstOperandType.isBitTypeId() && secondOperandType.isStringTypeId()) {
            // we do not support cast of Long varchar and CLOB
            if (secondOperandType.isClobTypeId() || secondOperandType.isLongVarcharTypeId()) {
                throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE,
                        "LOCATE", "FUNCTION");
            }
            leftOperand = castArgToVarBit(leftOperand);
        }

        /*
        ** The result type of a LocateFunctionNode is an integer.
        */
        setType(new DataTypeDescriptor(TypeId.INTEGER_ID,
                true));

        return this;
    }

    /* cast arg to a varchar */
    protected ValueNode castArgToString(ValueNode vn) throws StandardException
    {
        TypeCompiler vnTC = vn.getTypeCompiler();
        if (! vn.getTypeId().isStringTypeId())
        {
            DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true,
                    vnTC.getCastToCharWidth(
                            vn.getTypeServices()));

            ValueNode newNode = (ValueNode)
                        getNodeFactory().getNode(
                            C_NodeTypes.CAST_NODE,
                            vn,
                            dtd,
                            getContextManager());

            // DERBY-2910 - Match current schema collation for implicit cast as we do for
            // explicit casts per SQL Spec 6.12 (10)
            newNode.setCollationUsingCompilationSchema();

            ((CastNode) newNode).bindCastNodeOnly();
            return newNode;
        }
        return vn;
    }

    // cast arg to Varbit
    protected ValueNode castArgToVarBit(ValueNode vn) throws StandardException
    {
        if (! vn.getTypeId().isBitTypeId()) {

            DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARBINARY, true,
                    vn.getTypeServices().getMaximumWidth());

            ValueNode newNode = (ValueNode)
                    getNodeFactory().getNode(
                            C_NodeTypes.CAST_NODE,
                            vn,
                            dtd,
                            getContextManager());

            newNode.setCollationUsingCompilationSchema();

            ((CastNode) newNode).bindCastNodeOnly();
            vn = newNode;
        }
        return vn;
    }
    /**
     * Bind substr expression.
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */

     public ValueNode substrBind()
            throws StandardException
    {
        TypeId    receiverType;

        // handle parameters here

        /* Is there a ? parameter for the receiver? */
        if (receiver.requiresTypeFromContext())
        {
            /*
            ** According to the SQL standard, if substr has a ? receiver,
            ** its type is varchar with the implementation-defined maximum length
            ** for a varchar.
            */
            receiver.setType(getVarcharDescriptor());
            //collation of ? operand should be same as the compilation schema
            //because that is the only context available for us to pick up the
            //collation. There are no other character operands to SUBSTR method
            //to pick up the collation from.
            receiver.setCollationUsingCompilationSchema();
        }

        /* Is there a ? parameter on the left? */
        if (leftOperand.requiresTypeFromContext())
        {
            /* Set the left operand type to int. */
            leftOperand.setType(
                new DataTypeDescriptor(TypeId.INTEGER_ID, true));
        }

        /* Is there a ? parameter on the right? */
        if ((rightOperand != null) && rightOperand.requiresTypeFromContext())
        {
            /* Set the right operand type to int. */
            rightOperand.setType(
                new DataTypeDescriptor(TypeId.INTEGER_ID, true));
        }

        bindToBuiltIn();

        if (!leftOperand.getTypeId().isNumericTypeId() ||
            (rightOperand != null && !rightOperand.getTypeId().isNumericTypeId()))
            throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, "SUBSTR", "FUNCTION");

        /*
        ** Check the type of the receiver - this function is allowed only on
        ** string value types.
        */
        receiverType = receiver.getTypeId();
        if (!receiverType.isStringTypeId() && !receiverType.isBitTypeId()) {
            throwBadType("SUBSTR", receiverType.getSQLTypeName());
        }


        // Determine the maximum length of the result
        int maximumWidth = receiver.getTypeServices().getMaximumWidth();
        boolean isFixedLength = false;
        int resultLen = maximumWidth;

        TypeId    resultType;

        // receiver is fixed type and either start or length is constant, then result should be fixed type with known length;
        // or receiver is vartype and length is constant, then result should be fixed type with known length
        if ((receiverType.getTypeFormatId() == StoredFormatIds.CHAR_TYPE_ID ||
            receiverType.getTypeFormatId() == StoredFormatIds.BIT_TYPE_ID)) {
            if (rightOperand != null && rightOperand instanceof ConstantNode)
            {
                resultLen = ((ConstantNode)rightOperand).getValue().getInt();
                isFixedLength = true;
            } else if (leftOperand instanceof ConstantNode) {
                int startPostion = ((ConstantNode)leftOperand).getValue().getInt();
                resultLen = maximumWidth - startPostion + 1;
                isFixedLength = true;
            }
        } else if ((receiverType.getTypeFormatId() == StoredFormatIds.VARCHAR_TYPE_ID ||
                receiverType.getTypeFormatId() == StoredFormatIds.VARBIT_TYPE_ID)) {
            if (rightOperand != null && rightOperand instanceof ConstantNode)
            {
                resultLen = ((ConstantNode)rightOperand).getValue().getInt();
                isFixedLength = true;
            }
        } else {
            if (rightOperand != null && rightOperand instanceof ConstantNode)
            {
                resultLen =((ConstantNode)rightOperand).getValue().getInt();
                if (resultLen > maximumWidth)
                    resultLen = maximumWidth;
            }
        }

        if (receiverType.getTypeFormatId() == StoredFormatIds.CLOB_TYPE_ID) {
        // special case for CLOBs: if we start with a CLOB, we have to get
        // a CLOB as a result (as opposed to a VARCHAR), because we can have a
        // CLOB that is beyond the max length of VARCHAR (ex. "clob(100k)").
        // This is okay because CLOBs, like VARCHARs, allow variable-length
        // values (which is a must for the substr to actually work).
            resultType = receiverType;
        } else if (receiverType.isLongVarbinaryTypeId() || receiverType.isBlobTypeId()) {
            resultType = receiverType;
        } else if (receiverType.isBitTypeId()) {
            resultType = TypeId.getBuiltInTypeId(isFixedLength? Types.BINARY : Types.VARBINARY);
        } else { // receiverType.isStringTypeId()
            resultType = TypeId.getBuiltInTypeId(isFixedLength? Types.CHAR : Types.VARCHAR);
        }

        /*
        ** The result type of substr is a string type or byte type
        */
        setType(new DataTypeDescriptor(
                        resultType,
                        true,
                        resultLen
                    ));
        //Result of SUSBSTR should pick up the collation of the 1st argument
        //to SUBSTR. The 1st argument to SUBSTR is represented by the variable
        //receiver in this class.
        setCollationInfo(receiver.getTypeServices());
        return this;
    }

    public ValueNode rightBind() throws StandardException
    {
        TypeId    receiverType;
        TypeId    resultType = TypeId.getBuiltInTypeId(Types.VARCHAR);

        if (receiver.requiresTypeFromContext())
        {
            receiver.setType(getVarcharDescriptor());
            receiver.setCollationUsingCompilationSchema();
        }
        if (leftOperand.requiresTypeFromContext())
        {
            leftOperand.setType(new DataTypeDescriptor(TypeId.INTEGER_ID, false));
        }

        bindToBuiltIn();

        if (!leftOperand.getTypeId().isIntegerNumericTypeId())
            throwBadType("RIGHT", leftOperand.getTypeId().getSQLTypeName());

        receiverType = receiver.getTypeId();
        switch (receiverType.getJDBCTypeId())
        {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                break;
            default:
            {
                throwBadType("RIGHT", receiverType.getSQLTypeName());
            }
        }

        if (receiverType.getTypeFormatId() == StoredFormatIds.CLOB_TYPE_ID)
        {
            resultType = receiverType;
        }

        int resultLen = receiver.getTypeServices().getMaximumWidth();

        if (leftOperand instanceof ConstantNode)
        {
            if (((ConstantNode)leftOperand).getValue().getInt() > resultLen)
                resultLen = ((ConstantNode)leftOperand).getValue().getInt();
        }

        setType(new DataTypeDescriptor(
                resultType,
                true,
                resultLen
        ));
        setCollationInfo(receiver.getTypeServices());
        return this;
    }

    public ValueNode leftBind() throws StandardException
    {
        TypeId    receiverType;
        TypeId    resultType = TypeId.getBuiltInTypeId(Types.VARCHAR);

        if (receiver.requiresTypeFromContext())
        {
            receiver.setType(getVarcharDescriptor());
            receiver.setCollationUsingCompilationSchema();
        }
        if (leftOperand.requiresTypeFromContext())
        {
            leftOperand.setType(new DataTypeDescriptor(TypeId.INTEGER_ID, false));
        }

        bindToBuiltIn();

        if (!leftOperand.getTypeId().isIntegerNumericTypeId())
            throwBadType("LEFT", leftOperand.getTypeId().getSQLTypeName());

        receiverType = receiver.getTypeId();
        switch (receiverType.getJDBCTypeId())
        {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                break;
            default:
            {
                throwBadType("LEFT", receiverType.getSQLTypeName());
            }
        }

        if (receiverType.getTypeFormatId() == StoredFormatIds.CLOB_TYPE_ID) {
            resultType = receiverType;
        }

        int resultLen = receiver.getTypeServices().getMaximumWidth();

        if (leftOperand instanceof ConstantNode)
        {
            if (((ConstantNode)leftOperand).getValue().getInt() > resultLen)
                resultLen = ((ConstantNode)leftOperand).getValue().getInt();
        }

        setType(new DataTypeDescriptor(
                resultType,
                true,
                resultLen
        ));
        setCollationInfo(receiver.getTypeServices());
        return this;
    }

    /**
     * Binds the replace expression.
     *
     * @return the new top of the expression tree.
     *
     * @exception StandardException thrown on error
     */

     public ValueNode replaceBind()
            throws StandardException
    {
        TypeId    receiverType;
        TypeId    resultType = TypeId.getBuiltInTypeId(Types.VARCHAR);

        // See substrBind() method also, for additional comments.

        /* Is there a ? parameter for the receiver? */
        if (receiver.requiresTypeFromContext())
        {
            // According to the SQL standard, if replace has a ? receiver,
            // its type is varchar with the implementation-defined maximum length
            // for a varchar.
            receiver.setType(getVarcharDescriptor());

            // collation of ? operand should be same as the compilation schema
            // because that is the only context available for us to pick up the
            // collation. There are no other character operands to SUBSTR method
            // to pick up the collation from.
            receiver.setCollationUsingCompilationSchema();
        }

        /* Is there a ? parameter on the left? */
        if (leftOperand.requiresTypeFromContext())
        {
            /* Set the left operand type to VARCHAR. */
            leftOperand.setType(
                new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true));
        }

        /* Is there a ? parameter on the right? */
        if ((rightOperand != null) && rightOperand.requiresTypeFromContext())
        {
            /* Set the right operand type to VARCHAR. */
            rightOperand.setType(
                new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true));
        }

        bindToBuiltIn();

        if (!leftOperand.getTypeId().isStringTypeId() ||
            (rightOperand != null && !rightOperand.getTypeId().isStringTypeId()))
            throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, "REPLACE", "FUNCTION");

        // Check the type of the receiver - this function is allowed only on
        // string value types.
        receiverType = receiver.getTypeId();
        switch (receiverType.getJDBCTypeId())
        {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                break;
            default:
            {
                throwBadType("REPLACE", receiverType.getSQLTypeName());
            }
        }
        if (receiverType.getTypeFormatId() == StoredFormatIds.CLOB_TYPE_ID) {
            // See comments in substrBind method, where this is also done.
            resultType = receiverType;
        }

        // Determine the maximum length of the result string
        int maxResultLen = receiver.getTypeServices().getMaximumWidth();
        setType(new DataTypeDescriptor(resultType, true, maxResultLen));

        // Result of REPLACE should pick up the collation of the 1st argument
        // to REPLACE. The 1st argument to REPLACE is represented by the variable
        // receiver in this class.
        setCollationInfo(receiver.getTypeServices());

        return this;
    }

    /**
     * Bind TIMESTAMPADD expression.
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */

     private ValueNode timestampAddBind()
            throws StandardException
    {
        if( ! bindParameter( rightOperand, Types.INTEGER))
        {
            int jdbcType = rightOperand.getTypeId().getJDBCTypeId();
            if( jdbcType != Types.TINYINT && jdbcType != Types.SMALLINT &&
                jdbcType != Types.INTEGER && jdbcType != Types.BIGINT)
                throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARG_TYPE,
                                                     rightOperand.getTypeId().getSQLTypeName(),
                                                     ReuseFactory.getInteger( 2),
                                                     operator);
        }
        bindDateTimeArg( receiver, 3);
        setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor( Types.TIMESTAMP));
        return this;
    } // end of timestampAddBind

    /**
     * Bind TIMESTAMPDIFF expression.
     *
     * @return    The new top of the expression tree.
     *
     * @exception StandardException        Thrown on error
     */

     private ValueNode timestampDiffBind()
            throws StandardException
    {
        bindDateTimeArg( rightOperand, 2);
        bindDateTimeArg( receiver, 3);
        setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor( Types.BIGINT));
        return this;
    } // End of timestampDiffBind

    private void bindDateTimeArg( ValueNode arg, int argNumber) throws StandardException
    {
        if( ! bindParameter( arg, Types.TIMESTAMP))
        {
            if( ! arg.getTypeId().isDateTimeTimeStampTypeId())
                throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARG_TYPE,
                                                     arg.getTypeId().getSQLTypeName(),
                                                     ReuseFactory.getInteger( argNumber),
                                                     operator);
        }
    } // end of bindDateTimeArg

    /**
     * This method gets called for non-character string types and hence no need
     * to set any collation info. Collation applies only to character string
     * types.
     *
     * @param arg Check if arg is a ? param and if yes, then set it's type to
     *    jdbcType if arg doesn't have a type associated with it.
     *
     * @param jdbcType Associate this type with arg if arg is a ? param with no
     *    type associated with it
     *
     * @return true if arg is a ? param with no type associated with it
     * @throws StandardException
     */
    private boolean bindParameter( ValueNode arg, int jdbcType) throws StandardException
    {
        if( arg.requiresTypeFromContext() && arg.getTypeId() == null)
        {
            arg.setType( new DataTypeDescriptor(TypeId.getBuiltInTypeId( jdbcType), true));
            return true;
        }
        return false;
    } // end of bindParameter

    public ValueNode getReceiver()
    {
        return receiver;
    }

    /* throw bad type message */
    private void throwBadType(String funcName, String type)
        throws StandardException
    {
        throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                                        funcName,
                                        type);
    }

    /* bind arguments to built in types */
    protected void bindToBuiltIn()
        throws StandardException
    {
        /* If the receiver is not a built-in type, then generate a bound conversion
         * tree to a built-in type.
         */
        if (receiver.getTypeId().userType())
        {
            receiver = receiver.genSQLJavaSQLTree();
        }

        /* If the left operand is not a built-in type, then generate a bound conversion
         * tree to a built-in type.
         */
        if (leftOperand.getTypeId().userType())
        {
            leftOperand = leftOperand.genSQLJavaSQLTree();
        }

        /* If the right operand is not a built-in type, then generate a bound conversion
         * tree to a built-in type.
         */
        if (rightOperand != null)
        {
            if (rightOperand.getTypeId().userType())
            {
                rightOperand = rightOperand.genSQLJavaSQLTree();
            }
        }
    }

    private DataTypeDescriptor getVarcharDescriptor() {
        return new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true);
    }

    private DataTypeDescriptor getVarBitDescriptor() {
        return new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARBINARY), true);
    }

    protected boolean isEquivalent(ValueNode o) throws StandardException
    {
        if (isSameNodeType(o))
    {
        TernaryOperatorNode other = (TernaryOperatorNode)o;

            /*
             * SUBSTR function can either have 2 or 3 arguments.  In the
             * 2-args case, rightOperand will be null and thus needs
             * additional handling in the equivalence check.
             */
            return (other.methodName.equals(methodName)
                && other.receiver.isEquivalent(receiver)
                    && other.leftOperand.isEquivalent(leftOperand)
                    && ( (rightOperand == null && other.rightOperand == null) ||
                         (other.rightOperand != null &&
                            other.rightOperand.isEquivalent(rightOperand)) ) );
        }
        return false;
    }

    public List<? extends QueryTreeNode> getChildren() {
        return new ArrayList<QueryTreeNode>(){{
            add(receiver);
            add(leftOperand);
            if (rightOperand != null ) add(rightOperand);
        }};
    }

    @Override
    public QueryTreeNode getChild(int index) {
        return getChildren().get(index);
    }

    @Override
    public void setChild(int index, QueryTreeNode newValue) {
        switch (index) {
            case 0:
                receiver = (ValueNode) newValue;
                break;
            case 1:
                leftOperand = (ValueNode) newValue;
                break;
            case 2:
                assert rightOperand != null;
                rightOperand = (ValueNode) newValue;
                break;
            default:
                assert false;
        }
    }

    @Override
    public long nonZeroCardinality(long numberOfRows) throws StandardException {
        long c1 = leftOperand != null ? leftOperand.nonZeroCardinality(numberOfRows) : 0;
        long c2 = rightOperand != null ? rightOperand.nonZeroCardinality(numberOfRows) : 0;
        long c3 = receiver!= null ? receiver.nonZeroCardinality(numberOfRows) : 0;

        return Math.max(Math.max(c1, c2), c3);
    }

    @Override
    public boolean isConstantOrParameterTreeNode() {
        if (leftOperand != null && !leftOperand.isConstantOrParameterTreeNode())
            return false;
        if (rightOperand != null && !rightOperand.isConstantOrParameterTreeNode())
            return false;

        if (receiver != null && !receiver.isConstantOrParameterTreeNode())
            return false;

        return true;
    }

    // Following 3 interfaces only applicable when this is a TRIM operator.
    public boolean isLeading()  {return trimType == StringDataValue.LEADING;}
    public boolean isTrailing() {return trimType == StringDataValue.TRAILING;}
    public boolean isBoth()     {return trimType == StringDataValue.BOTH;}
}
