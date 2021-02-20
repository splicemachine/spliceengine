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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.util.ReuseFactory;

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.*;

/**
 * A TernaryOperatorNode represents a built-in ternary operators.
 * This covers  built-in functions like substr().
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 */

public class TernaryOperatorNode extends OperatorNode
{
    int            operatorType;

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
    public static final int SPLIT_PART = 9;
    public static final int DB2RTRIM = 10;
    static final String[] TernaryOperators = {"trim", "LOCATE", "substring", "like", "TIMESTAMPADD", "TIMESTAMPDIFF", "replace", "right", "left", "split_part", "trim"};
    static final String[] TernaryMethodNames = {"ansiTrim", "locate", "substring", "like", "timestampAdd", "timestampDiff", "replace", "right", "left", "split_part", "db2Trim"};

    static final String[] TernaryResultType = {
            ClassName.StringDataValue,
            ClassName.NumberDataValue,
            ClassName.ConcatableDataValue,
            ClassName.BooleanDataValue,
            ClassName.DateTimeDataValue,
            ClassName.NumberDataValue,
            ClassName.ConcatableDataValue,
            ClassName.StringDataValue,
            ClassName.StringDataValue,
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
            {ClassName.StringDataValue, ClassName.NumberDataValue, ClassName.NumberDataValue}, // left
            {ClassName.StringDataValue, ClassName.StringDataValue, ClassName.NumberDataValue}, // split_part
            {ClassName.StringDataValue, ClassName.StringDataValue, "java.lang.Integer"} // DB2 rtrim
    };

    public TernaryOperatorNode() {}
    public TernaryOperatorNode( int nodeType, // todo: nodeType is not really used, only operatorType
                                ValueNode receiver, ValueNode leftOperand,
                         ValueNode rightOperand, Integer operatorType, Integer trimType, ContextManager cm )
    {
        setNodeType(nodeType);
        setContextManager(cm);
        init(receiver, leftOperand, rightOperand, operatorType, trimType);
    }

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
        this.operands = new ArrayList<>(Arrays.asList((ValueNode) receiver, (ValueNode) leftOperand, (ValueNode) rightOperand));
        this.operatorType = (Integer) operatorType;
        this.operator = TernaryOperators[this.operatorType];
        this.methodName = TernaryMethodNames[this.operatorType];
        this.resultInterfaceType = TernaryResultType[this.operatorType];
        this.interfaceTypes = new ArrayList<>(Arrays.asList(TernaryArgType[this.operatorType]));
        if (trimType != null)
                this.trimType = (Integer) trimType;
    }

    String getReceiverInterfaceType() {
        return interfaceTypes.get(0);
    }

    void setReceiverInterfaceType(String iType) {
        interfaceTypes.set(0, iType);
    }

    String getLeftInterfaceType() {
        return interfaceTypes.get(1);
    }

    String getRightInterfaceType() {
        return interfaceTypes.get(2);
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
                "receiverInterfaceType: " + getReceiverInterfaceType() + "\n" +
                "leftInterfaceType: " + getLeftInterfaceType() + "\n" +
                "rightInterfaceType: " + getRightInterfaceType() + "\n" +
                super.toString();
        }
        else
        {
            return "";
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
        bindOperands(fromList, subqueryList,  aggregateVector);
        if (operatorType == TRIM || operatorType == DB2RTRIM)
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
        else if (operatorType == SPLIT_PART)
            splitPartBind();

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
    @Override
    protected int getOrderableVariantType() throws StandardException {
        if (operator != null && operator.equals("trim") && getRightOperand() == null)
            return Qualifier.VARIANT;
        int leftType = getLeftOperand().getOrderableVariantType();
        return getRightOperand() == null ?
                leftType : Math.min(leftType, getRightOperand().getOrderableVariantType());
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

        getReceiver().generateExpression(acb, mb);
        if (operatorType == TRIM || operatorType == DB2RTRIM)
        {
            mb.push(trimType);
            getLeftOperand().generateExpression(acb, mb);
            mb.cast(getLeftInterfaceType());

            mb.getField(field);
            nargs = 3;
            receiverType = getReceiverInterfaceType();
        }
        else if (operatorType == LOCATE)
        {
            getLeftOperand().generateExpression(acb, mb);
            mb.upCast(getLeftInterfaceType());
            getRightOperand().generateExpression(acb, mb);
            mb.upCast(getRightInterfaceType());
            mb.getField(field);
            nargs = 3;
            receiverType = getReceiverInterfaceType();
        }
        else if (operatorType == SUBSTRING)
        {
            getLeftOperand().generateExpression(acb, mb);
            mb.upCast(getLeftInterfaceType());
            if (getRightOperand() != null)
            {
                getRightOperand().generateExpression(acb, mb);
                mb.upCast(getRightInterfaceType());
            }
            else
            {
                mb.pushNull(getRightInterfaceType());
            }

            mb.getField(field); // third arg
            mb.push(getReceiver().getTypeServices().getMaximumWidth());
            mb.push(getTypeServices().getTypeId().getTypeFormatId() == StoredFormatIds.CHAR_TYPE_ID || getTypeServices().getTypeId().getTypeFormatId() == StoredFormatIds.BIT_TYPE_ID);
            nargs = 5;
            receiverType = getReceiverInterfaceType();
        }
        else if (operatorType == LEFT)
        {
            getLeftOperand().generateExpression(acb, mb);
            mb.upCast(getLeftInterfaceType());
            mb.getField(field);
            nargs = 2;
            receiverType = getReceiverInterfaceType();
        }
        else if (operatorType == RIGHT)
        {
            getLeftOperand().generateExpression(acb, mb);
            mb.upCast(getLeftInterfaceType());
            mb.getField(field);
            nargs = 2;
            receiverType = getReceiverInterfaceType();
        }
        else if (operatorType == TIMESTAMPADD || operatorType == TIMESTAMPDIFF)
        {
            Object intervalType = getLeftOperand().getConstantValueAsObject();
            if( SanityManager.DEBUG)
                SanityManager.ASSERT( intervalType != null && intervalType instanceof Integer,
                                      "Invalid interval type used for " + operator);
            mb.push((Integer) intervalType);
            getRightOperand().generateExpression( acb, mb);
            mb.upCast( TernaryArgType[ operatorType][2]);
            acb.getCurrentDateExpression( mb);
            mb.getField(field);
            nargs = 4;
            receiverType = getReceiverInterfaceType();
        }
        else if (operatorType == REPLACE)
        {
            getLeftOperand().generateExpression(acb, mb);
            mb.upCast(getLeftInterfaceType());
            if (getRightOperand() != null)
            {
                getRightOperand().generateExpression(acb, mb);
                mb.upCast(getRightInterfaceType());
            }
            else
            {
                mb.pushNull(getRightInterfaceType());
            }
            mb.getField(field);
            nargs = 3;
            receiverType = getReceiverInterfaceType();
        }
        else if (operatorType == SPLIT_PART)
        {
            getLeftOperand().generateExpression(acb, mb);
            mb.upCast(getLeftInterfaceType());
            if (getRightOperand() != null)
            {
                getRightOperand().generateExpression(acb, mb);
                mb.upCast(getRightInterfaceType());
            }
            else
            {
                mb.pushNull(getRightInterfaceType());
            }
            mb.getField(field);
            nargs = 3;
            receiverType = getReceiverInterfaceType();
        }

        mb.callMethod(VMOpcode.INVOKEINTERFACE, receiverType, methodName, resultInterfaceType, nargs);

        /*
        ** Store the result of the method call in the field, so we can re-use
        ** the object.
        */
//        mb.putField(field);
    }

    /**
     * Set the leftOperand to the specified ValueNode
     *
     * @param newLeftOperand    The new leftOperand
     */
    public void setLeftOperand(ValueNode newLeftOperand)
    {
        operands.set(1, newLeftOperand);
    }

    /**
     * Get the leftOperand
     *
     * @return The current leftOperand.
     */
    public ValueNode getLeftOperand()
    {
        return operands.get(1);
    }

    public void setReceiver(ValueNode receiver) {
        this.operands.set(0, receiver);
    }

    /**
     * Set the rightOperand to the specified ValueNode
     *
     * @param newRightOperand    The new rightOperand
     */
    public void setRightOperand(ValueNode newRightOperand)
    {
        operands.set(2, newRightOperand);
    }

    /**
     * Get the rightOperand
     *
     * @return The current rightOperand.
     */
    public ValueNode getRightOperand()
    {
        return operands.get(2);
    }

    /**
     * Categorize this predicate.  Initially, this means
     * building a bit map of the referenced tables for each predicate,
     * and a mapping from table number to the column numbers
     * from that table present in the predicate.
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
     * @param referencedColumns  An object which maps tableNumber to the columns
     *                           from that table which are present in the predicate.
     * @param simplePredsOnly    Whether or not to consider method
     *                            calls, field references and conditional nodes
     *                            when building bit map
     *
     * @return boolean        Whether or not source.expression is a ColumnReference
     *                        or a VirtualColumnNode.
     * @exception StandardException            Thrown on error
     */
    public boolean categorize(JBitSet referencedTabs, ReferencedColumnsMap referencedColumns, boolean simplePredsOnly)
        throws StandardException
    {
        boolean pushable;
        pushable = getReceiver().categorize(referencedTabs, referencedColumns, simplePredsOnly);
        pushable = (getLeftOperand().categorize(referencedTabs, referencedColumns, simplePredsOnly) && pushable);
        if (getRightOperand() != null)
        {
            pushable = (getRightOperand().categorize(referencedTabs, referencedColumns, simplePredsOnly) && pushable);
        }
        return pushable;
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
        if (getReceiver().requiresTypeFromContext())
        {
            /*
            ** According to the SQL standard, if trim has a ? receiver,
            ** its type is varchar with the implementation-defined maximum length
            ** for a varchar.
            */

            getReceiver().setType(getVarcharDescriptor());
            //check if this parameter can pick up it's collation from the
            //character that will be used for trimming. If not(meaning the
            //character to be trimmed is also a parameter), then it will take
            //it's collation from the compilation schema.
            if (!getLeftOperand().requiresTypeFromContext()) {
                getReceiver().setCollationInfo(getLeftOperand().getTypeServices());
            } else {
                getReceiver().setCollationUsingCompilationSchema();
            }
        }

        /* Is there a ? parameter on the left? */
        if (getLeftOperand().requiresTypeFromContext())
        {
            /* Set the left operand type to varchar. */
            getLeftOperand().setType(getVarcharDescriptor());
            //collation of ? operand should be picked up from the context.
            //By the time we come here, receiver will have correct collation
            //set on it and hence we can rely on it to get correct collation
            //for the ? for the character that needs to be used for trimming.
            getLeftOperand().setCollationInfo(getReceiver().getTypeServices());
        }

        bindToBuiltIn();

        /*
        ** Check the type of the receiver - this function is allowed only on
        ** string value types.
        */
        receiverType = getReceiver().getTypeId();
        if (receiverType.userType())
            throwBadType("trim", receiverType.getSQLTypeName());

        setReceiver(castArgToString(getReceiver()));

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
        leftCTI = getLeftOperand().getTypeId();
        if (leftCTI.userType())
            throwBadType("trim", leftCTI.getSQLTypeName());

        setLeftOperand(castArgToString(getLeftOperand()));

        /*
        ** The result type of trim is varchar.
        */
        setResultType(resultType);
        //Result of TRIM should pick up the collation of the character string
        //that is getting trimmed (which is variable receiver) because it has
        //correct collation set on it.
        setCollationInfo(getReceiver().getTypeServices());

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
                        getReceiver().getTypeServices().getMaximumWidth()
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
        if( getReceiver().requiresTypeFromContext())
        {
            if( getLeftOperand().requiresTypeFromContext())
            {
                // we cannot tell whether it is StringType or BitType, so set to BitType as default
                getReceiver().setType(getVarBitDescriptor());
                //Since both receiver and leftOperands are parameters, use the
                //collation of compilation schema for receiver.
                getReceiver().setCollationUsingCompilationSchema();
            }
            else
            {
                if( getLeftOperand().getTypeId().isStringTypeId() || getLeftOperand().getTypeId().isBitTypeId())
                {
                    //Since the leftOperand is not a parameter, receiver will
                    //get it's collation from leftOperand through following
                    //setType method
                    getReceiver().setType(
                                     getLeftOperand().getTypeServices());
                }
            }
        }

        /*
         * Is there a ? parameter for the second arg.  Copy the receiver's.
         * If the receiver are both parameters, both will be max length.
         */
        if(getLeftOperand().requiresTypeFromContext())
        {
            if(getReceiver().requiresTypeFromContext())
            {
                // we cannot tell whether it is StringType or BitType, so set to BitType as default
                getLeftOperand().setType(getVarBitDescriptor());
            }
            else
            {
                if( getReceiver().getTypeId().isStringTypeId() || getReceiver().getTypeId().isBitTypeId())
                {
                    getLeftOperand().setType(
                                     getReceiver().getTypeServices());
                }
            }
            //collation of ? operand should be picked up from the context.
            //By the time we come here, receiver will have correct collation
            //set on it and hence we can rely on it to get correct collation
            //for this ?
            getLeftOperand().setCollationInfo(getReceiver().getTypeServices());
        }

        /*
         * Is there a ? paramter for the third arg.  It will be an int.
         */
        if( getRightOperand().requiresTypeFromContext())
        {
            getRightOperand().setType(
                new DataTypeDescriptor(TypeId.INTEGER_ID, true));
        }

        bindToBuiltIn();

        /*
        ** Check the type of the operand - this function is allowed only
        ** for: receiver = CHAR or CharBit
        **      firstOperand = CHAR or CharBit
        **      secondOperand = INT
        */
        secondOperandType = getLeftOperand().getTypeId();
        offsetType = getRightOperand().getTypeId();
        firstOperandType = getReceiver().getTypeId();

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
            setReceiver(castArgToVarBit(getReceiver()));
        } else if (firstOperandType.isBitTypeId() && secondOperandType.isStringTypeId()) {
            // we do not support cast of Long varchar and CLOB
            if (secondOperandType.isClobTypeId() || secondOperandType.isLongVarcharTypeId()) {
                throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE,
                        "LOCATE", "FUNCTION");
            }
            setLeftOperand(castArgToVarBit(getLeftOperand()));
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
        return castArgToString(vn, false);
    }

    /* cast arg to a varchar */
    protected ValueNode castArgToString(ValueNode vn, boolean parseSingleByteCharacterSet) throws StandardException
    {
        TypeCompiler vnTC = vn.getTypeCompiler();
        if (! vn.getTypeId().isStringTypeId())
        {
            DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true,
                    vnTC.getCastToCharWidth(
                            vn.getTypeServices(), getCompilerContext()));

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
            if(parseSingleByteCharacterSet && isBitDataString(vn)) {
                ((CastNode) newNode).setForSbcsData(true);
            }
            return newNode;
        }
        return vn;
    }

    protected boolean isBitDataString(ValueNode vn) throws StandardException {
        return vn.getTypeId().isVarBitDataTypeId() || vn.getTypeId().isFixedBitDataTypeId() || vn.getTypeId().isLongVarbinaryTypeId();
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
        if (getReceiver().requiresTypeFromContext())
        {
            /*
            ** According to the SQL standard, if substr has a ? receiver,
            ** its type is varchar with the implementation-defined maximum length
            ** for a varchar.
            */
            getReceiver().setType(getVarcharDescriptor());
            //collation of ? operand should be same as the compilation schema
            //because that is the only context available for us to pick up the
            //collation. There are no other character operands to SUBSTR method
            //to pick up the collation from.
            getReceiver().setCollationUsingCompilationSchema();
        }

        /* Is there a ? parameter on the left? */
        if (getLeftOperand().requiresTypeFromContext())
        {
            /* Set the left operand type to int. */
            getLeftOperand().setType(
                new DataTypeDescriptor(TypeId.INTEGER_ID, true));
        }

        /* Is there a ? parameter on the right? */
        if ((getRightOperand() != null) && getRightOperand().requiresTypeFromContext())
        {
            /* Set the right operand type to int. */
            getRightOperand().setType(
                new DataTypeDescriptor(TypeId.INTEGER_ID, true));
        }

        bindToBuiltIn();

        if (!getLeftOperand().getTypeId().isNumericTypeId() ||
            (getRightOperand() != null && !getRightOperand().getTypeId().isNumericTypeId()))
            throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, "SUBSTR", "FUNCTION");

        /*
        ** Check the type of the receiver - this function is allowed only on
        ** string value types.
        */
        receiverType = getReceiver().getTypeId();
        if (!receiverType.isStringTypeId() && !receiverType.isBitTypeId()) {
            throwBadType("SUBSTR", receiverType.getSQLTypeName());
        }


        // Determine the maximum length of the result
        int maximumWidth = getReceiver().getTypeServices().getMaximumWidth();
        boolean isFixedLength = false;
        int resultLen = maximumWidth;

        TypeId    resultType;

        // receiver is fixed type and either start or length is constant, then result should be fixed type with known length;
        // or receiver is vartype and length is constant, then result should be fixed type with known length
        if ((receiverType.getTypeFormatId() == StoredFormatIds.CHAR_TYPE_ID ||
            receiverType.getTypeFormatId() == StoredFormatIds.BIT_TYPE_ID)) {
            if (getRightOperand() != null && getRightOperand() instanceof ConstantNode)
            {
                resultLen = ((ConstantNode)getRightOperand()).getValue().getInt();
                isFixedLength = true;
            } else if (getLeftOperand() instanceof ConstantNode) {
                int startPostion = ((ConstantNode)getLeftOperand()).getValue().getInt();
                resultLen = maximumWidth - startPostion + 1;
                isFixedLength = true;
            }
        } else if ((receiverType.getTypeFormatId() == StoredFormatIds.VARCHAR_TYPE_ID ||
                receiverType.getTypeFormatId() == StoredFormatIds.VARBIT_TYPE_ID)) {
            if (getRightOperand() != null && getRightOperand() instanceof ConstantNode)
            {
                resultLen = ((ConstantNode)getRightOperand()).getValue().getInt();
                isFixedLength = true;
            }
        } else {
            if (getRightOperand() != null && getRightOperand() instanceof ConstantNode)
            {
                resultLen =((ConstantNode)getRightOperand()).getValue().getInt();
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
        setCollationInfo(getReceiver().getTypeServices());
        return this;
    }

    public ValueNode rightBind() throws StandardException
    {
        TypeId    receiverType;
        TypeId    resultType = TypeId.getBuiltInTypeId(Types.VARCHAR);

        if (getReceiver().requiresTypeFromContext())
        {
            getReceiver().setType(getVarcharDescriptor());
            getReceiver().setCollationUsingCompilationSchema();
        }
        if (getLeftOperand().requiresTypeFromContext())
        {
            getLeftOperand().setType(new DataTypeDescriptor(TypeId.INTEGER_ID, false));
        }

        bindToBuiltIn();

        if (!getLeftOperand().getTypeId().isIntegerNumericTypeId())
            throwBadType("RIGHT", getLeftOperand().getTypeId().getSQLTypeName());

        receiverType = getReceiver().getTypeId();
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

        int resultLen = getReceiver().getTypeServices().getMaximumWidth();

        if (getLeftOperand() instanceof ConstantNode)
        {
            if (((ConstantNode)getLeftOperand()).getValue().getInt() > resultLen)
                resultLen = ((ConstantNode)getLeftOperand()).getValue().getInt();
        }

        setType(new DataTypeDescriptor(
                resultType,
                true,
                resultLen
        ));
        setCollationInfo(getReceiver().getTypeServices());
        return this;
    }

    public ValueNode leftBind() throws StandardException
    {
        TypeId    receiverType;
        TypeId    resultType = TypeId.getBuiltInTypeId(Types.VARCHAR);

        if (getReceiver().requiresTypeFromContext())
        {
            getReceiver().setType(getVarcharDescriptor());
            getReceiver().setCollationUsingCompilationSchema();
        }
        if (getLeftOperand().requiresTypeFromContext())
        {
            getLeftOperand().setType(new DataTypeDescriptor(TypeId.INTEGER_ID, false));
        }

        bindToBuiltIn();

        if (!getLeftOperand().getTypeId().isIntegerNumericTypeId())
            throwBadType("LEFT", getLeftOperand().getTypeId().getSQLTypeName());

        receiverType = getReceiver().getTypeId();
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

        int resultLen = getReceiver().getTypeServices().getMaximumWidth();

        if (getLeftOperand() instanceof ConstantNode)
        {
            if (((ConstantNode)getLeftOperand()).getValue().getInt() > resultLen)
                resultLen = ((ConstantNode)getLeftOperand()).getValue().getInt();
        }

        setType(new DataTypeDescriptor(
                resultType,
                true,
                resultLen
        ));
        setCollationInfo(getReceiver().getTypeServices());
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
        if (getReceiver().requiresTypeFromContext())
        {
            // According to the SQL standard, if replace has a ? receiver,
            // its type is varchar with the implementation-defined maximum length
            // for a varchar.
            getReceiver().setType(getVarcharDescriptor());

            // collation of ? operand should be same as the compilation schema
            // because that is the only context available for us to pick up the
            // collation. There are no other character operands to SUBSTR method
            // to pick up the collation from.
            getReceiver().setCollationUsingCompilationSchema();
        }

        /* Is there a ? parameter on the left? */
        if (getLeftOperand().requiresTypeFromContext())
        {
            /* Set the left operand type to VARCHAR. */
            getLeftOperand().setType(
                new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true));
        }

        /* Is there a ? parameter on the right? */
        if ((getRightOperand() != null) && getRightOperand().requiresTypeFromContext())
        {
            /* Set the right operand type to VARCHAR. */
            getRightOperand().setType(
                new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true));
        }

        bindToBuiltIn();

        if (!getLeftOperand().getTypeId().isStringTypeId() ||
            (getRightOperand() != null && !getRightOperand().getTypeId().isStringTypeId()))
            throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, "REPLACE", "FUNCTION");

        // Check the type of the receiver - this function is allowed only on
        // string value types.
        receiverType = getReceiver().getTypeId();
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
        int maxResultLen = getReceiver().getTypeServices().getMaximumWidth();
        setType(new DataTypeDescriptor(resultType, true, maxResultLen));

        // Result of REPLACE should pick up the collation of the 1st argument
        // to REPLACE. The 1st argument to REPLACE is represented by the variable
        // receiver in this class.
        setCollationInfo(getReceiver().getTypeServices());

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
        if( ! bindParameter( getRightOperand(), Types.INTEGER))
        {
            int jdbcType = getRightOperand().getTypeId().getJDBCTypeId();
            if( jdbcType != Types.TINYINT && jdbcType != Types.SMALLINT &&
                jdbcType != Types.INTEGER && jdbcType != Types.BIGINT)
                throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARG_TYPE,
                                                     getRightOperand().getTypeId().getSQLTypeName(),
                                                     ReuseFactory.getInteger( 2),
                                                     operator);
        }
        bindDateTimeArg( getReceiver(), 3);
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
        bindDateTimeArg( getRightOperand(), 2);
        bindDateTimeArg( getReceiver(), 3);
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
     * Binds the split_part expression.
     *
     * @return the new top of the expression tree.
     *
     * @exception StandardException thrown on error
     */

    public ValueNode splitPartBind()
            throws StandardException
    {
        TypeId    receiverType;
        TypeId    resultType = TypeId.getBuiltInTypeId(Types.VARCHAR);

        bindToBuiltIn();

        if (!getLeftOperand().getTypeId().isStringTypeId() || !getRightOperand().getTypeId().isNumericTypeId())
            throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, "SPLIT_PART", "FUNCTION");

        // Check the type of the receiver - this function is allowed only on
        // string value types.
        receiverType = getReceiver().getTypeId();
        switch (receiverType.getJDBCTypeId())
        {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                break;
            default:
            {
                throwBadType("SPLIT_PART", receiverType.getSQLTypeName());
            }
        }
        if (receiverType.getTypeFormatId() == StoredFormatIds.CLOB_TYPE_ID) {
            resultType = receiverType;
        }

        // Determine the maximum length of the result string
        int maxResultLen = getReceiver().getTypeServices().getMaximumWidth();
        setType(new DataTypeDescriptor(resultType, true, maxResultLen));

        setCollationInfo(getReceiver().getTypeServices());

        return this;
    }

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
        return operands.get(0);
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
        if (getReceiver().getTypeId().userType())
        {
            setReceiver(getReceiver().genSQLJavaSQLTree());
        }

        /* If the left operand is not a built-in type, then generate a bound conversion
         * tree to a built-in type.
         */
        if (getLeftOperand().getTypeId().userType())
        {
            setLeftOperand(getLeftOperand().genSQLJavaSQLTree());
        }

        /* If the right operand is not a built-in type, then generate a bound conversion
         * tree to a built-in type.
         */
        if (getRightOperand() != null)
        {
            if (getRightOperand().getTypeId().userType())
            {
                setRightOperand(getRightOperand().genSQLJavaSQLTree());
            }
        }
    }

    private DataTypeDescriptor getVarcharDescriptor() {
        return new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true);
    }

    private DataTypeDescriptor getVarBitDescriptor() {
        return new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARBINARY), true);
    }

    // Following 3 interfaces only applicable when this is a TRIM operator.
    public boolean isLeading()  {return trimType == StringDataValue.LEADING;}
    public boolean isTrailing() {return trimType == StringDataValue.TRAILING;}
    public boolean isBoth()     {return trimType == StringDataValue.BOTH;}

    @Override
    public ValueNode replaceIndexExpression(ResultColumnList childRCL) throws StandardException {
        if (childRCL == null) {
            return this;
        }
        // this special handling for like predicate is fine because like cannot appear in index expressions
        if (operatorType == LIKE) {
            if (getReceiver() != null) {
                setReceiver(getReceiver().replaceIndexExpression(childRCL));
            }
            if (getLeftOperand() != null) {
                setLeftOperand(getLeftOperand().replaceIndexExpression(childRCL));
            }
            if (getRightOperand() != null) {
                setRightOperand(getRightOperand().replaceIndexExpression(childRCL));
            }
            return this;
        } else {
            return super.replaceIndexExpression(childRCL);
        }
    }

    @Override
    public boolean collectExpressions(Map<Integer, Set<ValueNode>> exprMap) {
        // this special handling for like predicate is fine because like cannot appear in index expressions
        if (operatorType == LIKE) {
            boolean result = true;
            if (getReceiver() != null) {
                result = getReceiver().collectExpressions(exprMap);
            }
            if (getLeftOperand() != null) {
                result = result && getLeftOperand().collectExpressions(exprMap);
            }
            if (getRightOperand() != null) {
                result = result && getRightOperand().collectExpressions(exprMap);
            }
            return result;
        } else {
            return this.collectSingleExpression(exprMap);
        }
    }
}
