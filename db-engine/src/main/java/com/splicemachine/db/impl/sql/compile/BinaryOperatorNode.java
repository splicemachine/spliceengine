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
import com.splicemachine.db.iapi.reference.JDBC40Translation;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A BinaryOperatorNode represents a built-in binary operator as defined by
 * the ANSI/ISO SQL standard.  This covers operators like +, -, *, /, =, <, etc.
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 */

public class BinaryOperatorNode extends OperatorNode
{
    ValueNode    receiver; // used in generation

    /*
    ** These identifiers are used in the grammar.
    */
    public final static int PLUS    = 1;
    public final static int MINUS    = 2;
    public final static int TIMES    = 3;
    public final static int DIVIDE    = 4;
    public final static int CONCATENATE    = 5;
    public final static int EQ    = 6;
    public final static int NE    = 7;
    public final static int GT    = 8;
    public final static int GE    = 9;
    public final static int LT    = 10;
    public final static int LE    = 11;
    public final static int AND    = 12;
    public final static int OR    = 13;
    public final static int LIKE    = 14;
    public final static int MOD    = 15;

    int            operatorType;

    /* If an operand matches an index expression. Once set,
     * values should be valid through the whole optimization
     * process of the current query.
     * -1  : no match
     * >=0 : table number
     */
    protected int leftMatchIndexExpr  = -1;
    protected int rightMatchIndexExpr = -1;

    /* The following four fields record operand matches for
     * which conglomerate and which column. These values
     * are reset and valid only for the current access path.
     * They should not be used beyond cost estimation.
     */
    protected ConglomerateDescriptor leftMatchIndexExprConglomDesc = null;
    protected ConglomerateDescriptor rightMatchIndexExprConglomDesc = null;

    // 0-based index column position
    protected int leftMatchIndexExprColumnPosition  = -1;
    protected int rightMatchIndexExprColumnPosition = -1;

    // At the time of adding XML support, it was decided that
    // we should avoid creating new OperatorNodes where possible.
    // So for the XML-related binary operators we just add the
    // necessary code to _this_ class, similar to what is done in
    // TernarnyOperatorNode. Subsequent binary operators (whether
    // XML-related or not) should follow this example when
    // possible.

    public final static int XMLEXISTS_OP = 0;
    public final static int XMLQUERY_OP = 1;
    public final static int REPEAT = 2;
    public final static int SIMPLE_LOCALE_STRING = 3;
    public final static int MULTIPLY_ALT = 3;

    // NOTE: in the following 4 arrays, order
    // IS important.

    static final String[] BinaryOperators = {
        "xmlexists",
        "xmlquery",
        "repeat",
        "multiply_alt"
    };

    static final String[] BinaryMethodNames = {
        "XMLExists",
        "XMLQuery",
        "repeat",
        "multiply_alt"
    };

    static final String[] BinaryResultTypes = {
        ClassName.BooleanDataValue,        // XMLExists
        ClassName.XMLDataValue,            // XMLQuery
        ClassName.StringDataValue,      // repeat
        ClassName.NumberDataValue       // multiply_alt
    };

    static final String[][] BinaryArgTypes = {
        {ClassName.StringDataValue, ClassName.XMLDataValue},    // XMLExists
        {ClassName.StringDataValue, ClassName.XMLDataValue},    // XMLQuery
        {ClassName.StringDataValue, ClassName.NumberDataValue}, // repeat
        {ClassName.NumberDataValue, ClassName.NumberDataValue}, // multiply_alt
    };

    /** The query expression if the operator is XMLEXISTS or XMLQUERY. */
    private String xmlQuery;

    /**
     * Initializer for a BinaryOperatorNode
     *
     * @param leftOperand    The left operand of the node
     * @param rightOperand    The right operand of the node
     * @param operator        The name of the operator
     * @param methodName    The name of the method to call for this operator
     * @param leftInterfaceType    The name of the interface for the left operand
     * @param rightInterfaceType    The name of the interface for the right
     *                                operand
     */

    public void init(
            Object leftOperand,
            Object rightOperand,
            Object operator,
            Object methodName,
            Object leftInterfaceType,
            Object rightInterfaceType)
    {
        operands = new ArrayList<>(Arrays.asList((ValueNode)leftOperand, (ValueNode)rightOperand));
        interfaceTypes = new ArrayList<>(Arrays.asList((String) leftInterfaceType, (String)rightInterfaceType));
        this.operator = (String) operator;
        this.methodName = (String) methodName;
        this.operatorType = -1;
    }

    public void init(
            Object leftOperand,
            Object rightOperand,
            Object leftInterfaceType,
            Object rightInterfaceType)
    {
        operands = new ArrayList<>(Arrays.asList((ValueNode)leftOperand, (ValueNode)rightOperand));
        interfaceTypes = new ArrayList<>(Arrays.asList((String) leftInterfaceType, (String)rightInterfaceType));
        this.operatorType = -1;
    }

    public void init(
            Object leftOperand,
            Object rightOperand,
            Object operator,
            Object methodName,
            Object leftInterfaceType,
            Object rightInterfaceType,
            Object resultInterfaceType,
            int operatorType)
    {
        init(leftOperand, rightOperand, operator, methodName, leftInterfaceType, rightInterfaceType);
        this.operatorType = operatorType;
        this.resultInterfaceType = (String) resultInterfaceType;
    }


    /**
     * Initializer for a BinaryOperatorNode
     *
     * @param leftOperand    The left operand of the node
     * @param rightOperand    The right operand of the node
     * @param opType  An Integer holding the operatorType
     *  for this operator.
     */
    public void init(
            Object leftOperand,
            Object rightOperand,
            Object opType)
    {
        operands = new ArrayList<>(Arrays.asList((ValueNode)leftOperand, (ValueNode)rightOperand));
        this.operatorType = (Integer) opType;
        this.interfaceTypes = new ArrayList<>(Arrays.asList(BinaryArgTypes[this.operatorType]));
        this.operator = BinaryOperators[this.operatorType];
        this.methodName = BinaryMethodNames[this.operatorType];
        this.resultInterfaceType = BinaryResultTypes[this.operatorType];
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
                super.toString();
        }
        else
        {
            return "";
        }
    }

    /**
     * Set the operator.
     *
     * @param operator    The operator.
     */
    void setOperator(String operator)
    {
        this.operator = operator;
        this.operatorType = -1;
    }

    /**
     * Set the methodName.
     *
     * @param methodName    The methodName.
     */
    void setMethodName(String methodName)
    {
        this.methodName = methodName;
        this.operatorType = -1;
    }

    /**
     * Set the interface type for the left and right arguments.
     * Used when we don't know the interface type until
     * later in binding.
     */
    protected void setLeftRightInterfaceType(String iType)
    {
        for (int i = 0; i < interfaceTypes.size(); ++i) {
            interfaceTypes.set(i, iType);
        }
        this.operatorType = -1;
    }

    protected void bindParameters() throws StandardException {
        /* Is there a ? parameter on the left? */
        if (getLeftOperand().requiresTypeFromContext()) {
            /* Default Splice behavior:
             * It's an error if both operands are ? parameters.
             * This is overridden is some of the derived classes to be DB2 compatible.
             * DB2 doc on this:
             * https://www.ibm.com/support/knowledgecenter/SSEPGG_11.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0053561.html
             */
            if (getRightOperand().requiresTypeFromContext()) {
                throw StandardException.newException(SQLState.LANG_BINARY_OPERANDS_BOTH_PARMS,
                        operator);
            }

            /* Set the left operand to the type of right parameter. */
            getLeftOperand().setType(getRightOperand().getTypeServices());
        }
        /* Is there a ? parameter on the right? */
        else if (getRightOperand().requiresTypeFromContext()) {
            /* Set the right operand to the type of the left parameter. */
            getRightOperand().setType(getLeftOperand().getTypeServices());
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
    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode>    aggregateVector) throws StandardException {
        bindOperands(fromList, subqueryList, aggregateVector);

        if ((operatorType == XMLEXISTS_OP) || (operatorType == XMLQUERY_OP))
            return bindXMLQuery();
        else if (operatorType == REPEAT)
            return bindRepeat();
        else if (operatorType == MULTIPLY_ALT)
            return bindMultiplyAlt(fromList, subqueryList, aggregateVector);

        bindParameters();

        // Simple date/time arithmetic
        if ("+".equals(operator) || "-".equals(operator)) {
            if (getLeftOperand().getTypeId().getJDBCTypeId() == Types.DATE ||
                getLeftOperand().getTypeId().getJDBCTypeId() == Types.TIMESTAMP)  {
                setLeftInterfaceType(ClassName.DateTimeDataValue);
            } else if (getLeftOperand().getTypeId().getJDBCTypeId() == Types.INTEGER) {
                setLeftInterfaceType(ClassName.NumberDataValue);
            }

            if (getRightOperand().getTypeId().getJDBCTypeId() == Types.DATE ||
                getRightOperand().getTypeId().getJDBCTypeId() == Types.TIMESTAMP)  {
                if (getLeftOperand().getTypeId().getJDBCTypeId() == Types.INTEGER && operator.equals("+")) {
                    // special case for n + <datetime> commutativity. Swap operands to: <datetime> + n
                    ValueNode temp = getLeftOperand().getClone();
                    setLeftOperand(getRightOperand().getClone());
                    setLeftInterfaceType(ClassName.DateTimeDataValue);
                    setRightOperand(temp);
                    setRightInterfaceType(ClassName.NumberDataValue);
                } else {
                    setRightInterfaceType(ClassName.DateTimeDataValue);
                }
            } else if (getRightOperand().getTypeId().getJDBCTypeId() == Types.INTEGER) {
                setRightInterfaceType(ClassName.NumberDataValue);
            }
        }

        return genSQLJavaSQLTree();
    }

    /**
     * Bind an XMLEXISTS or XMLQUERY operator.  Makes sure
     * the operand type and target type are both correct
     * and sets the result type.
     *
     * @exception StandardException Thrown on error
     */
    public ValueNode bindXMLQuery()
        throws StandardException
    {
        // Check operand types.
        TypeId rightOperandType = getRightOperand().getTypeId();

        // Left operand is query expression and must be a string
        // literal.  SQL/XML spec doesn't allow params nor expressions
        // 6.17: <XQuery expression> ::= <character string literal> 
        if (!(getLeftOperand() instanceof CharConstantNode))
        {
            throw StandardException.newException(
                SQLState.LANG_INVALID_XML_QUERY_EXPRESSION);
        }
        else {
            xmlQuery = ((CharConstantNode)getLeftOperand()).getString();
        }

        // Right operand must be an XML data value.  NOTE: This
        // is a Derby-specific restriction, not an SQL/XML one.
        // We have this restriction because the query engine
        // that we use (currently Xalan) cannot handle non-XML
        // context items.
        if ((rightOperandType != null) &&
            !rightOperandType.isXMLTypeId())
        {
            throw StandardException.newException(
                SQLState.LANG_INVALID_CONTEXT_ITEM_TYPE,
                rightOperandType.getSQLTypeName());
        }

        // Is there a ? parameter on the right?
        if (getRightOperand().requiresTypeFromContext())
        {
            // For now, since JDBC has no type defined for XML, we
            // don't allow binding to an XML parameter.
            throw StandardException.newException(
                SQLState.LANG_ATTEMPT_TO_BIND_XML);
        }

        // Set the result type of this operator.
        if (operatorType == XMLEXISTS_OP) {
        // For XMLEXISTS, the result type is always SQLBoolean.
        // The "true" in the next line says that the result
        // can be nullable--which it can be if evaluation of
        // the expression returns a null (this is per SQL/XML
        // spec, 8.4)
            setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, true));
        }
        else {
        // The result of an XMLQUERY operator is always another
        // XML data value, per SQL/XML spec 6.17: "...yielding a value
        // X1 of an XML type."
            setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                    JDBC40Translation.SQLXML));
        }

        return genSQLJavaSQLTree();
    }

    public ValueNode bindMultiplyAlt(FromList fromList,
                SubqueryList subqueryList,
                List<AggregateNode>    aggregateVector) throws StandardException
    {
        boolean castToDecfloat =
                getLeftOperand().requiresTypeFromContext() ||
                getLeftOperand().getTypeId() == null ||
                !(getLeftOperand().getTypeId().isIntegerNumericTypeId() || getLeftOperand().getTypeId().getJDBCTypeId() == Types.DECIMAL) ||
                getRightOperand().requiresTypeFromContext() ||
                getRightOperand().getTypeId() == null ||
                !(getRightOperand().getTypeId().isIntegerNumericTypeId() || getRightOperand().getTypeId().getJDBCTypeId() == Types.DECIMAL);
        if (castToDecfloat) {
            setLeftOperand((ValueNode)
                    getNodeFactory().getNode(
                            C_NodeTypes.CAST_NODE,
                            getLeftOperand(),
                            DataTypeDescriptor.getBuiltInDataTypeDescriptor(com.splicemachine.db.iapi.reference.Types.DECFLOAT),
                            getContextManager()));
            ((CastNode) getLeftOperand()).bindCastNodeOnly();
            setRightOperand((ValueNode)
                    getNodeFactory().getNode(
                            C_NodeTypes.CAST_NODE,
                            getRightOperand(),
                            DataTypeDescriptor.getBuiltInDataTypeDescriptor(com.splicemachine.db.iapi.reference.Types.DECFLOAT),
                            getContextManager()));
            ((CastNode) getRightOperand()).bindCastNodeOnly();
        }
        ValueNode multiplication = ((BinaryArithmeticOperatorNode) getNodeFactory().getNode(
                C_NodeTypes.BINARY_TIMES_OPERATOR_NODE,
                getLeftOperand(),
                getRightOperand(),
                getContextManager())).bindExpression(fromList, subqueryList, aggregateVector);
        if (castToDecfloat) {
            return multiplication;
        } else {
            int leftPrecision = getLeftOperand().getTypeServices().getPrecision();
            int rightPrecision = getRightOperand().getTypeServices().getPrecision();
            int leftScale = getLeftOperand().getTypeServices().getScale();
            int rightScale = getRightOperand().getTypeServices().getScale();
            int precision = Math.min(38, leftPrecision + rightPrecision);
            int scale;
            if (leftScale == 0 && rightScale == 0) {
                scale = 0;
            } else if (leftPrecision + rightPrecision <= 38) {
                scale = Math.min(38, leftScale + rightScale);
            } else {
                scale = Math.max(Math.min(3, leftScale + rightScale), 38 - (leftPrecision - leftScale + rightPrecision - rightScale));
            }
            ValueNode castToDecimal = (ValueNode)
                getNodeFactory().getNode(
                        C_NodeTypes.CAST_NODE,
                        multiplication,
                        DataTypeDescriptor.getSQLDataTypeDescriptor(
                                "java.math.BigDecimal", precision, scale, true, precision),
                        getContextManager());
            ((CastNode) castToDecimal).bindCastNodeOnly();
            return castToDecimal;
    }
}

    public ValueNode bindRepeat() throws StandardException {
        /*
         * Is there a ? parameter for the first arg.
         */
        if( getLeftOperand().requiresTypeFromContext())
        {
            getLeftOperand().setType(new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true));
            getLeftOperand().setCollationUsingCompilationSchema();
        }

        /*
         * Is there a ? paramter for the second arg.  It will be an int.
         */
        if( getRightOperand().requiresTypeFromContext())
        {
            getRightOperand().setType(
                    new DataTypeDescriptor(TypeId.INTEGER_ID, true));
        }

        /*
        ** Check the type of the operand - this function is allowed only
        ** for: leftOperand = CHAR, VARCHAR, LONG VARCHAR
        **      rightOperand = INT
        */
        TypeId stringOperandType = getLeftOperand().getTypeId();
        TypeId repeatOperandType = getRightOperand().getTypeId();

        if (!stringOperandType.isStringTypeId() || stringOperandType.isClobTypeId())
            throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARG_TYPE, stringOperandType.getSQLTypeName(),
                    1, "FUNCTION");

        if (!repeatOperandType.isIntegerNumericTypeId())
            throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARG_TYPE, repeatOperandType.getSQLTypeName(),
                    2, "FUNCTION");

        /*
        ** The result type of a repeat is of the same type as the leftOperand
        */
        if (getRightOperand() instanceof ConstantNode) {
            int repeatTimes = ((ConstantNode) getRightOperand()).getValue().getInt();
            if (repeatTimes < 0)
                throw StandardException.newException(
                        SQLState.LANG_INVALID_FUNCTION_ARGUMENT, getRightOperand(), "REPEAT");
            int resultLength = getLeftOperand().getTypeId().getMaximumMaximumWidth();
            if (getLeftOperand().getTypeServices().getMaximumWidth() * repeatTimes < resultLength) {
                resultLength = getLeftOperand().getTypeServices().getMaximumWidth() * repeatTimes;
            }
            setType(new DataTypeDescriptor(stringOperandType, true, resultLength));
        } else {
            setType(new DataTypeDescriptor(stringOperandType, true));
        }

        return genSQLJavaSQLTree();

    }

    public boolean leftIsReceiver() throws StandardException {
        return (getLeftOperand().getTypeId().typePrecedence() >
                getRightOperand().getTypeId().typePrecedence() ||
                operatorType == REPEAT || operatorType == SIMPLE_LOCALE_STRING);
    }

    /**
     * Do code generation for this binary operator.
     *
     * @param acb    The ExpressionClassBuilder for the class we're generating
     * @param mb    The method the code to place the code
     *
     *
     * @exception StandardException        Thrown on error
     */

    public void generateExpression(ExpressionClassBuilder acb,
                                            MethodBuilder mb)
        throws StandardException
    {
        /* If this BinaryOperatorNode was created as a part of an IN-list
         * "probe predicate" then we do not want to generate the relational
         * operator itself; instead we want to generate the underlying
         * IN-list for which this operator node was created.
         *
         * We'll get here in situations where the optimizer chooses a plan
         * for which the probe predicate is *not* a useful start/stop key
         * and thus is not being used for execution-time index probing.
         * In this case we are effectively "reverting" the probe predicate
         * back to the InListOperatorNode from which it was created.  Or put
         * another way, we are "giving up" on index multi-probing and simply
         * generating the original IN-list as a regular restriction.
         */
        if (this instanceof BinaryRelationalOperatorNode)
        {
            InListOperatorNode ilon =
                ((BinaryRelationalOperatorNode)this).getInListOp();

            if (ilon != null)
            {
                ilon.generateExpression(acb, mb);
                return;
            }
        }

        String        resultTypeName;
        String        receiverType;

        /*
        ** if i have a operator.getOrderableType() == constant, then just cache
        ** it in a field.  if i have QUERY_INVARIANT, then it would be good to
        ** cache it in something that is initialized each execution,
        ** but how?
        */

        // The number of arguments to pass to the method that implements the
        // operator, depends on the type of the operator.
        int numArgs;

        // If we're dealing with XMLEXISTS or XMLQUERY, there is some
        // additional work to be done.
        boolean xmlGen =
            (operatorType == XMLQUERY_OP) || (operatorType == XMLEXISTS_OP);

        boolean leftIsReceiver = leftIsReceiver();
        boolean receiverIsNumeric =
                 (leftIsReceiver ? getLeftOperand().getTypeCompiler() instanceof NumericTypeCompiler :
                                   getRightOperand().getTypeCompiler() instanceof NumericTypeCompiler);
        boolean dupReceiver = (xmlGen ||
                              !(this instanceof BinaryArithmeticOperatorNode && receiverIsNumeric));
        /*
        ** The receiver is the operand with the higher type precedence.
        ** Like always makes the left the receiver.
        **
        */
        if (leftIsReceiver)
        {
            receiver = getLeftOperand();
            /*
            ** let the receiver type be determined by an
            ** overridable method so that if methods are
            ** not implemented on the lowest interface of
            ** a class, they can note that in the implementation
            ** of the node that uses the method.
            */
            receiverType = !dupReceiver ? getTypeCompiler().interfaceName() :
                (operatorType == -1) ? getReceiverInterfaceName() : getLeftInterfaceType();

            /*
            ** Generate (with <left expression> only being evaluated once)
            **
            **    <left expression>.method(<left expression>, <right expression>...)
            */
            if (dupReceiver)
                getLeftOperand().generateExpression(acb, mb);
            else
                acb.generateNull(mb, getTypeCompiler(), getTypeServices());

            mb.cast(receiverType); // cast the method instance
            // stack: receiver

            if (dupReceiver)
                mb.dup();
            else
                getLeftOperand().generateExpression(acb, mb);
            mb.cast(getLeftInterfaceType());
            // stack: left, left

            getRightOperand().generateExpression(acb, mb);
            mb.cast(getRightInterfaceType()); // second arg with cast
            // stack: receiver, left, right

            // We've pushed two arguments
            numArgs = 2;
        }
        else
        {
            receiver = getRightOperand();
            /*
            ** let the receiver type be determined by an
            ** overridable method so that if methods are
            ** not implemented on the lowest interface of
            ** a class, they can note that in the implementation
            ** of the node that uses the method.
            */
            receiverType = !dupReceiver ? getTypeCompiler().interfaceName() :
                (operatorType == -1) ? getReceiverInterfaceName() : getRightInterfaceType();

            /*
            ** Generate (with <right expression> only being evaluated once)
            **
            **    <right expression>.method(<left expression>, <right expression>)
            **
            ** UNLESS we're generating an XML operator such as XMLEXISTS.
            ** In that case we want to generate
            **
            **  <right expression>.method(sqlXmlUtil)
            */

            if (dupReceiver)
                getRightOperand().generateExpression(acb, mb);
            else
                acb.generateNull(mb, getTypeCompiler(), getTypeServices());

            mb.cast(receiverType); // cast the method instance
            // stack: receiver

            if (xmlGen) {
                // Push one argument (the SqlXmlUtil instance)
                numArgs = 1;
                pushSqlXmlUtil(acb, mb, xmlQuery, operator);
                // stack: right,sqlXmlUtil
            } else {
                // Push two arguments (left, right)
                numArgs = 2;

                if (dupReceiver)
                    mb.dup();
                else
                    getRightOperand().generateExpression(acb, mb);
                mb.cast(getRightInterfaceType());
                // stack: receiver,right

                getLeftOperand().generateExpression(acb, mb);
                mb.cast(getLeftInterfaceType()); // second arg with cast
                // stack: receiver,right,left

                mb.swap();
                // stack: receiver,left,right
            }
        }

        /* Figure out the result type name */
        resultTypeName = (operatorType == -1)
            ? getTypeCompiler().interfaceName()
            : resultInterfaceType;

        // Boolean return types don't need a result field. For other types,
        // allocate an object for re-use to hold the result of the operator.
        boolean genResultField = !getTypeId().isBooleanTypeId();

        // Push the result field onto the stack, if there is a result field.
        if (genResultField) {
            /*
             ** Call the method for this operator.
             */
            // Don't push a null for decimal types, because it causes
            // easy overflow.
            if (getTypeId().isDecimalTypeId()) {
                LocalField resultField = acb.newFieldDeclaration(Modifier.PRIVATE, resultTypeName);
                mb.getField(resultField);
            }
            else {
                acb.generateNull(mb, getTypeCompiler(), getTypeServices());
                mb.cast(resultTypeName); // cast the method instance; // third arg
            }
            // Adjust number of arguments for the result field
            numArgs++;

            /* pass statically calculated scale to decimal divide method to make
             * result set scale consistent, beetle 3901
             */
            int jdbcType;
            if ((getTypeServices() != null) &&
                ((jdbcType = getTypeServices().getJDBCTypeId()) == java.sql.Types.DECIMAL ||
                 jdbcType == java.sql.Types.NUMERIC) &&
                operator.equals("/"))
            {
                mb.push(getTypeServices().getScale());        // 4th arg
                numArgs++;
            }
        }

        mb.callMethod(VMOpcode.INVOKEINTERFACE, receiverType,
                      methodName, resultTypeName, numArgs);

        // Store the result of the method call, if there is a result field.
        if (genResultField) {
            //the need for following if was realized while fixing bug 5704 where decimal*decimal was resulting an overflow value but we were not detecting it
            if (getTypeId().variableLength() && //since result type is numeric variable length, generate setWidth code.
                receiver.getTypeServices().getTypeId().variableLength()) //receiver type may be different from result.
            {
                if (getTypeId().isNumericTypeId())
                {
                    // to leave the DataValueDescriptor value on the stack, since setWidth is void
                    mb.dup();

                    mb.push(getTypeServices().getPrecision());
                    mb.push(getTypeServices().getScale());
                    mb.push(true);
                    mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.VariableSizeDataValue, "setWidth", "void", 3);
                }
            }
        }
    }

    /**
     * Set the leftOperand to the specified ValueNode
     *
     * @param newLeftOperand    The new leftOperand
     */
    public void setLeftOperand(ValueNode newLeftOperand)
    {
        operands.set(0, newLeftOperand);
    }

    /**
     * Get the leftOperand
     *
     * @return The current leftOperand.
     */
    public ValueNode getLeftOperand()
    {
        return operands.get(0);
    }

    /**
     * Set the rightOperand to the specified ValueNode
     *
     * @param newRightOperand    The new rightOperand
     */
    public void setRightOperand(ValueNode newRightOperand)
    {
        operands.set(1, newRightOperand);
    }

    /**
     * Get the rightOperand
     *
     * @return The current rightOperand.
     */
    public ValueNode getRightOperand()
    {
        return operands.get(1);
    }

    void setLeftInterfaceType(String iType) {
        interfaceTypes.set(0, iType);
    }

    void setRightInterfaceType(String iType) {
        interfaceTypes.set(1, iType);
    }

    String getLeftInterfaceType() {
        return interfaceTypes.get(0);
    }

    String getRightInterfaceType() {
        return interfaceTypes.get(1);
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
    public boolean categorize(JBitSet referencedTabs,
                              ReferencedColumnsMap referencedColumns,
                              boolean simplePredsOnly)
        throws StandardException
    {
        boolean pushable;

        pushable = getLeftOperand().categorize(referencedTabs,
                                          referencedColumns, simplePredsOnly);
        pushable = (getRightOperand().categorize(referencedTabs,
                                            referencedColumns, simplePredsOnly) && pushable);

        if (getLeftOperand() instanceof ColumnReference) {
            ColumnReference lcr = (ColumnReference) getLeftOperand();
            if (lcr.getSource().getExpression() instanceof CurrentRowLocationNode) {
                if (methodName.compareToIgnoreCase("NOTEQUALS") == 0) {
                    return false;
                }
            }
            if (getRightOperand() instanceof ColumnReference) {
                ColumnReference rcr = (ColumnReference) getRightOperand();
                if (rcr.getSource().getExpression() instanceof CurrentRowLocationNode) {
                    return false;
                }
            }
        }
        return pushable;
    }


    /**
     * Determine the type the binary method is called on.
     * By default, based on the receiver.
     *
     * Override in nodes that use methods on super-interfaces of
     * the receiver's interface, such as comparisons.
     *
     * @exception StandardException        Thrown on error
     */
    public String getReceiverInterfaceName() throws StandardException {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(receiver!=null,"can't get receiver interface name until receiver is set");
        }

        return receiver.getTypeCompiler().interfaceName();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isSemanticallyEquivalent(ValueNode o) throws StandardException {
        if (isCommutative()) {
            if (!isSameNodeType(o)) {
                return false;
            }
            BinaryOperatorNode other = (BinaryOperatorNode) o;
            if (methodName.equals(other.methodName)) {
                return (getLeftOperand().isSemanticallyEquivalent(other.getLeftOperand()) &&
                        getRightOperand().isSemanticallyEquivalent(other.getRightOperand())) ||
                        (getLeftOperand().isSemanticallyEquivalent(other.getRightOperand()) &&
                        getRightOperand().isSemanticallyEquivalent(other.getLeftOperand()));
            }
        }
        return super.isSemanticallyEquivalent(o);
    }

    /**
     * Check if this binary operator is commutative on its operands.
     * @return True if it is commutative, false otherwise.
     */
    public boolean isCommutative() {
        if (methodName == null)
            return false;
        // Only methodName is always set for all kinds of binary operators.
        // Do not use operator or operatorType here.
        switch (methodName) {
            case "plus":
            case "times":
            case "equals":
            case "notEquals":
            case "and":
            case "or":
                return true;
            default:
                break;
        }
        return false;
    }

    public boolean isRepeat () { return this.operatorType == REPEAT; }

    public int getOperatorType() { return operatorType; }

    public void setMatchIndexExpr(int tableNumber, int columnPosition, ConglomerateDescriptor conglomDesc, boolean isLeft) {
        if (isLeft) {
            this.leftMatchIndexExpr = tableNumber;
            this.leftMatchIndexExprColumnPosition = columnPosition;
            this.leftMatchIndexExprConglomDesc = conglomDesc;
        } else {
            this.rightMatchIndexExpr = tableNumber;
            this.rightMatchIndexExprColumnPosition = columnPosition;
            this.rightMatchIndexExprConglomDesc = conglomDesc;
        }
    }

    public int getLeftMatchIndexExprTableNumber() { return this.leftMatchIndexExpr; }
    public int getRightMatchIndexExprTableNumber() { return this.rightMatchIndexExpr; }
}

