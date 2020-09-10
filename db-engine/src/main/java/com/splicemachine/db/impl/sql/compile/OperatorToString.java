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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicate;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.system.SimpleSparkVersion;
import com.splicemachine.system.SparkVersion;
import org.apache.commons.lang3.mutable.MutableInt;

import static com.splicemachine.db.iapi.reference.Property.SPLICE_SPARK_COMPILE_VERSION;
import static com.splicemachine.db.iapi.reference.Property.SPLICE_SPARK_VERSION;
import static com.splicemachine.db.iapi.services.io.StoredFormatIds.*;
import static java.lang.String.format;
import static com.splicemachine.db.impl.sql.compile.SparkConstantExpression.SpecialValue;
/**
 * Utility to get the string representation of a given operator.
 * <p/>
 * Used for debugging.
 */
public class OperatorToString {

    public boolean      sparkExpression;
    public SparkVersion sparkVersion;
    public MutableInt   relationalOpDepth;
    public SparkExpressionNode sparkExpressionTree;
    public boolean buildExpressionTree;
    public int leftResultSetNumber;
    public int rightResultSetNumber;

    private static final SparkVersion spark_2_2_0 = new SimpleSparkVersion("2.2.0");
    private static final SparkVersion spark_2_3_0 = new SimpleSparkVersion("2.3.0");

    OperatorToString(boolean    sparkExpression,
                     MutableInt relationalOpDepth,
                     boolean    buildExpressionTree,
                     int        leftResultSetNumber,
                     int        rightResultSetNumber) {
        this.sparkExpression   = sparkExpression;
        this.sparkVersion      = getSparkVersion();
        this.relationalOpDepth = relationalOpDepth;
        this.sparkExpressionTree = null;
        this.buildExpressionTree = buildExpressionTree;
        this.leftResultSetNumber = leftResultSetNumber;
        this.rightResultSetNumber = rightResultSetNumber;
    }

    /**
     * Satisfy non-guava (derby client) compile dependency.
     * @param predicate the predicate
     * @return Return string representation of Derby Predicate
     */
    public static String toString(Predicate predicate) {
        if (predicate == null) {
            return null;
        }
        ValueNode operand = predicate.getAndNode().getLeftOperand();
        return opToString(operand);
    }

    // Helper method for initializing the spark major version.
    private static SparkVersion getSparkVersion() {

        // If splice.spark.version is manually set, use it...
        String spliceSparkVersionString = System.getProperty(SPLICE_SPARK_VERSION);
        SparkVersion sparkVersion =
            (spliceSparkVersionString != null && !spliceSparkVersionString.isEmpty()) ?
              new SimpleSparkVersion(spliceSparkVersionString) : null;

        // ... otherwise pick up the splice compile-time version of spark.
        if (sparkVersion == null || sparkVersion.isUnknown()) {
            spliceSparkVersionString = System.getProperty(SPLICE_SPARK_COMPILE_VERSION);
            sparkVersion = new SimpleSparkVersion(spliceSparkVersionString);
            if (sparkVersion.isUnknown())
                sparkVersion = CompilerContext.DEFAULT_SPLICE_SPARK_VERSION;
        }
        return sparkVersion;
    }

    /**
     * Satisfy non-guava (derby client) compile dependency.
     * @param predicateList the predicate list
     * @return Return string representation of Derby Predicates in a predicate list
     */
    public static String toString(PredicateList predicateList) {
        if (predicateList == null || predicateList.isEmpty()) {
            return null;
        }
        StringBuilder buf = new StringBuilder();
        for (int i = 0, s = predicateList.size(); i < s; i++) {
            OptimizablePredicate predicate = predicateList.getOptPredicate(i);
            ValueNode operand = ((Predicate)predicate).getAndNode().getLeftOperand();
            buf.append(opToString(operand)).append(", ");
        }
        if (buf.length() > 2) {
            // trim last ", "
            buf.setLength(buf.length() - 2);
        }
        return buf.toString();
    }

    /**
     * Return string representation of a Derby expression
     */
    public static String opToString(ValueNode operand) {
        try {
            OperatorToString vars =
                new OperatorToString(false,
                                     new MutableInt(0),
                                     false, 0, 0);
            return opToString2(operand, vars);
        }
        catch(StandardException e) {
            return "Bad SQL Expression";
        }
    }

    /**
     * Return a spark SQL expression given a Derby SQL expression, with column
     * references indicating column names in the source DataFrame.
     */
    public static String opToSparkString(ValueNode operand) throws StandardException {
        String retval = "";

        // Do not throw any errors encountered.  An error condition
        // just means we can't generate a valid spark representation
        // of the SQL expression to apply to a native spark Dataset,
        // so should not be considered a fatal error.
        try {
            OperatorToString vars =
                new OperatorToString(true,
                                     new MutableInt(0),
                                     false, 0, 0);
            retval = opToString2(operand, vars);

        }
        catch (StandardException e) {
            if (e.getSQLState() != SQLState.LANG_DOES_NOT_IMPLEMENT)
                throw e;
        }
        return retval;
    }

    /**
     * Return a tree representation of a spark SQL expression given a Derby SQL expression,
     * with column references indicating column names in the source DataFrame, and whether
     * the source DataFrame is the left operation or the right operation (e.g. of a join).
     */
    public static SparkExpressionNode
    opToSparkExpressionTree(ValueNode operand,
                            int leftResultSetNumber,
                            int rightResultSetNumber) throws StandardException {
        SparkExpressionNode retval = null;

        // Do not throw any errors encountered.  An error condition
        // just means we can't generate a valid spark representation
        // of the SQL expression to apply to a native spark Dataset,
        // so should not be considered a fatal error.
        try {
            OperatorToString vars =
                new OperatorToString(true,
                                     new MutableInt(0),
                                     true,
                                     leftResultSetNumber,
                                     rightResultSetNumber);
            if (vars.sparkVersion.lessThan(spark_2_2_0))
                return null;
            opToString2(operand, vars);
            retval = vars.sparkExpressionTree;

        }
        catch (StandardException e) {
            if (e.getSQLState() != SQLState.LANG_DOES_NOT_IMPLEMENT)
                throw e;
        }
        return retval;
    }

    private static void throwNotImplementedError() throws StandardException {
        throw StandardException.newException(SQLState.LANG_DOES_NOT_IMPLEMENT);
    }

    // We don't support REAL (float), because the way spark
    // evaluates expressions involving float causes accuracy errors
    // that don't occur when splice does the evaluation.
    private static boolean sparkSupportedType(int typeFormatId, ValueNode operand) {

        return (typeFormatId == BOOLEAN_TYPE_ID  ||
                typeFormatId == DATE_TYPE_ID     ||
                typeFormatId == CHAR_TYPE_ID     ||
                typeFormatId == VARCHAR_TYPE_ID  ||
                typeFormatId == LONGVARCHAR_TYPE_ID  ||
                typeFormatId == TINYINT_TYPE_ID  ||
                typeFormatId == SMALLINT_TYPE_ID ||
                typeFormatId == INT_TYPE_ID      ||
                typeFormatId == LONGINT_TYPE_ID  ||
                typeFormatId == DECIMAL_TYPE_ID  ||
                typeFormatId == DOUBLE_TYPE_ID   ||
                typeFormatId == TIMESTAMP_TYPE_ID);
    }

    private static boolean isNumericTypeFormatID(int typeFormatId) {
        return (typeFormatId == TINYINT_TYPE_ID  ||
                typeFormatId == SMALLINT_TYPE_ID ||
                typeFormatId == INT_TYPE_ID      ||
                typeFormatId == LONGINT_TYPE_ID  ||
                typeFormatId == DECIMAL_TYPE_ID  ||
                typeFormatId == DOUBLE_TYPE_ID   ||
                typeFormatId == REAL_TYPE_ID);
    }

    private static boolean isOverflowSensitive(ValueNode operand) throws StandardException {
        return (operand.getTypeId().getTypeFormatId() == LONGINT_TYPE_ID ||
                operand.getTypeId().getTypeFormatId() == DECIMAL_TYPE_ID ||
                operand.getTypeId().getTypeFormatId() == DOUBLE_TYPE_ID);
    }
    private static void checkOverflowHidingCases(BinaryArithmeticOperatorNode bao,
                                                 MutableInt relationalOpDepth) throws StandardException {
        if (bao.getCompilerContext().getAllowOverflowSensitiveNativeSparkExpressions())
            return;
        if (relationalOpDepth.intValue() <= 0)
            return;
        ValueNode leftOperand = bao.getLeftOperand();
        ValueNode rightOperand = bao.getRightOperand();
        if (isOverflowSensitive(leftOperand) || isOverflowSensitive(rightOperand))
            throwNotImplementedError();
    }

    /**
     * Returns The string representation of a Derby expression tree.
     * 
     * @param  operand           The expression tree to parse and translate to text.
     * @param  vars              It contains the following parameters:
     *      vars.sparkExpression
     *                           True, if converting the expression to spark SQL,
     *                           otherwise false.
     *      vars.sparkVersion
     *                           The spark major version for which we're generating
     *                           the spark SQL expression, if "sparkExpression" is true.
     *      vars.relationalOpDepth
     *                           The current number of relational operators or other
     *                           null-hiding expressions, such as CASE, which we
     *                           are nested within.  Used in determining if a spark
     *                           SQL expression is ANSI SQL compliant.
     * @return true              The SQL representation of the expression tree
     *                           held in "operand".
     * @throws StandardException  
     * @notes  If sparkExpression is true, and the expression tree cannot be
     *         represented in Spark SQL, or the resulting expression would not
     *         behave in an ANSI SQL compliant manner (e.g. would hide numeric
     *         overflows), then a zero length String is returned.
     */
    private static String opToString2(ValueNode        operand,
                                      OperatorToString vars) throws StandardException {
        if(operand==null){
            return "";
        } else if (operand instanceof UntypedNullConstantNode) {
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            return " null ";
        }
        else if(operand instanceof UnaryOperatorNode){
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            UnaryOperatorNode uop=(UnaryOperatorNode)operand;
            String operatorString = uop.getOperatorString();
            if (vars.sparkExpression) {
                if (operand instanceof IsNullNode) {
                    vars.relationalOpDepth.increment();
                    String isNullString = format("%s %s", opToString2(uop.getOperand(), vars), operatorString);
                    vars.relationalOpDepth.decrement();
                    return isNullString;
                }
                else if (operand instanceof GroupingFunctionNode) {
                    GroupingFunctionNode gfn = (GroupingFunctionNode) operand;
                    ValueNode groupingIdRefForSpark = gfn.getGroupingIdRefForSpark();
                    if (groupingIdRefForSpark == null)
                        throwNotImplementedError();
                    else
                        return opToString2(groupingIdRefForSpark, vars);
                }
                else if (operand instanceof ExtractOperatorNode) {
                    ExtractOperatorNode eon = (ExtractOperatorNode) operand;
                    String functionName = eon.sparkFunctionName();

                    // Splice extracts fractional seconds, but spark only extracts whole seconds.
                    // Spark by default counts weeks starting on Sunday.
                    if (functionName.equals("SECOND") || functionName.equals("WEEK")) {
                        throwNotImplementedError();
                    } else if (functionName.equals("WEEKDAY")) {
                        return format("cast(date_format(%s, \"u\") as int) ", opToString2(uop.getOperand(), vars));
                    } else if (functionName.equals("WEEKDAYNAME")) {
                        return format("date_format(%s, \"EEEE\") ", opToString2(uop.getOperand(), vars));
                    } else {
                        return format("%s(%s) ", functionName, opToString2(uop.getOperand(), vars));
                    }
                }
                else if (operand instanceof DB2LengthOperatorNode) {
                    DB2LengthOperatorNode lengthOp = (DB2LengthOperatorNode)operand;
                    String functionName = lengthOp.getOperatorString();
                    ValueNode vn = lengthOp.getOperand();
                    int type = vn.getTypeId().getTypeFormatId();
                    boolean stringType =
                             (type == CHAR_TYPE_ID ||
                              type == VARCHAR_TYPE_ID ||
                              type == LONGVARCHAR_TYPE_ID ||
                              type == CLOB_TYPE_ID);
                    // The length function has the same behavior on splice and
                    // spark only for string types.
                    if (!stringType)
                        throwNotImplementedError();

                    return format("%s(%s) ", functionName, opToString2(lengthOp.getOperand(), vars));
                }
                else if (operand instanceof UnaryArithmeticOperatorNode) {
                    UnaryArithmeticOperatorNode uao = (UnaryArithmeticOperatorNode) operand;
                    if (operatorString.equals("+") || operatorString.equals("-"))
                        return format("%s%s ", operatorString, opToString2(uao.getOperand(), vars));
                    else if (operatorString.equals("ABS/ABSVAL"))
                        operatorString = "abs";
                }
                else if (operand instanceof SimpleStringOperatorNode ||
                         operand instanceof NotNode) {
                    return format("%s(%s) ", operatorString, opToString2(uop.getOperand(), vars));
                }
                else
                    throwNotImplementedError();
            }
            return format("%s(%s)", operatorString, opToString2(uop.getOperand(), vars));
        }else if(operand instanceof BinaryRelationalOperatorNode){
            vars.relationalOpDepth.increment();
            BinaryRelationalOperatorNode bron=(BinaryRelationalOperatorNode)operand;
            try {
                SparkExpressionNode leftExpr, rightExpr, resultExpr = null;
                InListOperatorNode inListOp = bron.getInListOp();

                if (inListOp != null) {
                    if (vars.buildExpressionTree)
                        throwNotImplementedError();
                    return opToString2(inListOp, vars);
                }
                String leftOperandString = opToString2(bron.getLeftOperand(), vars);
                leftExpr = vars.sparkExpressionTree;
                String rightOperandString = opToString2(bron.getRightOperand(), vars);
                rightExpr = vars.sparkExpressionTree;

                // do explict padding for comparison of fixed char types
                if (vars.sparkExpression) {
                    ValueNode leftOperand = bron.getLeftOperand();
                    ValueNode rightOperand = bron.getRightOperand();
                    if (leftOperand.getTypeId() != null && leftOperand.getTypeId().getTypeFormatId() == StoredFormatIds.CHAR_TYPE_ID &&
                        rightOperand.getTypeId() != null && rightOperand.getTypeId().getTypeFormatId() == StoredFormatIds.CHAR_TYPE_ID) {
                        
                        if (leftOperand.getTypeServices() != null && rightOperand.getTypeServices() != null) {
                            int leftWidth = leftOperand.getTypeServices().getMaximumWidth();
                            int rightWidth = rightOperand.getTypeServices().getMaximumWidth();
                            if (leftWidth > rightWidth) {
                                // pad right
                                rightOperandString = format("RPAD(%s, %d, ' ') ",
                                        rightOperandString,
                                        leftWidth);
                                if (vars.buildExpressionTree)
                                    rightExpr = new SparkStringPadOperator(rightExpr, leftWidth, " ", true);
                            } else if (leftWidth < rightWidth) {
                                // pad left
                                leftOperandString = format("RPAD(%s, %d, ' ') ",
                                        leftOperandString,
                                        rightWidth);
                                if (vars.buildExpressionTree)
                                    leftExpr = new SparkStringPadOperator(leftExpr, rightWidth, " ", true);
                            }
                        }
                    }
                }

                String opString =
                        format("(%s %s %s)", leftOperandString,
                               bron.getOperatorString(), rightOperandString);
                vars.relationalOpDepth.decrement();
                if (vars.buildExpressionTree) {
                    resultExpr =
                        new SparkRelationalOperator(bron.getOperator(),
                                                    leftExpr, rightExpr);
                    vars.sparkExpressionTree = resultExpr;
                }
                return opString;
            }
            catch (StandardException e) {
                if (vars.sparkExpression)
                    throw e;
                else
                    return "PARSE_ERROR_WHILE_CONVERTING_OPERATOR";
            }
        }else if(operand instanceof BinaryListOperatorNode){
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            vars.relationalOpDepth.increment();
            boolean isBetween = operand instanceof BetweenOperatorNode;
            BinaryListOperatorNode blon = (BinaryListOperatorNode)operand;
            StringBuilder inList = isBetween ? new StringBuilder() :
                                               new StringBuilder("(");
            if (!blon.isSingleLeftOperand()) {
                ValueNodeList vnl = blon.leftOperandList;
                inList.append("(");
                for (int i = 0; i < vnl.size(); i++) {
                    ValueNode vn = (ValueNode) vnl.elementAt(i);
                    if (i != 0)
                        inList.append(",");
                    inList.append(opToString2(vn, vars));
                }
                inList.append(")");
            }
            else
                inList.append(opToString2(blon.getLeftOperand(), vars));
            inList.append(" ").append(blon.getOperator());
            String appendString = isBetween ? " " : " (";
            inList.append(appendString);
            ValueNodeList rightOperandList=blon.getRightOperandList();
            boolean isFirst = true;
            for(Object qtn: rightOperandList){
                if(isFirst) isFirst = false;
                else if (isBetween)
                    inList = inList.append(" and ");
                else
                    inList = inList.append(",");
                inList = inList.append(opToString2((ValueNode)qtn, vars));
            }
            if (!isBetween)
                inList.append("))");
            String retval = inList.toString();
            vars.relationalOpDepth.decrement();
            return retval;
        }else if (operand instanceof BinaryOperatorNode) {

            BinaryOperatorNode bop = (BinaryOperatorNode) operand;
            ValueNode leftOperand = bop.getLeftOperand();
            ValueNode rightOperand = bop.getRightOperand();
            SparkExpressionNode leftExpr = null, rightExpr = null, resultExpr = null;
            String leftOperandString = opToString2(leftOperand, vars);
            if (vars.buildExpressionTree)
                leftExpr = vars.sparkExpressionTree;
            String rightOperandString = opToString2(rightOperand, vars);
            if (vars.buildExpressionTree)
                rightExpr = vars.sparkExpressionTree;

            if (vars.sparkExpression) {

                if (operand instanceof ConcatenationOperatorNode) {
                    if (vars.buildExpressionTree)
                        throwNotImplementedError();
                    return format("concat(%s, %s) ", leftOperandString,
                                                     rightOperandString);
                }
                else if (operand instanceof TruncateOperatorNode) {
                    if (vars.buildExpressionTree)
                        throwNotImplementedError();
                    if (leftOperand.getTypeId().getTypeFormatId() == DATE_TYPE_ID) {
                        return format("trunc(%s, %s) ", leftOperandString,
                                                        rightOperandString);
                    }
                    else if (vars.sparkVersion.greaterThanOrEqualTo(spark_2_3_0) &&
                               leftOperand.getTypeId().getTypeFormatId() == TIMESTAMP_TYPE_ID) {
                        return format("date_trunc(%s, %s) ", leftOperandString,
                                                             rightOperandString);
                    } else
                        throwNotImplementedError();
                }
                else if (operand instanceof BinaryArithmeticOperatorNode) {
                    BinaryArithmeticOperatorNode bao = (BinaryArithmeticOperatorNode)operand;

                    // The way spark converts real to double causes
                    // inaccurate results, so avoid native spark data sets
                    // for these cases.
                    if ((leftOperand.getTypeId().getTypeFormatId() == REAL_TYPE_ID ||
                        rightOperand.getTypeId().getTypeFormatId() == REAL_TYPE_ID) &&
                        !operand.getCompilerContext().
                         getAllowOverflowSensitiveNativeSparkExpressions())
                        throwNotImplementedError();

                    // Spark may hide overflow errors by generating +Infinity, -Infinity
                    // or null, and any predicate containing these values may appear to
                    // evaluate successfully, but really the query should error out.
                    checkOverflowHidingCases(bao, vars.relationalOpDepth);

                    // Splice automatically builds a binary arithmetic expression
                    // in the requested final data type.  For spark, we need to
                    // provide an explicit CAST to get the same effect.
                    boolean doCast = false;
                    String targetType = null;

                    if (leftOperand.getTypeId().getTypeFormatId() !=
                        bao.getTypeId().getTypeFormatId() &&
                        rightOperand.getTypeId().getTypeFormatId() !=
                        bao.getTypeId().getTypeFormatId()) {
                        // if date difference or date subtraction operation, the input parameter and result types are meant to be different */
                        if (!(bao.getOperatorString() == "-" &&
                                leftOperand.getTypeId().getTypeFormatId() == DATE_TYPE_ID)) {
                            doCast = true;
                            targetType = bao.getTypeServices().toSparkString();
                        }
                    }
                    if (doCast) {
                        if (leftOperand.getTypeServices().getTypeId().typePrecedence() >
                            rightOperand.getTypeServices().getTypeId().typePrecedence()) {
                            leftOperandString = format("CAST(%s as %s) ",
                                                       leftOperandString,
                                                       targetType);
                            if (vars.buildExpressionTree)
                                leftExpr = new SparkCastNode(leftExpr, targetType);
                        }
                        else {
                            rightOperandString = format("CAST(%s as %s) ",
                                                         rightOperandString,
                                                         targetType);
                            if (vars.buildExpressionTree)
                                rightExpr = new SparkCastNode(rightExpr, targetType);
                        }
                    }
                    if (vars.buildExpressionTree) {
                        resultExpr =
                           new SparkArithmeticOperator
                                (bao.getOperatorString(), leftExpr, rightExpr);
                        vars.sparkExpressionTree = resultExpr;
                    }

                    // Though documented as supported by spark, mod
                    // is not recognized.  Disable for now.
                    // Division by zero results in a null value on
                    // spark, but splice expects this to error out,
                    // so we can't use native spark sql for "/".
                    if (bao.getOperatorString() == "mod")
                        throwNotImplementedError();
                    else if (bao.getOperatorString() == "/" &&
                             !bao.getCompilerContext().
                              getAllowOverflowSensitiveNativeSparkExpressions())
                        throwNotImplementedError();
                    else if (bao.getOperatorString() == "+") {
                        if (leftOperand.getTypeId().getTypeFormatId() == DATE_TYPE_ID) {
                            if (vars.buildExpressionTree)
                                throwNotImplementedError();
                            return format("date_add(%s, %s) ", leftOperandString, rightOperandString );
                        }
                        else if (leftOperand.getTypeId().getTypeFormatId() == TIMESTAMP_TYPE_ID)
                            throwNotImplementedError();
                    }
                    else if (bao.getOperatorString() == "-") {
                        if (leftOperand.getTypeId().getTypeFormatId() == DATE_TYPE_ID) {
                            if (vars.buildExpressionTree)
                                throwNotImplementedError();
                            /* use datediff if both operands are of date type */
                            if (rightOperand.getTypeId().getTypeFormatId() == DATE_TYPE_ID)
                                return format("datediff(%s, %s) ", leftOperandString, rightOperandString);
                            else
                                return format("date_sub(%s, %s) ", leftOperandString, rightOperandString);
                        }
                        else if (leftOperand.getTypeId().getTypeFormatId() == TIMESTAMP_TYPE_ID)
                            throwNotImplementedError();
                    }
                }
                else if (operand.getClass() == BinaryOperatorNode.class) {
                    if (vars.buildExpressionTree)
                        throwNotImplementedError();
                    if (((BinaryOperatorNode) operand).isRepeat()) {
                        return format("%s(%s, %s) ", bop.getOperatorString(),
                          opToString2(bop.getLeftOperand(), vars), opToString2(bop.getRightOperand(), vars));
                    }
                }
                else if (operand instanceof TimestampOperatorNode ||
                         operand instanceof SimpleLocaleStringOperatorNode)
                    throwNotImplementedError();
                else if (vars.buildExpressionTree && operand instanceof BinaryLogicalOperatorNode) {
                    BinaryLogicalOperatorNode blon = (BinaryLogicalOperatorNode) operand;
                    resultExpr =
                        SparkLogicalOperator.getNewSparkLogicalOperator
                            (blon.getOperatorType(), leftExpr, rightExpr);
                    vars.sparkExpressionTree = resultExpr;
                }
                else if (vars.buildExpressionTree)
                    throwNotImplementedError();
            }

            // Need to CAST if the final type is decimal because the precision
            // or scale used by spark to hold the result may not match what
            // splice has chosen, and could cause an overflow.
            //
            // DB-9333
            // Also need a final CAST for division operator because Spark may
            // decide to use a different result type. It happens when at least
            // one operand of the division is an aggregate function. Plus, minus,
            // and multiplication of aggregate functions are fine.
            boolean doCast = operand instanceof BinaryArithmeticOperatorNode &&
                             (operand.getTypeId().getTypeFormatId() == DECIMAL_TYPE_ID ||
                              ((BinaryArithmeticOperatorNode)operand).getOperatorString().equals("/")) &&
                             vars.sparkExpression;
            String expressionString =
                    format("(%s %s %s)", leftOperandString,
                                         bop.getOperatorString(), rightOperandString);
            if (doCast) {
                // Spark generates a null value on decimal overflow instead of erroring out,
                // (see SPARK-23179), so until an option is provided to catch
                // the overflow, we have to avoid spark-native evaluation of operations
                // which could hide the overflow.
                if (!operand.getCompilerContext().getAllowOverflowSensitiveNativeSparkExpressions())
                    throwNotImplementedError();

                expressionString = format("CAST(%s as %s) ",
                                           expressionString,
                                           operand.getTypeServices().toSparkString());
                if (vars.buildExpressionTree)
                    vars.sparkExpressionTree =
                        new SparkCastNode(vars.sparkExpressionTree,
                                          operand.getTypeServices().toSparkString());
            }
            return expressionString;
        } else if (operand instanceof ArrayOperatorNode) {
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            if (vars.sparkExpression)
                throwNotImplementedError();
            ArrayOperatorNode array = (ArrayOperatorNode) operand;
            ValueNode op = array.operand;
            return format("%s[%d]", op == null ? "" : opToString2(op, vars), array.extractField);
        } else if (operand instanceof TernaryOperatorNode) {
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            TernaryOperatorNode top = (TernaryOperatorNode) operand;
            ValueNode rightOp = top.getRightOperand();
            if (vars.sparkExpression) {
                if (operand instanceof LikeEscapeOperatorNode) {
                    vars.relationalOpDepth.increment();
                    if (rightOp != null)
                        throwNotImplementedError();
                    else {
                        String likeString =  format("(%s %s %s) ", opToString2(top.getReceiver(), vars), top.getOperator(),
                        opToString2(top.getLeftOperand(), vars));
                        vars.relationalOpDepth.decrement();
                        return likeString;
                    }
                }
                else if (operand.getClass() == TernaryOperatorNode.class) {
                    vars.relationalOpDepth.increment();
                    if (top.getOperator().equals("LOCATE") ||
                        top.getOperator().equals("replace") ||
                        (top.getOperator().equals("substring") && top.getRightOperand() != null)) {

                        vars.relationalOpDepth.decrement();
                        String retval = format("%s(%s, %s, %s) ", top.getOperator(), opToString2(top.getReceiver(), vars),
                                opToString2(top.getLeftOperand(), vars), opToString2(top.getRightOperand(), vars));
                        vars.relationalOpDepth.decrement();
                        return retval;
                    } else if (top.getOperator().equals("substring")) {
                        assert top.getRightOperand() == null;
                        vars.relationalOpDepth.decrement();
                        String retval = format("%s(%s, %s) ", top.getOperator(), opToString2(top.getReceiver(), vars),
                                opToString2(top.getLeftOperand(), vars));
                        vars.relationalOpDepth.decrement();
                        return retval;

                    } else if (top.getOperator().equals("trim")) {
                        // Trim is supported starting at Spark 2.3.
                        if (vars.sparkVersion.lessThan(spark_2_3_0))
                            throwNotImplementedError();
                        String retval;
                        if (top.isLeading())
                            retval = format("%s(LEADING %s FROM %s) ",  top.getOperator(), opToString2(top.getLeftOperand(), vars),
                                opToString2(top.getReceiver(), vars));
                        else if (top.isTrailing())
                            retval = format("%s(TRAILING %s FROM %s) ",  top.getOperator(), opToString2(top.getLeftOperand(), vars),
                                opToString2(top.getReceiver(), vars));
                        else
                            retval = format("%s(BOTH %s FROM %s) ",  top.getOperator(), opToString2(top.getLeftOperand(), vars),
                                opToString2(top.getReceiver(), vars));

                        vars.relationalOpDepth.decrement();
                        return retval;
                    }
                    else
                        throwNotImplementedError();
                }
                else
                    throwNotImplementedError();
            }
            return format("%s(%s, %s%s) ", top.getOperator(), opToString2(top.getReceiver(), vars),
                          opToString2(top.getLeftOperand(), vars), rightOp == null ? "" : ", " + opToString2(rightOp, vars));
        }
        else if (operand instanceof ArrayConstantNode) {
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            vars.relationalOpDepth.increment();
            if (vars.sparkExpression)
                throwNotImplementedError();;
            ArrayConstantNode arrayConstantNode = (ArrayConstantNode) operand;
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            int i = 0;
            for (Object object: arrayConstantNode.argumentsList) {
                if (i!=0)
                    builder.append(",");
                builder.append(opToString2((ValueNode)object, vars));
                i++;
            }
            builder.append("]");
            vars.relationalOpDepth.decrement();
            return builder.toString();
        } else if (operand instanceof ListValueNode) {
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            vars.relationalOpDepth.increment();
            if (vars.sparkExpression)
                throwNotImplementedError();;
            ListValueNode lcn = (ListValueNode) operand;
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            for (int i = 0; i < lcn.numValues(); i++) {
                ValueNode vn = lcn.getValue(i);
                if (i != 0)
                    builder.append(",");
                builder.append(opToString2(vn, vars));
            }
            builder.append(")");
            vars.relationalOpDepth.decrement();
            return builder.toString();
        }
        else if (operand instanceof ColumnReference) {
            ColumnReference cr = (ColumnReference) operand;
            String table = cr.getTableName();
            ResultColumn source = cr.getSource();
            if (! vars.sparkExpression) {
                return format("%s%s%s", table == null ? "" : format("%s.", table),
                cr.getColumnName(), source == null ? "" :
                format("[%s:%s]", source.getResultSetNumber(), source.getVirtualColumnId()));
            }
            else {
                if (!sparkSupportedType(cr.getTypeId().getTypeFormatId(), operand))
                    throwNotImplementedError();

                String columnString = format("c%d", source.getVirtualColumnId()-1);
                if (vars.buildExpressionTree) {
                    boolean leftDataFrame;
                    int rsn =  source.getResultSetNumber();
                    if (rsn != vars.leftResultSetNumber &&
                        rsn != vars.rightResultSetNumber)
                        opToString2(source.getExpression(), vars);
                    else {
                        leftDataFrame = (rsn == vars.leftResultSetNumber);
                        vars.sparkExpressionTree =
                        new SparkColumnReference(columnString, leftDataFrame);
                    }
                }
                return columnString;
            }
        } else if (operand instanceof VirtualColumnNode) {
            VirtualColumnNode vcn = (VirtualColumnNode) operand;
            ResultColumn source = vcn.getSourceColumn();
            String table = source.getTableName();
            if (! vars.sparkExpression) {
                return format("%s%s%s", table == null ? "" : format("%s.", table),
                source.getName(),
                format("[%s:%s]", source.getResultSetNumber(), source.getVirtualColumnId()));
            }
            else {
                if (!sparkSupportedType(operand.getTypeId().getTypeFormatId(), operand))
                    throwNotImplementedError();

                String columnString = format("c%d", source.getVirtualColumnId()-1);
                if (vars.buildExpressionTree) {
                    boolean leftDataFrame;
                    int rsn =  source.getResultSetNumber();
                    if (rsn != vars.leftResultSetNumber &&
                        rsn != vars.rightResultSetNumber)
                        opToString2(source.getExpression(), vars);
                    else {
                        leftDataFrame = (rsn == vars.leftResultSetNumber);
                        vars.sparkExpressionTree =
                        new SparkColumnReference(columnString, leftDataFrame);
                    }
                }
                return columnString;
            }
        } else if (operand instanceof SubqueryNode) {
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            if (vars.sparkExpression)
                throwNotImplementedError();
            SubqueryNode subq = (SubqueryNode) operand;
            return format("subq=%s", subq.getResultSet().getResultSetNumber());
        } else if (operand instanceof ConstantNode) {
            ConstantNode cn = (ConstantNode) operand;
            try {
                DataValueDescriptor dvd = cn.getValue();
                String str = null;
                if (dvd == null) {
                    if (cn instanceof NumericConstantNode)
                        str = format("CAST(null as %s) ", cn.getTypeServices().toSparkString());
                    else
                        str = "null ";
                }
                else if (vars.sparkExpression) {
                    if (dvd instanceof SQLChar ||
                        dvd instanceof SQLVarchar ||
                        dvd instanceof SQLLongvarchar ||
                        dvd instanceof SQLClob) {
                        if (vars.buildExpressionTree)
                            str = format("%s", cn.getValue().getString());
                        else
                            str = format("\'%s\' ", cn.getValue().getString());                 
                    }
                    else if (dvd instanceof SQLDate)
                        str = format("date(\'%s\') ", cn.getValue().getString());
                    else if (dvd instanceof SQLTimestamp)
                        str = format("timestamp(\'%s\') ", cn.getValue().getString());
                    else if (dvd instanceof SQLDouble)
                        str = format("double(\'%s\') ", cn.getValue().getString());
                    else if (dvd instanceof SQLInteger  ||
                             dvd instanceof SQLDecimal  ||
                             dvd instanceof SQLBoolean)
                        str = cn.getValue().getString();
                    else if (dvd instanceof SQLLongint  ||
                             dvd instanceof SQLSmallint ||
                             dvd instanceof SQLTinyint)
                        str = format("CAST(%s as %s) ",
                                      cn.getValue().getString(),
                                      cn.getTypeServices().toSparkString());
                    else
                        throwNotImplementedError();
                }
                else
                    str = cn.getValue().getString();

                if (vars.buildExpressionTree) {
                    SpecialValue sv = SpecialValue.NONE;
                    if (dvd == null)
                        sv = SpecialValue.NULL;
                    else if (cn instanceof BooleanConstantNode) {
                        BooleanConstantNode bcn = (BooleanConstantNode) cn;
                        if (bcn.isBooleanTrue())
                            sv = SpecialValue.TRUE;
                        else
                            sv = SpecialValue.FALSE;
                    }
                    vars.sparkExpressionTree =
                        new SparkConstantExpression(str, sv,
                                                    cn.getTypeServices().
                                                       toSparkString());
                }
                return str;
            } catch (StandardException se) {
                if (vars.sparkExpression)
                    throw(se);
                else
                    return se.getMessage();
            }
        } else if(operand instanceof CastNode){
            String castString = null;
            if (vars.sparkExpression) {
                StringBuilder sb = new StringBuilder();
                CastNode cn = (CastNode)operand;
                ValueNode castOperand = cn.getCastOperand();
                int typeFormatId = operand.getTypeId().getTypeFormatId();
                if (!sparkSupportedType(typeFormatId, operand))
                    throwNotImplementedError();

                sb.append(format("CAST(%s ", opToString2(castOperand, vars)));
                SparkExpressionNode childExpression = vars.sparkExpressionTree;
                String dataTypeString = null;

                if (typeFormatId == LONGVARCHAR_TYPE_ID)
                    dataTypeString = "varchar(32670)";
                else if (isNumericTypeFormatID(typeFormatId) &&
                         typeFormatId != DOUBLE_TYPE_ID      &&
                         !operand.getCompilerContext().
                          getAllowOverflowSensitiveNativeSparkExpressions() &&
                          (! (castOperand instanceof ColumnReference) ||
                             operand.getTypeId().typePrecedence() <
                             castOperand.getTypeId().typePrecedence() )) {
                    // Disallow manual cast to a numeric type
                    // for possible problematic cases.
                    // Decimal overflow on spark returns null
                    // instead of throwing an error.
                    // CASTing to other numerics can truncate
                    // higher order bits and return incorrect
                    // results instead of throwing an overflow
                    // error.
                    throwNotImplementedError();
                }
                else
                    dataTypeString = cn.getTypeServices().toSparkString();

                if (vars.buildExpressionTree) {
                    // Spark doesn't handle casting char to numeric very well.
                    if (isNumericTypeFormatID(typeFormatId) &&
                        !isNumericTypeFormatID(castOperand.getTypeId().getTypeFormatId()))
                        throwNotImplementedError();
                    vars.sparkExpressionTree = new SparkCastNode(childExpression, dataTypeString);
                }
                sb.append(format("AS %s) ", dataTypeString));
                castString = sb.toString();
            }
            else
                castString = opToString2(((CastNode)operand).getCastOperand(), vars);

            return castString;
        }
        else if (operand instanceof CoalesceFunctionNode) {
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            vars.relationalOpDepth.increment();
            StringBuilder sb = new StringBuilder();
            sb.append("coalesce(");
            int i = 0;
            for (Object ob : ((CoalesceFunctionNode) operand).argumentsList) {
                ValueNode vn = (ValueNode)ob;
                if (i > 0)
                    sb.append(", ");
                sb.append(format("%s", opToString2(vn, vars)));
                i++;
            }
            sb.append(") ");
            vars.relationalOpDepth.decrement();
            return sb.toString();
        }
        else if (operand instanceof CurrentDatetimeOperatorNode) {
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            CurrentDatetimeOperatorNode cdtOp = (CurrentDatetimeOperatorNode)operand;
            StringBuilder sb = new StringBuilder();
            if (cdtOp.isCurrentDate())
                sb.append("current_date");
            else if (cdtOp.isCurrentTime()) {
                if (vars.sparkExpression)
                    throwNotImplementedError();
                sb.append("current_time");
            }
            else if (cdtOp.isCurrentTimestamp())
                sb.append("current_timestamp");
            else
                throwNotImplementedError();
            if (vars.sparkExpression)
                sb.append("() ");

            return sb.toString();
        }
        else if (operand instanceof ConditionalNode) {
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            vars.relationalOpDepth.increment();
            ConditionalNode cn = (ConditionalNode)operand;
            StringBuilder sb = new StringBuilder();
            sb.append(format ("CASE WHEN %s ", opToString2(cn.getTestCondition(), vars)));
            int i = 0;
            for (Object ob : cn.getThenElseList()) {
                ValueNode vn = (ValueNode)ob;
                if (i == 0)
                    sb.append(format("THEN %s ", opToString2(vn, vars)));
                else
                    sb.append(format("ELSE %s ", opToString2(vn, vars)));
                i++;
            }
            sb.append("END ");
            vars.relationalOpDepth.decrement();
            return sb.toString();
        }
        else {
            if (vars.buildExpressionTree)
                throwNotImplementedError();
            if (vars.sparkExpression) {
                if (operand instanceof JavaToSQLValueNode &&
                ((JavaToSQLValueNode) operand).isSystemFunction()) {
                    vars.relationalOpDepth.increment();
                    JavaToSQLValueNode javaFun = (JavaToSQLValueNode) operand;
                    JavaValueNode method = javaFun.getJavaValueNode();

                    if (method instanceof StaticMethodCallNode) {
                        StaticMethodCallNode smc = (StaticMethodCallNode) method;
                        StringBuilder sb = new StringBuilder();
                        String methodName = smc.getMethodName();
                        boolean needsExtraClosingParens = false;

                        // Spark MONTHS_BETWEEN calculates fractional
                        // months, splice MONTH_BETWEEN does not.
                        // Splice and spark use different rounding rules
                        // for the ROUND function.
                        // ADD_MONTHS returns incorrect results on
                        // Spark for old dates.
                        // The ROUND function came from Splice package (uppercase) and java.lang.StrictMath (lowercase),
                        // we need to handle it especially.
                        if (methodName.equals("MONTH_BETWEEN") ||
                            methodName.equals("REGEXP_LIKE")   ||
                            methodName.equals("ADD_MONTHS")    ||
                            methodName.equals("ADD_YEARS")     ||
                            methodName.equals("ADD_DAYS")      ||
                            methodName.equalsIgnoreCase("ROUND"))
                            throwNotImplementedError();
                        else if (methodName.equals("toDegrees"))
                            methodName = "degrees";
                        else if (methodName.equals("toRadians"))
                            methodName = "radians";
                        else if (methodName.equals("SIGN")) {
                            methodName = "int(sign";
                            needsExtraClosingParens = true;
                        }
                        else if (methodName.equals("floor")) {
                            methodName = "double(floor";
                            needsExtraClosingParens = true;
                        }
                        else if (methodName.equals("RAND")) {
                            JavaValueNode param = smc.getMethodParms()[0];
                            if (!(param instanceof SQLToJavaValueNode))
                                throwNotImplementedError();
                            if (! (((SQLToJavaValueNode) param).getSQLValueNode() instanceof ConstantNode))
                                throwNotImplementedError();
                        }
                        else if (methodName.equals("random")) {
                            methodName = "rand";
                        }
                        else if (methodName.equals("ceil")) {
                            methodName = "double(ceil";
                            needsExtraClosingParens = true;
                        }
                        sb.append(format("%s(", methodName));
                        int i = 0;
                        for (JavaValueNode param : smc.getMethodParms()) {
                            if (!(param instanceof SQLToJavaValueNode))
                                throwNotImplementedError();
                            ValueNode vn = ((SQLToJavaValueNode) param).getSQLValueNode();
                            if (i > 0)
                                sb.append(", ");
                            sb.append(opToString2(vn, vars));
                            i++;
                        }
                        if (needsExtraClosingParens)
                            sb.append(")");
                        sb.append(") ");
                        vars.relationalOpDepth.decrement();
                        return sb.toString();
                    }
                    throwNotImplementedError();
                }
                else
                    throwNotImplementedError();
            }
            return replace(operand.toString(), "\n", " ");
        }
    }


    private static String replace(String text, String searchString, String replacement) {
        return replace(text, searchString, replacement, -1);
    }

    private static String replace(String text, String searchString, String replacement, int max) {
        if (text.isEmpty() || searchString.isEmpty() || replacement == null || max == 0) {
            return text;
        }
        int start = 0;
        int end = text.indexOf(searchString, start);
        if (end == -1) {
            return text;
        }
        int replLength = searchString.length();
        int increase = replacement.length() - replLength;
        increase = (increase < 0 ? 0 : increase);
        increase *= (max < 0 ? 16 : (max > 64 ? 64 : max));
        StringBuilder buf = new StringBuilder(text.length() + increase);
        while (end != -1) {
            buf.append(text.substring(start, end)).append(replacement);
            start = end + replLength;
            if (--max == 0) {
                break;
            }
            end = text.indexOf(searchString, start);
        }
        buf.append(text.substring(start));
        return buf.toString();
    }

}
