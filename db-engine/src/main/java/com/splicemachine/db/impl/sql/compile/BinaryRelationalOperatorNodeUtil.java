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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;


import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.types.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Given a BinaryRelationalOperatorNode with a ColumnReferenceOperand and a NumericConstantNode operand
 * make the NumericConstantNode have the same type as the column reference.  If the numeric constant does not
 * fit in the column's type, then change the value of the constant to Type.MAX_VALUE and adjust the relational
 * operator node's operation accordingly (to always evaluate to TRUE or FALSE as appropriate).
 */
class BinaryRelationalOperatorNodeUtil {

    private static final DataValueDescriptor ZERO_BIG_DECIMAL = new SQLDecimal(BigDecimal.ZERO);

    public static void coerceDataTypeIfNecessary(BinaryRelationalOperatorNode operatorNode) {
        try {
            ValueNode leftOperand = operatorNode.getLeftOperand();
            ValueNode rightOperand = operatorNode.getRightOperand();

            if (leftOperand instanceof ColumnReference && rightOperand instanceof NumericConstantNode) {
                coerceDataTypeIfNecessary(operatorNode, (ColumnReference) leftOperand, (NumericConstantNode) rightOperand, ColumnSide.LEFT);
            } else if (rightOperand instanceof ColumnReference && leftOperand instanceof NumericConstantNode) {
                coerceDataTypeIfNecessary(operatorNode, (ColumnReference) rightOperand, (NumericConstantNode) leftOperand, ColumnSide.RIGHT);
            }
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    private static void coerceDataTypeIfNecessary(
            BinaryRelationalOperatorNode operatorNode,
            ColumnReference columnReferenceNode,
            NumericConstantNode numericConstantNode,
            ColumnSide columnSide) throws StandardException {

        DataTypeDescriptor columnType = columnReferenceNode.getTypeServices();
        TypeId columnTypeId = columnType.getTypeId();

        if (columnTypeId.equals(numericConstantNode.getTypeId()) && !columnTypeId.isDecimalTypeId()) {
            /* Types match, nothing to do */
            return;
        }

        DataValueDescriptor colReferValueDescriptor = columnType.getNull();
        DataValueDescriptor constantValueDescriptor = numericConstantNode.getValue();

        /* We ALWAYS change the type of the numeric constant node */
        int typeFormatId = colReferValueDescriptor.getTypeFormatId();
        int newTargetNodeType = getCorrectNodeType(typeFormatId, numericConstantNode.getNodeType());
        numericConstantNode.setNodeType(newTargetNodeType);
        numericConstantNode.setType(columnType);

        /* We have a constant value that FITS in the column type. */
        if (doesFit(columnType, colReferValueDescriptor, constantValueDescriptor)) {
            colReferValueDescriptor.setValue(constantValueDescriptor);
            numericConstantNode.setValue(colReferValueDescriptor);
        }
        /* We have a constant value bigger than the column. */
        else {
            updateOperator(operatorNode, columnType, colReferValueDescriptor, numericConstantNode, columnSide);
        }
    }

    /**
     * Does the *VALUE* of the numeric constant fit within the *TYPE* of the column reference?
     */
    private static boolean doesFit(DataTypeDescriptor columnType,
                                   DataValueDescriptor colReferValueDescriptor,
                                   DataValueDescriptor constValueDescriptor) throws StandardException {

        /* If the col ref type is larger, and it is not decimal, return true */
        if (!columnType.getTypeId().isDecimalTypeId() &&
                colReferValueDescriptor.getLength() >= constValueDescriptor.getLength()) {
            return true;
        }
        /* Otherwise only if the constant is <= the column's max type. AND >= the column's min type */
        DataValueDescriptor maxValueForColumn = getMaxValueForType(columnType, colReferValueDescriptor);
        DataValueDescriptor minValueForColumn = getMinValueForType(columnType, colReferValueDescriptor);

        if (columnType.getTypeId().isDecimalTypeId()) {
            /* If the col is decimal, use it to compare with constant value*/
            return colReferValueDescriptor.lessOrEquals(constValueDescriptor, maxValueForColumn).getBoolean() &&
                    colReferValueDescriptor.greaterOrEquals(constValueDescriptor, minValueForColumn).getBoolean();
        }
        else {
            /* In this case, col is not decimal. If they are numeric types, type of const is larger. Use it for
               comparison. Otherwise, col and const must be the same type.
             */
            return constValueDescriptor.lessOrEquals(constValueDescriptor, maxValueForColumn).getBoolean() &&
                    constValueDescriptor.greaterOrEquals(constValueDescriptor, minValueForColumn).getBoolean();
        }
    }

    /**
     * When mutating the operator there are really four cases. Here I illustrate the cases using a column
     * reference "column" and a positive numeric constant "L" which we have determined to be too large for
     * the column's type.
     *
     * CASE 1: expressions that always evaluate to TRUE
     *
     * column !=   L  ===> column <= Type.MAX_VALUE
     * column !=  -L  ===> column <= Type.MAX_VALUE
     *
     * column <    L  ===> column <= Type.MAX_VALUE
     * column <=   L  ===> column <= Type.MAX_VALUE
     *
     * column >   -L  ===> column <= Type.MAX_VALUE
     * column >=  -L  ===> column <= Type.MAX_VALUE
     *
     * CASE 2: expressions that always evaluate to FALSE
     *
     * column =   L  ===>  column > Type.MAX_VALUE
     * column =  -L  ===>  column > Type.MAX_VALUE
     *
     * column >   L  ===>  column > Type.MAX_VALUE
     * column >=  L  ===>  column > Type.MAX_VALUE
     *
     * column <  -L  ===>  column > Type.MAX_VALUE
     * column <= -L  ===>  column > Type.MAX_VALUE
     *
     * CASE 3: expressions that always evaluate to TRUE
     *
     * L != column  ===> column <= Type.MAX_VALUE
     * -L != column  ===> column <= Type.MAX_VALUE
     *
     * L >  column  ===>  column > Type.MAX_VALUE
     * L >= column  ===>  column > Type.MAX_VALUE
     *
     * -L <  column  ===>  column > Type.MAX_VALUE
     * -L <= column  ===>  column > Type.MAX_VALUE
     *
     * CASE 4: expressions that always evaluate to FALSE
     *
     * L =  column  ===>  column > Type.MAX_VALUE
     * -L =  column  ===>  column > Type.MAX_VALUE
     *
     * L <  column  ===> column <= Type.MAX_VALUE
     * L <= column  ===> column <= Type.MAX_VALUE
     *
     * -L >  column  ===> column <= Type.MAX_VALUE
     * -L >= column  ===> column <= Type.MAX_VALUE
     */
    private static void updateOperator(BinaryRelationalOperatorNode operatorNode,
                                       DataTypeDescriptor columnType,
                                       DataValueDescriptor colReferValueDescriptor,
                                       NumericConstantNode numericConstantNode,
                                       ColumnSide columnSide) throws StandardException {

        boolean isPositiveConstant = ZERO_BIG_DECIMAL.compare(numericConstantNode.getValue()) < 0;
        int currentOperator = operatorNode.getNodeType();

        boolean expectedResult;
        if (columnSide == ColumnSide.LEFT) {
            expectedResult = (isPositiveConstant && OP_SET_POSITIVE.contains(currentOperator)) ||
                    (!isPositiveConstant && OP_SET_NEGATIVE.contains(currentOperator));
        } else {
            expectedResult = (isPositiveConstant && OP_SET_NEGATIVE.contains(currentOperator)) ||
                    (!isPositiveConstant && OP_SET_POSITIVE.contains(currentOperator));
        }

        int newOperator;
        if (expectedResult) {
            if (columnSide == ColumnSide.LEFT) {
                newOperator = C_NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE;
            } else {
                newOperator = C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE;
            }
        } else {
            if (columnSide == ColumnSide.LEFT) {
                newOperator = C_NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE;
            } else {
                newOperator = C_NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE;
            }
        }

        operatorNode.reInitWithNodeType(newOperator);
        numericConstantNode.setValue(getMaxValueForType(columnType, colReferValueDescriptor));
    }

    private static final List<Integer> OP_SET_POSITIVE = Arrays.asList(
            C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE,
            C_NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE,
            C_NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE
    );
    private static final List<Integer> OP_SET_NEGATIVE = Arrays.asList(
            C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE,
            C_NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE,
            C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE
    );

    private static DataValueDescriptor getMinValueForType(DataTypeDescriptor columnType,
                                                          DataValueDescriptor colReferValueDescriptor) throws StandardException {
        switch (colReferValueDescriptor.getTypeFormatId()) {
            case StoredFormatIds.SQL_TINYINT_ID:
                return new SQLTinyint(Byte.MIN_VALUE);
            case StoredFormatIds.SQL_SMALLINT_ID:
                return new SQLSmallint(Short.MIN_VALUE);
            case StoredFormatIds.SQL_INTEGER_ID:
                return new SQLInteger(Integer.MIN_VALUE);
            case StoredFormatIds.SQL_LONGINT_ID:
                return new SQLLongint(Long.MIN_VALUE);
            case StoredFormatIds.SQL_REAL_ID:
                return new SQLReal(Limits.DB2_SMALLEST_REAL);
            case StoredFormatIds.SQL_DOUBLE_ID:
                return new SQLDouble(Limits.DB2_SMALLEST_DOUBLE);
            case StoredFormatIds.SQL_DECIMAL_ID:
                return new SQLDecimal(getMaxBigDecimal(columnType.getPrecision(), columnType.getScale()).negate());
            default:
                throw new IllegalArgumentException();
        }
    }

    private static DataValueDescriptor getMaxValueForType(DataTypeDescriptor columnType,
                                                          DataValueDescriptor colReferValueDescriptor) throws StandardException {
        switch (colReferValueDescriptor.getTypeFormatId()) {
            case StoredFormatIds.SQL_TINYINT_ID:
                return new SQLTinyint(Byte.MAX_VALUE);
            case StoredFormatIds.SQL_SMALLINT_ID:
                return new SQLSmallint(Short.MAX_VALUE);
            case StoredFormatIds.SQL_INTEGER_ID:
                return new SQLInteger(Integer.MAX_VALUE);
            case StoredFormatIds.SQL_LONGINT_ID:
                return new SQLLongint(Long.MAX_VALUE);
            case StoredFormatIds.SQL_REAL_ID:
                return new SQLReal(Limits.DB2_LARGEST_REAL);
            case StoredFormatIds.SQL_DOUBLE_ID:
                return new SQLDouble(Limits.DB2_LARGEST_DOUBLE);
            case StoredFormatIds.SQL_DECIMAL_ID:
                return new SQLDecimal(getMaxBigDecimal(columnType.getPrecision(), columnType.getScale()));
            default:
                throw new IllegalArgumentException();
        }
    }

    private static BigDecimal getMaxBigDecimal(int precision, int scale) {
        BigDecimal fraction = BigDecimal.ONE.subtract(BigDecimal.ONE.divide(BigDecimal.TEN.pow(scale)));
        BigDecimal whole = BigDecimal.TEN.pow(precision - scale).subtract(BigDecimal.ONE);
        return whole.add(fraction);
    }

    private static int getCorrectNodeType(int typeFormatId, int originalNodeType) {
        switch (typeFormatId) {
            case StoredFormatIds.SQL_TINYINT_ID:
                return C_NodeTypes.TINYINT_CONSTANT_NODE;
            case StoredFormatIds.SQL_SMALLINT_ID:
                return C_NodeTypes.SMALLINT_CONSTANT_NODE;
            case StoredFormatIds.SQL_INTEGER_ID:
                return C_NodeTypes.INT_CONSTANT_NODE;
            case StoredFormatIds.SQL_LONGINT_ID:
                return C_NodeTypes.LONGINT_CONSTANT_NODE;
            case StoredFormatIds.SQL_REAL_ID:
                return C_NodeTypes.FLOAT_CONSTANT_NODE;
            case StoredFormatIds.SQL_DOUBLE_ID:
                return C_NodeTypes.DOUBLE_CONSTANT_NODE;
            case StoredFormatIds.SQL_DECIMAL_ID:
                return C_NodeTypes.DECIMAL_CONSTANT_NODE;
            default:
                return originalNodeType;
        }
    }

    /**
     * Indicates which side the column reference is on
     */
    private enum ColumnSide {
        LEFT,
        RIGHT
    }

}