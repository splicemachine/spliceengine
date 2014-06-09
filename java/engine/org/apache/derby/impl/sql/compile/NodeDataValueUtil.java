package org.apache.derby.impl.sql.compile;


import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.types.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Utility method(s) for manipulating Node DataValueDescriptors (splice).
 */
class NodeDataValueUtil {

    private static final DataValueDescriptor ZERO_BIG_DECIMAL = new SQLDecimal(BigDecimal.ZERO);

    /**
     * Given a BinaryRelationalOperatorNode with a ColumnReferenceOperand and a NumericConstantNode operand
     * make the NumericConstantNode have the same type as the column reference.  If the numeric constant does not
     * fit in the column's type, then change the value of the constant to Type.MAX_VALUE and adjust the relational
     * operator node's operation accordingly (to always evaluate to TRUE or FALSE as appropriate).
     */
    public static void coerceDataTypeIfNecessary(BinaryRelationalOperatorNode operatorNode) {

        ValueNode leftOperand = operatorNode.getLeftOperand();
        ValueNode rightOperand = operatorNode.getRightOperand();

        if (leftOperand instanceof ColumnReference && rightOperand instanceof NumericConstantNode) {
            coerceDataTypeIfNecessary(operatorNode, (ColumnReference) leftOperand, (NumericConstantNode) rightOperand, ColumnSide.LEFT);
        } else if (rightOperand instanceof ColumnReference && leftOperand instanceof NumericConstantNode) {
            coerceDataTypeIfNecessary(operatorNode, (ColumnReference) rightOperand, (NumericConstantNode) leftOperand, ColumnSide.RIGHT);
        }
    }

    private static void coerceDataTypeIfNecessary(
            BinaryRelationalOperatorNode operatorNode,
            ColumnReference columnReferenceNode,
            NumericConstantNode numericConstantNode,
            ColumnSide columnSide) {

        DataTypeDescriptor newType = columnReferenceNode.getTypeServices();
        try {
            if (!newType.getTypeId().equals(numericConstantNode.getTypeId())) {
                coerceDataType(operatorNode, numericConstantNode, newType, columnSide);
            }
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    private static void coerceDataType(
            BinaryRelationalOperatorNode operatorNode,
            NumericConstantNode numericConstantNode,
            DataTypeDescriptor newType,
            ColumnSide columnSide) throws StandardException {

        DataValueDescriptor colReferValueDescriptor = newType.getNull();
        DataValueDescriptor constantValueDescriptor = numericConstantNode.getValue();

        /* We ALWAYS change the type of the numeric constant node */
        int typeFormatId = colReferValueDescriptor.getTypeFormatId();
        int newTargetNodeType = getCorrectNodeType(typeFormatId, numericConstantNode.getNodeType());
        numericConstantNode.setNodeType(newTargetNodeType);
        numericConstantNode.setType(newType);

        /* We have a constant value that FITS in the column type. */
        if (doesFit(colReferValueDescriptor, constantValueDescriptor)) {
            colReferValueDescriptor.setValue(constantValueDescriptor);
            numericConstantNode.setValue(colReferValueDescriptor);
        }
        /* We have a constant value bigger than the column. */
        else {
            /*
             * CASE: scalar, pretty easy
             */
            if (isScalarType(typeFormatId)) {
                updateOperator(operatorNode, numericConstantNode, typeFormatId, columnSide);
            }
        }
    }

    /**
     * Does the *VALUE* of the numeric constant fit within the *TYPE* of the column reference?
     */
    private static boolean doesFit(DataValueDescriptor colReferValueDescriptor,
                                   DataValueDescriptor constValueDescriptor) throws StandardException {

        /* If the col ref type is larger, then yes */
        if (colReferValueDescriptor.getLength() >= constValueDescriptor.getLength()) {
            return true;
        }
        /* Otherwise only if the constant is <= the column's max type. AND >= the column's min type */
        DataValueDescriptor maxValueForColumn = getMaxValueForType(colReferValueDescriptor.getTypeFormatId());
        DataValueDescriptor minValueForColumn = getMinValueForType(colReferValueDescriptor.getTypeFormatId());
        return constValueDescriptor.lessOrEquals(constValueDescriptor, maxValueForColumn).getBoolean()
                &&
                constValueDescriptor.greaterOrEquals(constValueDescriptor, minValueForColumn).getBoolean();
    }

    /**
     * When mutating the operator there are really four cases. Here I illustrate the cases using a column
     * reference "column" and a positive numeric constant "L" which derby has determined to be too large for
     * the column's type, perhaps it is an SQLLongint (long).
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
     *  L != column  ===> column <= Type.MAX_VALUE
     * -L != column  ===> column <= Type.MAX_VALUE
     *
     *  L >  column  ===>  column > Type.MAX_VALUE
     *  L >= column  ===>  column > Type.MAX_VALUE
     *
     * -L <  column  ===>  column > Type.MAX_VALUE
     * -L <= column  ===>  column > Type.MAX_VALUE
     *
     * CASE 4: expressions that always evaluate to FALSE
     *
     *  L =  column  ===>  column > Type.MAX_VALUE
     * -L =  column  ===>  column > Type.MAX_VALUE
     *
     *  L <  column  ===> column <= Type.MAX_VALUE
     *  L <= column  ===> column <= Type.MAX_VALUE
     *
     * -L >  column  ===> column <= Type.MAX_VALUE
     * -L >= column  ===> column <= Type.MAX_VALUE
     */
    private static void updateOperator(BinaryRelationalOperatorNode operatorNode,
                                       NumericConstantNode numericConstantNode,
                                       int typeFormatId,
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
        numericConstantNode.setValue(getMaxValueForType(typeFormatId));
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

    private static DataValueDescriptor getMinValueForType(int typeFormatId) throws StandardException {
        switch (typeFormatId) {
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
                throw new IllegalArgumentException();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static DataValueDescriptor getMaxValueForType(int typeFormatId) throws StandardException {
        switch (typeFormatId) {
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
                throw new IllegalArgumentException();
            default:
                throw new IllegalArgumentException();
        }
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

    private static boolean isScalarType(int columnFormat) {
        return (columnFormat == StoredFormatIds.SQL_TINYINT_ID
                || columnFormat == StoredFormatIds.SQL_SMALLINT_ID
                || columnFormat == StoredFormatIds.SQL_INTEGER_ID
                || columnFormat == StoredFormatIds.SQL_LONGINT_ID);
    }

    /**
     * Indicates which side the column reference is on
     */
    private static enum ColumnSide {
        LEFT,
        RIGHT
    }

}