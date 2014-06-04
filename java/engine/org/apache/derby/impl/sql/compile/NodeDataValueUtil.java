package org.apache.derby.impl.sql.compile;


import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * Utility method(s) for manipulating Node DataValueDescriptors (splice).
 */
class NodeDataValueUtil {

    /**
     * Make the specified numeric constant node have the specified data type if possible.  Does nothing
     * if the numeric constant node already has the specified type, or if the value of the numeric constant cannot
     * fit within the specified type.
     *
     * @param node change this (NumericConstantNode) node's type
     * @param newType take the type from this node
     */
    public static void coerceDataTypeIfNecessary(NumericConstantNode node, DataTypeDescriptor newType) {
        try {
            if(!newType.getTypeId().equals(node.getTypeId())) {
                coerceDataType(node, newType);
            }
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    private static void coerceDataType(NumericConstantNode node, DataTypeDescriptor newType) throws StandardException {
        DataValueDescriptor srcValueDescriptor = newType.getNull();
        DataValueDescriptor dstValueDescriptor = node.getValue();

        if(srcValueDescriptor.getLength() >= dstValueDescriptor.getLength()) {
            srcValueDescriptor.setValue(dstValueDescriptor);
            node.setValue(srcValueDescriptor);

            int newTargetNodeType = getCorrectNodeType(srcValueDescriptor.getTypeFormatId(), node.getNodeType());
            node.setNodeType(newTargetNodeType);
            node.setType(newType);
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

}