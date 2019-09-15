package com.splicemachine.db.impl.sql.calcite.reloperators;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.sql.calcite.SpliceContext;
import com.splicemachine.db.impl.sql.compile.ConstantNode;
import com.splicemachine.db.impl.sql.compile.ResultColumn;
import com.splicemachine.db.impl.sql.compile.ResultColumnList;
import com.splicemachine.db.impl.sql.compile.ResultSetNode;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;

/**
 * Created by yxia on 9/15/19.
 */
public class SpliceImplementor {
    SpliceContext sc;
    NodeFactory nodeFactory;
    ContextManager contextManager;

    public SpliceImplementor(SpliceContext sc) {
        this.sc = sc;
        nodeFactory = sc.getNodeFactory();
        contextManager = sc.getContextManager();
    }

    public ResultSetNode visitChild(int ordinal, RelNode input) throws StandardException {
        ResultSetNode resultSet = ((SpliceRelNode) input).implement(this);
        return addScrollInsensitiveNode(resultSet);
    }


    public ConstantNode literalToValueNode(Object value) throws StandardException {
        if (value == null)
            return null;

        if (value instanceof RexLiteral) {
            Object value1 = ((RexLiteral) value).getValue();
            switch (((RexLiteral) value).getType().getSqlTypeName()) {
                case BINARY:
                    value = byte[].class.cast(((ByteString) value1).getBytes());
                    break;
                case CHAR:
                case VARCHAR:
                    value = String.class.cast(((NlsString) value1).getValue());
                    break;
                case BIGINT:
                case INTEGER:
                    value = Integer.class.cast(((BigDecimal) value1).intValue());
                    break;
                case SMALLINT:
                    value = Short.class.cast(((BigDecimal) value1).shortValue());
                    break;
                case TINYINT:
                    value = Byte.class.cast(((BigDecimal) value1).byteValue());
                    break;
                case DOUBLE:
                    value = Double.class.cast(((BigDecimal) value1).doubleValue());
                    break;
                case REAL:
                case FLOAT:
                    value = Float.class.cast(((BigDecimal) value1).floatValue());
                    break;
                default:
                    value = value1;
            }
        }

        /* From DerbyToCalciteRelBuilder
} else if (value instanceof Boolean) {
  return rexBuilder.makeLiteral((Boolean) value);
} else if (value instanceof BigDecimal) {
  return rexBuilder.makeExactLiteral((BigDecimal) value);
} else if (value instanceof Float || value instanceof Double) {
  return rexBuilder.makeApproxLiteral(
      BigDecimal.valueOf(((Number) value).doubleValue()));
} else if (value instanceof Number) {
  return rexBuilder.makeExactLiteral(
      BigDecimal.valueOf(((Number) value).longValue()));
} else if (value instanceof String) {
  return rexBuilder.makeLiteral((String) value);
*/
        if (value instanceof Boolean) {
            return (ConstantNode) sc.getNodeFactory().getNode(C_NodeTypes.BOOLEAN_CONSTANT_NODE, value, sc.getContextManager());
        } else if (value instanceof BigDecimal || value instanceof Float || value instanceof Double) {
            return (ConstantNode) sc.getNodeFactory().getNode(C_NodeTypes.DECIMAL_CONSTANT_NODE, value, sc.getContextManager());
        } else if (value instanceof Integer) {
            return (ConstantNode) sc.getNodeFactory().getNode(C_NodeTypes.INT_CONSTANT_NODE, value, sc.getContextManager());
        } else if (value instanceof Long) {
            return (ConstantNode) sc.getNodeFactory().getNode(C_NodeTypes.LONGINT_CONSTANT_NODE, value, sc.getContextManager());
        } else if (value instanceof String) {
            return (ConstantNode) sc.getNodeFactory().getNode(C_NodeTypes.CHAR_CONSTANT_NODE, value, sc.getContextManager());
        }

        /* TODO date/time/timestamps */
        assert false : "TODO convert more expressions";
        return null;
    }

    public void setNameAndType(RelDataTypeField field, ResultColumn rc) throws StandardException {

        RelDataType type = field.getType();
        String name = field.getName();

        assert type instanceof BasicSqlType : type + "cannot be mapped!";

        SqlTypeName sqlTypeName = type.getSqlTypeName();

        DataTypeDescriptor dtd = null;
     //   if (sqlTypeName.allowsPrecScale(true, true)) {
            dtd = new DataTypeDescriptor(TypeId.getBuiltInTypeId(sqlTypeName.getJdbcOrdinal()),
                    type.getPrecision(),
                    type.getScale(),
                    type.isNullable(),
                    100 /*todo */
                /*,
        type.getCollation().getCollationName(),
        int collationDerivation */);
      //  }

        rc.setType(dtd);
        rc.setName(name);
    }

    private ResultSetNode addScrollInsensitiveNode(ResultSetNode resultSet) throws StandardException {
         /* we need to generate a new ResultSetNode to enable the scrolling
		 * on top of the tree before modifying the access paths.
		 */
        ResultSetNode siChild = resultSet;
        /* We get a shallow copy of the ResultColumnList and its
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
        ResultColumnList siRCList = resultSet.getResultColumns();
        ResultColumnList childRCList = siRCList.copyListAndObjects();
        resultSet.setResultColumns(childRCList);

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ScrollInsensitiveResultSetNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
        siRCList.genVirtualColumnNodes(resultSet, childRCList);

		/* Finally, we create the new ScrollInsensitiveResultSetNode */
        resultSet = (ResultSetNode) nodeFactory.getNode(
                C_NodeTypes.SCROLL_INSENSITIVE_RESULT_SET_NODE,
                resultSet,
                siRCList,
                null,
                contextManager);
        // Propagate the referenced table map if it's already been created
        if (siChild.getReferencedTableMap() != null) {
            resultSet.setReferencedTableMap((JBitSet) siChild.getReferencedTableMap().clone());
        }

        return resultSet;
    }
}
