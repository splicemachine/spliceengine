package com.splicemachine.derby.impl.sql.compile.calcite.reloperators;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.derby.impl.sql.compile.calcite.SpliceContext;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
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


    public ValueNode convertExpression(RexNode expression, ResultSetNode source) throws StandardException {
      if (expression == null)
          return null;

      if (expression instanceof RexLiteral)
          return literalToValueNode((RexLiteral) expression);

      if (expression instanceof RexInputRef) {
          // map to ColumnReference
          ResultColumn rc = source.getResultColumns().elementAt(((RexInputRef) expression).getIndex());
          ColumnReference cr = (ColumnReference)nodeFactory.getNode(C_NodeTypes.COLUMN_REFERENCE, rc.getName(), null, contextManager);
          cr.setSource(rc);
          // TODO do we still need to fill the tableNumber for columnreference?
          return cr;
      }

      if (expression instanceof RexCall) {
          RexCall rexCall = (RexCall)expression;
          SqlOperator op = rexCall.getOperator();
          SqlKind kind = op.getKind();
          // binary operators:
          switch (kind) {
              case OTHER_FUNCTION:
                  ValueNode leftOperand = convertExpression(rexCall.getOperands().get(0), source);
                  ValueNode rightOperand = convertExpression(rexCall.getOperands().get(1), source);
                  if (op.getName().equals("REPEAT")) {
                      ValueNode binaryOpNode = (BinaryOperatorNode) nodeFactory.getNode(C_NodeTypes.REPEAT_OPERATOR_NODE, leftOperand, rightOperand, ReuseFactory.getInteger(BinaryOperatorNode.REPEAT), contextManager);
                      DataTypeDescriptor dtd = new DataTypeDescriptor(leftOperand.getTypeId(), rexCall.getType().getPrecision(),
                              rexCall.getType().getScale(), rexCall.getType().isNullable(),
                              rexCall.getType().getPrecision());
                      binaryOpNode.setType(dtd);
                      return binaryOpNode;
                  }
                  break;
              // binary arithematic operators:
              case PLUS:
              case MINUS:
              case DIVIDE:
              case TIMES:
              case MOD:
                  break;
              default:
                  assert false : "TODO: convert expressions";
                  break;
            }
        } else {
            assert false : "TODO: convert expressions";
        }

      return null;
    }

    public ConstantNode literalToValueNode(RexLiteral expression) throws StandardException {
        Object value1 = ((RexLiteral) expression).getValue();
        Object value;
        switch (((RexLiteral) expression).getType().getSqlTypeName()) {
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

    /**
     * convert condtion represented using Calcite's RexNode to Derby Conditions
     * @param rexNode
     * @param source
     */
    public ValueNode convertCondition(RexNode rexNode, ResultSetNode source) throws StandardException {
        if (rexNode instanceof RexLiteral) {
            return convertExpression(rexNode, null);
        }

        if (rexNode instanceof RexCall) {
            if (((RexCall) rexNode).getOperator() == SqlStdOperatorTable.AND) {
                ValueNode leftCond = convertCondition(((RexCall) rexNode).getOperands().get(0), source);
                ValueNode rightCond = convertCondition(((RexCall) rexNode).getOperands().get(1), source);
                ValueNode andNode = (AndNode)nodeFactory.getNode(C_NodeTypes.AND_NODE, leftCond, rightCond, contextManager);
                return andNode;
            } else if (((RexCall) rexNode).getOperator() == SqlStdOperatorTable.OR) {
                ValueNode leftCond = convertCondition(((RexCall) rexNode).getOperands().get(0), source);
                ValueNode rightCond = convertCondition(((RexCall) rexNode).getOperands().get(1), source);
                ValueNode orNode = (OrNode)nodeFactory.getNode(C_NodeTypes.OR_NODE, leftCond, rightCond, contextManager);
                return orNode;
            } else if (((RexCall) rexNode).getOperator() instanceof SqlBinaryOperator) {
                return convertBinaryOperator((RexCall)rexNode, source);
            }
        }

        // expressions
        ValueNode expressionNode = convertExpression(rexNode, source);

        return expressionNode;
    }

    private ValueNode convertBinaryOperator(RexCall rexCall, ResultSetNode source) throws StandardException {
        ValueNode leftOperand = convertCondition(rexCall.getOperands().get(0), source);
        ValueNode rightOperand = convertCondition(rexCall.getOperands().get(1), source);
        ValueNode opNode;
        boolean nullableResult;
        switch (rexCall.getOperator().getKind()) {
            /* relational operator */
            case EQUALS:
                opNode = (BinaryRelationalOperatorNode)nodeFactory.getNode(C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE, leftOperand, rightOperand, contextManager);
                setBinaryRelationalOperatorNullabilityAndType(opNode, leftOperand, rightOperand);
                break;
            case NOT_EQUALS:
                opNode = (BinaryRelationalOperatorNode)nodeFactory.getNode(C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE, leftOperand, rightOperand, contextManager);
                setBinaryRelationalOperatorNullabilityAndType(opNode, leftOperand, rightOperand);
                break;
            case GREATER_THAN:
                opNode = (BinaryRelationalOperatorNode)nodeFactory.getNode(C_NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE, leftOperand, rightOperand, contextManager);
                setBinaryRelationalOperatorNullabilityAndType(opNode, leftOperand, rightOperand);
                break;
            case GREATER_THAN_OR_EQUAL:
                opNode = (BinaryRelationalOperatorNode)nodeFactory.getNode(C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE, leftOperand, rightOperand, contextManager);
                setBinaryRelationalOperatorNullabilityAndType(opNode, leftOperand, rightOperand);
                break;
            case LESS_THAN:
                opNode = (BinaryRelationalOperatorNode)nodeFactory.getNode(C_NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE, leftOperand, rightOperand, contextManager);
                setBinaryRelationalOperatorNullabilityAndType(opNode, leftOperand, rightOperand);
                break;
            case LESS_THAN_OR_EQUAL:
                opNode = (BinaryRelationalOperatorNode)nodeFactory.getNode(C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE, leftOperand, rightOperand, contextManager);
                setBinaryRelationalOperatorNullabilityAndType(opNode, leftOperand, rightOperand);
                break;
            /* arithematic operator */
            case PLUS:
                opNode = (BinaryArithmeticOperatorNode)nodeFactory.getNode(C_NodeTypes.BINARY_PLUS_OPERATOR_NODE, leftOperand, rightOperand, contextManager);
                // simplify the type setting to go with left for now
                nullableResult = leftOperand.getTypeServices().isNullable() ||
                        rightOperand.getTypeServices().isNullable();
                opNode.setType(leftOperand.getTypeServices());
                opNode.setNullability(nullableResult);
                break;
            default:
                throw StandardException.newException(SQLState.LANG_INVADLID_CONVERSION, rexCall.getOperator().getKind());
        }

        return opNode;
    }

    private void setBinaryRelationalOperatorNullabilityAndType(ValueNode opNode,
                                                               ValueNode leftOperand,
                                                               ValueNode rightOperand) throws StandardException {
        boolean nullableResult = leftOperand.getTypeServices().isNullable() ||
                rightOperand.getTypeServices().isNullable();
        opNode.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, nullableResult));
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
