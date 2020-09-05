package com.splicemachine.derby.impl.sql.compile.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.compile.TypeCompiler;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.utils.Pair;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.*;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Created by yxia on 8/27/19.
 */
public class DerbyToCalciteRelBuilder extends RelBuilder {
    SpliceContext sc;

    public DerbyToCalciteRelBuilder(SpliceContext spliceContext, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(spliceContext, cluster, relOptSchema);
        this.sc = spliceContext;
    }

    public RelNode convertResultSet(ResultSetNode resultSetNode) throws StandardException {
        if (resultSetNode instanceof SelectNode)
            return convertSelect((SelectNode)resultSetNode);
        else if (resultSetNode instanceof RowResultSetNode)
            return convertRowResultSet((RowResultSetNode)resultSetNode);
        return null;
    }

    public RelNode convertSelect(SelectNode selectNode) throws StandardException {
        ConvertSelectContext selectContext = new ConvertSelectContext(selectNode);
        //construct the joins
        convertFromList(selectContext);

        // construct operator for where
        convertWhere(selectContext);

        // consruct operator for the final projection
        convertResultColumnList(selectContext);

        return selectContext.relRoot;
    }

    private void saveBaseColumns(FromBaseTable fromBaseTable) {
        ResultColumnList rcl = fromBaseTable.getResultColumns();
        int tableNumber = fromBaseTable.getTableNumber();
        for (int i=0; i < rcl.size(); i++) {
            ResultColumn rc = rcl.elementAt(i);
            if (rc.getExpression() instanceof BaseColumnNode) {
                int columnPosition = rc.getColumnPosition();
                sc.addBaseColumn(Pair.newPair(tableNumber, columnPosition), rc);
            }
        }
    }

    public RelNode convertFromList(ConvertSelectContext selectContext) {
        Map<Integer, Integer> startColPos = new HashMap<>();
        SelectNode selectNode = selectContext.root;
        FromList fromList = selectNode.getFromList();
        RelNode root = null;
        int totalCols = 0;
        for (int i=0; i< fromList.size(); i++) {
            FromTable fromTable = (FromTable) fromList.elementAt(i);
            RelNode oneTable = null;
            if (fromTable instanceof FromBaseTable) {
                oneTable = convertFromBaseTable((FromBaseTable)fromTable);
                startColPos.put(fromTable.getTableNumber(), totalCols);
                totalCols += oneTable.getRowType().getFieldCount();

                // save the result columns corresponding to base column in SpliceContext
                saveBaseColumns((FromBaseTable)fromTable);
            }
            if (root == null)
                root = oneTable;
            else if (oneTable != null) {
                JoinRelType convertedJoinType = JoinRelType.INNER;
                RexNode conditionExp;
                conditionExp =
                        cluster.getRexBuilder().makeLiteral(true);
                root = createJoin(
                                root,
                                oneTable,
                                conditionExp,
                                convertedJoinType);
            }
        }

        selectContext.setStartColPos(startColPos);
        selectContext.relRoot = root;
        return root;
    }

    public RelNode convertFromBaseTable(FromBaseTable fromBaseTable) {
        TableName tn = fromBaseTable.getTableNameField();
        List<String> names  = new ArrayList<>();
        names.add(tn.getSchemaName());
        names.add(tn.getTableName());
        final RelOptTable relOptTable = relOptSchema.getTableForMember(names);
        if (relOptTable == null) {
            throw RESOURCE.tableNotFound(String.join(".", names)).ex();
        }

        SpliceTable spliceTable = relOptTable.unwrap(SpliceTable.class);
        spliceTable.setFromBaseTableNode(fromBaseTable);
        spliceTable.setTableNumber(fromBaseTable.getTableNumber());
        final RelNode scan = LogicalTableScan.create(cluster, relOptTable, Collections.emptyList());

        return scan;

    }

    public RelNode createJoin(RelNode left, RelNode right, RexNode joinCond, JoinRelType joinType) {
        final Join originalJoin =
                (Join) RelFactories.DEFAULT_JOIN_FACTORY.createJoin(left, right,Collections.emptyList(),
                        joinCond, ImmutableSet.of(), joinType, false);

        return RelOptUtil.pushDownJoinConditions(originalJoin, this);
    }

    public RelNode convertWhere(ConvertSelectContext selectContext) throws StandardException {
        ValueNode whereClause = selectContext.root.getWhereClause();

        if (whereClause != null) {
            push(selectContext.relRoot);
            RexNode convertedCondition = convertCondition(whereClause, selectContext);

            selectContext.relRoot = filter(convertedCondition).build();
        }
        return selectContext.relRoot;
    }

    public void convertResultColumnList(ConvertSelectContext selectContext) throws StandardException {
        push(selectContext.relRoot);
        final ImmutableList.Builder<RexNode> fields = ImmutableList.builder();
        ResultColumnList rcl = selectContext.root.getResultColumns();
        for (int i=0; i<rcl.size(); i++) {
            ResultColumn rc = rcl.elementAt(i);
            fields.add(convertExpression(rc.getExpression(), selectContext));
        }

        selectContext.relRoot = project(fields.build()).build();
        return;
    }


    public RexNode convertJoinCondition(ConditionalNode joinCond, JoinConditionType type, RelNode left, RelNode right) {
        return null;
    }

    public RexNode convertExpression(ValueNode node,
                                     ConvertSelectContext selectContext) throws StandardException {
        if (node instanceof ColumnReference) {
            ColumnReference cr = (ColumnReference)node;
            // column number is 1-based, but calcite column pos is 0-based
            int fieldPos = selectContext.startColPos.get(cr.getTableNumber()) + cr.getColumnNumber()-1;
            return field(1, 0, fieldPos);
        }

        if (node instanceof ConstantNode) {
            Object constantObject = ((ConstantNode) node).getValue().getObject();
            return literal(constantObject);
        }

        if (node instanceof BinaryArithmeticOperatorNode) {
            BinaryArithmeticOperatorNode binaryOp = (BinaryArithmeticOperatorNode)node;
            String operator = binaryOp.getOperatorString();
            SqlOperator sqlOperator;
            switch (operator) {
                case TypeCompiler.PLUS_OP:
                    sqlOperator = SqlStdOperatorTable.PLUS;
                    break;
                case TypeCompiler.DIVIDE_OP:
                    sqlOperator = SqlStdOperatorTable.DIVIDE;
                    break;
                case TypeCompiler.MINUS_OP:
                    sqlOperator = SqlStdOperatorTable.MINUS;
                    break;
                case TypeCompiler.TIMES_OP:
                    sqlOperator = SqlStdOperatorTable.MULTIPLY;
                    break;
                default:
                    throw StandardException.newException(SQLState.LANG_INVADLID_CONVERSION, operator);
            }

            RexNode leftOperand = convertExpression(binaryOp.getLeftOperand(), selectContext);
            RexNode rightOperand = convertExpression(binaryOp.getRightOperand(), selectContext);
            return call(sqlOperator, leftOperand, rightOperand);
        }

        if (node instanceof TruncateOperatorNode) {

        }
        assert false: "TODO convert more expressions";
        return null;
    }

    public RexNode convertCondition(ValueNode node,
                                    ConvertSelectContext selectContext) throws StandardException {
        if (node == null)
            return null;

        if (node instanceof BooleanConstantNode) {
            return literal(((BooleanConstantNode)node).getValue().getBoolean());
        }

        if (node instanceof BinaryRelationalOperatorNode) {
            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode)node;
            int operator = bron.getOperator();
            SqlOperator sqlOperator = mapRelationalOperatorToCalciteSqlOperator(operator);
            RexNode leftOperand = convertExpression(bron.getLeftOperand(), selectContext);
            RexNode rightOperand = convertExpression(bron.getRightOperand(), selectContext);
            return call(sqlOperator, leftOperand, rightOperand);
        }

        if (node instanceof AndNode) {
            RexNode leftOperand = convertCondition(((AndNode)node).getLeftOperand(), selectContext);
            RexNode rightOperand = convertCondition(((AndNode)node).getRightOperand(), selectContext);
            return call(SqlStdOperatorTable.AND, leftOperand, rightOperand);
        }

        if (node instanceof OrNode) {
            RexNode leftOperand = convertCondition(((OrNode)node).getLeftOperand(), selectContext);
            RexNode rightOperand = convertCondition(((OrNode)node).getRightOperand(), selectContext);
            return call(SqlStdOperatorTable.OR, leftOperand, rightOperand);
        }

        if(node instanceof NotNode) {
            RexNode leftOperand = convertCondition(((NotNode)node).getOperand(), selectContext);
            return call(SqlStdOperatorTable.NOT, leftOperand);
        }

        assert false: "TODO convert more conditions";
        return null;
    }

    private SqlOperator mapRelationalOperatorToCalciteSqlOperator(int operator) throws StandardException {
        switch (operator) {
            case RelationalOperator.EQUALS_RELOP:
                return SqlStdOperatorTable.EQUALS;
            case RelationalOperator.NOT_EQUALS_RELOP:
                return SqlStdOperatorTable.NOT_EQUALS;
            case RelationalOperator.GREATER_THAN_RELOP:
                return SqlStdOperatorTable.GREATER_THAN;
            case RelationalOperator.GREATER_EQUALS_RELOP:
                return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            case RelationalOperator.LESS_THAN_RELOP:
                return SqlStdOperatorTable.LESS_THAN;
            case RelationalOperator.LESS_EQUALS_RELOP:
                return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            case RelationalOperator.IS_NOT_NULL_RELOP:
                return SqlStdOperatorTable.IS_NOT_NULL;
            case RelationalOperator.IS_NULL_RELOP:
                return SqlStdOperatorTable.IS_NULL;
            default:
                throw StandardException.newException(SQLState.LANG_INVADLID_CONVERSION, operator);
        }
    }

    protected class ConvertSelectContext {
        public SelectNode root;
        public RelNode relRoot;
        Map<Integer, Integer> startColPos = null;

        ConvertSelectContext(SelectNode selectNode) {
            root = selectNode;
        }

        void setStartColPos(Map<Integer, Integer> map) {
            startColPos = map;
        }
    }

    public RelNode convertRowResultSet(RowResultSetNode rowResultSetNode) throws StandardException {
        ResultColumnList rcl = rowResultSetNode.getResultColumns();
        Object[] values = new Object[rcl.size()];
        String[] names = new String[rcl.size()];
        for (int i=0; i<rcl.size(); i++) {
            ResultColumn rc = rcl.elementAt(i);
            ValueNode valueNode = rc.getExpression();

            // TODO: how to handle expresion
            assert valueNode instanceof ConstantNode: "does not support non-constant expression yet";
            Object constantObject = ((ConstantNode) valueNode).getValue().getObject();
            values[i] = constantObject;
            names[i] = rc.getName();
        }

        RelNode relNode = values(names, values).build();
        return relNode;
    }

    public RelNode getValuesStmtForPlan(RelNode root) {
        Object[] values = new Object[1];
        String[] names = new String[1];
        values[0] = RelOptUtil.toString(root);
        names[0] = "Plan";
        return values(names, values).build();
    }
}
