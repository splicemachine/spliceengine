package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.PreparedStatement;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;

import static java.lang.String.format;

/**
 * Created by jyuan on 5/30/14.
 */
public class XPlainUtils {
    public static String opToString(ValueNode operand) {
        if (operand == null) {
            return "";
        } else if (operand instanceof UnaryOperatorNode) {
            UnaryOperatorNode uop = (UnaryOperatorNode) operand;
            return format("%s(%s)", uop.getOperatorString(), opToString(uop.getOperand()));
        } else if (operand instanceof BinaryOperatorNode) {
            BinaryOperatorNode bop = (BinaryOperatorNode) operand;
            return format("(%s %s %s)", opToString(bop.getLeftOperand()),
                    bop.getOperatorString(), opToString(bop.getRightOperand()));
        } else if (operand instanceof TernaryOperatorNode) {
            TernaryOperatorNode top = (TernaryOperatorNode) operand;
            ValueNode rightOp = top.getRightOperand();
            return format("%s(%s, %s%s)", top.getOperator(), opToString(top.getReceiver()),
                    opToString(top.getLeftOperand()), rightOp == null ? "" : ", " + opToString(rightOp));
        } else if (operand instanceof ColumnReference) {
            ColumnReference cr = (ColumnReference) operand;
            String table = cr.getTableName();
            ResultColumn source = cr.getSource();
            return format("%s%s%s", table == null ? "" : format("%s.", table),
                    cr.getColumnName(), source == null ? "" :
                            format("[%s:%s]", source.getResultSetNumber(), source.getVirtualColumnId()));
        } else if (operand instanceof VirtualColumnNode) {
            VirtualColumnNode vcn = (VirtualColumnNode) operand;
            ResultColumn source = vcn.getSourceColumn();
            String table = source.getTableName();
            return format("%s%s%s", table == null ? "" : format("%s.", table),
                    source.getName(),
                    format("[%s:%s]", source.getResultSetNumber(), source.getVirtualColumnId()));
        } else if (operand instanceof SubqueryNode) {
            SubqueryNode subq = (SubqueryNode) operand;
            return format("subq=%s", subq.getResultSet().getResultSetNumber());
        } else if (operand instanceof ConstantNode) {
            ConstantNode cn = (ConstantNode) operand;
            try {
                return cn.getValue().getString();
            } catch (StandardException se) {
                return se.getMessage();
            }
        } else {
            return operand.toString().replace("\n", " ");
        }
    }

    public static boolean shouldTrace(LanguageConnectionContext lcc) {

        boolean result = false;
        if (lcc.getStatisticsTiming() && lcc.getRunTimeStatisticsMode()) {

            if (!lcc.getStatementContext().hasXPlainTableOrProcedure()) {
                result = true;
            }
        }
        else {
            // automatically turn on explain trace
            if (!lcc.getStatementContext().hasXPlainTableOrProcedure() &&
                    lcc.getStatementContext().getMaxCardinality() > 3) {
                result = true;
            }
        }

        return result;
    }

    public static void setXPlainTrace(LanguageConnectionContext lcc, PreparedStatement preparedStatement, Activation activation) {

        StatementContext statementContext = lcc.getStatementContext();

        // Determine if the statement should be auto traced
        if (!preparedStatement.isAutoTraced() && statementContext != null &&
            (statementContext.getMaxCardinality() > 0 || statementContext.hasXPlainTableOrProcedure())) {

            preparedStatement.setXPlainTableOrProcedure(statementContext.hasXPlainTableOrProcedure());
            if (statementContext.getMaxCardinality() > 3 && !statementContext.hasXPlainTableOrProcedure()) {
                // Should be auto traced
                preparedStatement.setAutoTraced(true);
            }
            // Clear flags because statement context may be reused
            statementContext.setXPlainTableOrProcedure(false);
            statementContext.setMaxCardinality(0);
        }

        // Turn on explain trace if it is set by a user
        if (lcc.getStatisticsTiming() && lcc.getRunTimeStatisticsMode()) {
            if (!preparedStatement.hasXPlainTableOrProcedure())
            activation.setTraced(true);
            return;
        }

        // If auto trace is on for current connection, and the statement should be auto traced, trace this activation
        if (lcc.isAutoTraced()) {
            if (preparedStatement.isAutoTraced()) {
                // This statement was auto traced before
                activation.setTraced(true);
            }
        }
    }
}
