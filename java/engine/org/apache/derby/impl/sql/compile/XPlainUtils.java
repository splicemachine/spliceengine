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

            if (!lcc.getStatementContext().hasExplainTableOrProcedure()) {
                result = true;
            }
        }
        else {
            // automatically turn on explain trace
            if (!lcc.getStatementContext().hasExplainTableOrProcedure() &&
                    lcc.getStatementContext().getMaxCardinality() > 3) {
                result = true;
            }
        }

        return result;
    }

    public static void setXPlainTrace(LanguageConnectionContext lcc, PreparedStatement preparedStatement, Activation activation) {

        StatementContext statementContext = lcc.getStatementContext();

        if (statementContext != null &&
            (statementContext.getMaxCardinality() > 0 || statementContext.hasExplainTableOrProcedure())) {
            // This is the first time a sql statement is parsed
            preparedStatement.setXPlainTableOrProcedure(statementContext.hasExplainTableOrProcedure());
            if (statementContext.getMaxCardinality() > 3 && !statementContext.hasExplainTableOrProcedure()) {
                // Should be auto traced
                preparedStatement.setAutoTraced(true);
                activation.setTraced(true);
            }
            else {
                if (lcc.getStatisticsTiming() && lcc.getRunTimeStatisticsMode()) {
                    // xplain trace is on
                    if (!statementContext.hasExplainTableOrProcedure()) {
                        activation.setTraced(true);

                    }
                }

            }
            statementContext.setExplainTableOrProcedure(false);
            statementContext.setMaxCardinality(0);
        }
        else {
            if (lcc.getStatisticsTiming() && lcc.getRunTimeStatisticsMode()) {
                // If xplain trace is on, and the statement was not auto traced
                if (!preparedStatement.isAutoTraced() && !preparedStatement.hasXPlainTableOrProcedure()) {
                    activation.setTraced(true);
                }
            }
            else {
                // if xplain trace is off, and the statement was not auto traced
                if (!preparedStatement.isAutoTraced()) {
                    activation.setTraced(false);
                }
            }
        }
    }
}
