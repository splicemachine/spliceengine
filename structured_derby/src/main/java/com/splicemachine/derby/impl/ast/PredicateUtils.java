package com.splicemachine.derby.impl.ast;

import com.google.common.base.Function;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.impl.sql.compile.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * @author P Trolard
 *         Date: 18/10/2013
 */
public class PredicateUtils {

    /**
     * Return string representation of Derby Predicate
     */
    public static Function<Predicate, String> predToString = new Function<Predicate, String>() {
        @Override
        public String apply(@Nullable Predicate predicate) {
            if (predicate == null) {
                return null;
            }
            ValueNode operand = predicate.getAndNode().getLeftOperand();
            return opToString(operand);
        }
    };

    /**
     * Return string representation of a Derby expression
     */
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
            return operand.toString();
        }
    }

    /**
     * Return a List of Predicates for a Derby PredicateList
     */
    public static List<Predicate> PLtoList(PredicateList pl) {
        List<Predicate> preds = new ArrayList<Predicate>(pl.size());
        for (int i = 0, s = pl.size(); i < s; i++) {
            OptimizablePredicate p = pl.getOptPredicate(i);
            preds.add((Predicate) p);
        }
        return preds;
    }

}
