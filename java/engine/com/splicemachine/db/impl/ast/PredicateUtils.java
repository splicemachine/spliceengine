package com.splicemachine.db.impl.ast;

import com.google.common.base.*;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicate;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.Predicate;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * @author P Trolard
 *         Date: 18/10/2013
 */
public class PredicateUtils {

    public static com.google.common.base.Predicate<Predicate> isEquiJoinPred = new com.google.common.base.Predicate<Predicate>() {
        @Override
        public boolean apply(Predicate p) {
            return p != null &&
                    p.isJoinPredicate() &&
                    p.getAndNode().getLeftOperand().isBinaryEqualsOperatorNode();
        }
    };

    /**
     * Return string representation of Derby Predicate
     */
    public static Function<Predicate, String> predToString = new Function<Predicate, String>() {
        @Override
        public String apply(Predicate predicate) {
            if (predicate == null) {
                return null;
            }
            ValueNode operand = predicate.getAndNode().getLeftOperand();
            return opToString(operand);
        }
    };

    /**
     * Return string representation of Derby PredicateList
     */
    public static Function<PredicateList, String> predListToString = new Function<PredicateList, String>() {
        @Override
        public String apply(PredicateList predicateList) {
            if (predicateList == null) {
                return null;
            }
            StringBuilder buf = new StringBuilder();
            for (int i = 0, s = predicateList.size(); i < s; i++) {
                OptimizablePredicate predicate = predicateList.getOptPredicate(i);
                ValueNode operand = ((Predicate) predicate).getAndNode().getLeftOperand();
                buf.append(opToString(operand)).append(", ");
            }
            if (buf.length() > 2) {
                // trim last ", "
                buf.setLength(buf.length() - 2);
            }
            return buf.toString();
        }
    };

    /**
     * Return a List of Predicates for a Derby PredicateList
     */
    public static List<Predicate> PLtoList(PredicateList pl) {
        List<Predicate> preds = new ArrayList<>(pl.size());
        for (int i = 0, s = pl.size(); i < s; i++) {
            OptimizablePredicate p = pl.getOptPredicate(i);
            preds.add((Predicate) p);
        }
        return preds;
    }

    /**
     * Return string representation of a Derby expression
     */
    public static String opToString(ValueNode operand){
        if(operand==null){
            return "";
        }else if(operand instanceof UnaryOperatorNode){
            UnaryOperatorNode uop=(UnaryOperatorNode)operand;
            return format("%s(%s)",uop.getOperatorString(),opToString(uop.getOperand()));
        }else if(operand instanceof BinaryRelationalOperatorNode){
            BinaryRelationalOperatorNode bron=(BinaryRelationalOperatorNode)operand;
            InListOperatorNode inListOp=bron.getInListOp();
            if(inListOp!=null) return opToString(inListOp);

            return format("(%s %s %s)",opToString(bron.getLeftOperand()),
                    bron.getOperatorString(),opToString(bron.getRightOperand()));
        }else if(operand instanceof BinaryListOperatorNode){
            BinaryListOperatorNode blon = (BinaryListOperatorNode)operand;
            StringBuilder inList = new StringBuilder("(").append(opToString(blon.getLeftOperand()))
                    .append(" ")
                    .append(blon.getOperator())
                    .append(" (");
            ValueNodeList rightOperandList=blon.getRightOperandList();
            boolean isFirst = true;
            for(Object qtn: rightOperandList){
                if(isFirst) isFirst = false;
                else inList = inList.append(",");
                inList = inList.append(opToString((ValueNode)qtn));
            }
            return inList.append("))").toString();
        }else if (operand instanceof BinaryOperatorNode) {
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
            return format("%s%s%s",table==null?"":format("%s.",table),
                    cr.getColumnName(),source==null?"":
                            format("[%s:%s]",source.getResultSetNumber(),source.getVirtualColumnId()));
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
        } else if(operand instanceof CastNode){
            return opToString(((CastNode)operand).getCastOperand());
        } else{
            return StringUtils.replace(operand.toString(), "\n", " ");
        }
    }
}
