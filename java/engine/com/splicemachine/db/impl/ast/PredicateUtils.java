package com.splicemachine.db.impl.ast;

import com.google.common.base.*;

import com.splicemachine.db.iapi.sql.compile.OptimizablePredicate;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.OperatorToString;
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

    public static com.google.common.base.Predicate<Predicate> isJoinPred = new com.google.common.base.Predicate<Predicate>() {
        @Override
        public boolean apply(Predicate p) {
            return p != null &&
                    p.isJoinPredicate();
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
            return com.splicemachine.db.impl.sql.compile.OperatorToString.opToString(operand);
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
                buf.append(OperatorToString.opToString(operand)).append(", ");
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
        if (pl==null)
            return new ArrayList<Predicate>();
        List<Predicate> preds = new ArrayList<>(pl.size());
        for (int i = 0, s = pl.size(); i < s; i++) {
            OptimizablePredicate p = pl.getOptPredicate(i);
            preds.add((Predicate) p);
        }
        return preds;
    }


    /**
     * TRUE if the left operation is a ColumnReference with the specified nesting level.
     */
    public static boolean isLeftColRef(BinaryRelationalOperatorNode pred, int atSourceLevel) {
        if (!(pred.getLeftOperand() instanceof ColumnReference)) {
            return false;
        }
        ColumnReference left = (ColumnReference) pred.getLeftOperand();
        return left.getSourceLevel() == atSourceLevel;
    }

    /**
     * TRUE if the right operation is a ColumnReference with the specified nesting level.
     */
    public static boolean isRightColRef(BinaryRelationalOperatorNode pred, int atSourceLevel) {
        if (!(pred.getRightOperand() instanceof ColumnReference)) {
            return false;
        }
        ColumnReference right = (ColumnReference) pred.getRightOperand();
        return right.getSourceLevel() == atSourceLevel;
    }


}
