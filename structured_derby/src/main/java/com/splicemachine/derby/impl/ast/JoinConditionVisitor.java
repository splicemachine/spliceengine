package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.impl.sql.compile.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;


/**
 * User: pjt
 * Date: 7/8/13
 */
public class JoinConditionVisitor extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(JoinConditionVisitor.class);

    public static boolean predicateIsEvalable(OptimizablePredicate p, ResultSetNode n) {
        JBitSet pRefs = (JBitSet) p.getReferencedMap().clone();
        JBitSet nRefs = n.getReferencedTableMap();
        // make pRefs represent union of tables referenced
        pRefs.or(nRefs);
        // make pRefs represent union of tables referenced minus tables referenced by n
        pRefs.xor(nRefs);
        // if all bits in pRefs are zero, then n refers to all tables referred to by p,
        // & p is therefore evalable in context of n
        return pRefs.getFirstSetBit() == -1;
    }

    private List<OptimizablePredicate> pulledPreds = new ArrayList<OptimizablePredicate>();

    @Override
    public FromBaseTable visit(FromBaseTable t){
        return pullPredsFromTable(t);
    }

    @Override
    public JoinNode visit(JoinNode j){
        return addPredsToJoin(j);
    }

    public FromBaseTable pullPredsFromTable(FromBaseTable t) {
        PredicateList pl = new PredicateList();
        try {
            t.pullOptPredicates(pl);
            for (int i = 0, s = pl.size(); i < s; i++) {
                OptimizablePredicate p = pl.getOptPredicate(i);
                if (!predicateIsEvalable(p, t)) {
                    pulledPreds.add(p);
                } else {
                    t.pushOptPredicate(p);
                }
            }
        } catch (StandardException e) {
            LOG.error("Error pull predicates", e);
        }
        return t;
    }

    public JoinNode addPredsToJoin(JoinNode j) {
        int pSize = pulledPreds.size();
        OptimizablePredicate[] currentPreds = pulledPreds.toArray(new OptimizablePredicate[pSize]);

        for (OptimizablePredicate p : currentPreds) {
            if (predicateIsEvalable(p, j)) {
                try {
                    j.pushOptPredicate(p);
                    pulledPreds.remove(p);
                } catch (StandardException e) {
                    LOG.error(e);
                }
            }
        }
        return j;
    }

}
