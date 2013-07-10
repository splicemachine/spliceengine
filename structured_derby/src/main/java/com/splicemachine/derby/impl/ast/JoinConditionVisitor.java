package com.splicemachine.derby.impl.ast;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.impl.sql.compile.*;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.util.*;


/**
 * User: pjt
 * Date: 7/8/13
 */
public class JoinConditionVisitor extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(JoinConditionVisitor.class);

    public static boolean predicateIsEvalable(Predicate p, ResultSetNode n){
        JBitSet pRefs = (JBitSet)p.getReferencedMap().clone();
        JBitSet nRefs = n.getReferencedTableMap();
        // make pRefs represent union of tables referenced
        pRefs.or(nRefs);
        // make pRefs represent union of tables referenced minus tables referenced by n
        pRefs.xor(nRefs);
        // if all bits in pRefs are zero, then n refers to all tables referred to by p,
        // & p is therefore evalable in context of n
        return pRefs.getFirstSetBit() == -1;
    }

    //private ListMultimap<JBitSet, Predicate> pulledPreds = ArrayListMultimap.create();
    private List<Predicate> pulledPreds = new ArrayList<Predicate>();

    @Override
    public ResultSetNode visit(ResultSetNode node) {
        if (node instanceof FromBaseTable) {
            node = pullPredsFromTable((FromBaseTable) node);
        } else if (node instanceof JoinNode) {
            node = addPredsToJoin((JoinNode)node);
        }
        return node;
    }

    public FromBaseTable pullPredsFromTable(FromBaseTable t){
        PredicateList pl = new PredicateList();
        try {
            t.pullOptPredicates(pl);
            for (int i = 0, s = pl.size(); i < s; i++){
               Predicate p = (Predicate)pl.getOptPredicate(i);
                if (!predicateIsEvalable(p, t)){
                    //pulledPreds.put(p.getReferencedMap(), p);
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

    public JoinNode addPredsToJoin(JoinNode j){
        for (Predicate p: pulledPreds){
            if (predicateIsEvalable(p, j)){
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
