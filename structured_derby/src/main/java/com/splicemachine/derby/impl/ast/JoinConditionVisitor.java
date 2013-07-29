package com.splicemachine.derby.impl.ast;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.impl.sql.compile.*;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * User: pjt
 * Date: 7/8/13
 */
public class JoinConditionVisitor extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(JoinConditionVisitor.class);

    public static boolean isMSJ(AccessPath ap){
        return (ap != null && ap.getJoinStrategy().getClass() == MergeSortJoinStrategy.class);
    }

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
    public FromBaseTable visit(FromBaseTable t) {
        return pullPredsFromTable(t);
    }

    @Override
    public JoinNode visit(JoinNode j) {
        return addPredsToJoin(j);
    }

    public FromBaseTable pullPredsFromTable(FromBaseTable t) {
        if (isMSJ(t.getTrulyTheBestAccessPath())) {
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
            } catch (NullPointerException npe) {
                LOG.error(npe);
            }
        }
        return t;
    }

    public JoinNode addPredsToJoin(JoinNode j) {
        int pSize = pulledPreds.size();
        OptimizablePredicate[] currentPreds = pulledPreds.toArray(new OptimizablePredicate[pSize]);

        for (OptimizablePredicate p : currentPreds) {
            if (predicateIsEvalable(p, j)) {
                try {
                    updatePredColRefsToThis((Predicate)p, j);
                    j.addOptPredicate(p);
                    pulledPreds.remove(p);
                } catch (StandardException e) {
                    LOG.error(e);
                }
            }
        }
        return j;
    }

    public Predicate updatePredColRefsToThis(Predicate p, JoinNode j)
            throws StandardException
    {
        ResultColumnList rcl = j.getResultColumns();
        Map<List<Integer>, ResultColumn> chain = rsnChainMap(rcl);
        Vector<ColumnReference> predCRs = collectNodes(p, ColumnReference.class);
        for (ColumnReference cr: predCRs){
            ResultColumn rc = cr.getSource();
            List<Integer> rsnAndCol = Arrays.asList(rc.getResultSetNumber(), rc.getVirtualColumnId());
            if (chain.containsKey(rsnAndCol)){
                cr.setSource(chain.get(rsnAndCol));
            }
        }
        return p;
    }


    public Map<List<Integer>, ResultColumn> rsnChainMap(ResultColumnList rcl)
            throws StandardException
    {
        Map<List<Integer>, ResultColumn> chain = new HashMap<List<Integer>, ResultColumn>();
        Vector<ResultColumn> cols = collectNodes(rcl, ResultColumn.class);

        for (ResultColumn rc: cols){
            List<Integer> top = Arrays.asList(rc.getResultSetNumber(), rc.getVirtualColumnId());
            chain.put(top, rc);
            for (List<Integer> link: rsnChain(rc)){
                chain.put(link, rc);
            }
        }

        return chain;
    }

    public List<List<Integer>> rsnChain(ResultColumn rc)
            throws StandardException
    {
        List<List<Integer>> chain = new ArrayList<List<Integer>>();

        ValueNode expression = rc.getExpression();
        while (expression != null) {
            if (expression instanceof VirtualColumnNode) {
                ResultColumn sc = ((VirtualColumnNode) expression).getSourceColumn();
                chain.add(Arrays.asList(sc.getResultSetNumber(), sc.getVirtualColumnId()));
                expression = sc.getExpression();
            } else if (expression instanceof ColumnReference) {
                ResultColumn sc = ((ColumnReference) expression).getSource();
                if (sc != null){ // A ColumnReference can be sourceless…
                    chain.add(Arrays.asList(sc.getResultSetNumber(), sc.getVirtualColumnId()));
                    expression = sc.getExpression();
                } else {
                    expression = null;
                }
            } else {
                expression = null;
            }
        }

        return chain;
    }

    public <N> Vector<N> collectNodes(Visitable node, Class<N> clazz)
            throws StandardException
    {
        CollectNodesVisitor v = new CollectNodesVisitor(clazz);
        node.accept(v);
        return (Vector<N>)v.getList();
    }
}
