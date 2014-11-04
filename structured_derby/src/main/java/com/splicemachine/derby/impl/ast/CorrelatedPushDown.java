package com.splicemachine.derby.impl.ast;


import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.*;
import org.apache.derby.impl.sql.compile.Predicate;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * When Derby encounters an IN subquery that cannot be flattened, it converts the expression
 * `expr IN (select col from subquery)` into `IS NOT NULL (subquery where expr = col)`. This
 * conversion makes an uncorrelated subquery into a correlated one (where expr contains
 * the correlated reference).
 *
 * For whatever reason, the predicate expr = col does not get pushed down as far as it could
 * be in the subquery tree. (TPCH Q20 is the triggering case here.) This visitor identifies
 * such correlated predicates and attempts to push them down as far as possible. Below is
 * the example from TPCH Q20.
 *
 * Plan for Q20 (omitting subquery n=15):
 *
 *   ScrollInsensitiveResultSetNode ({n=20})
 *    OrderByNode ({n=19})
 *      ProjectRestrictNode ({quals=[is not null(subq=17)], n=18})
 *        JoinNode ({n=4, preds=[(S_NATIONKEY[4:4] = N_NATIONKEY[4:5])], exe=SORTMERGE})
 *          ProjectRestrictNode ({n=3})
 *            FromBaseTable ({n=2, table=NATION,1280})
 *        ProjectRestrictNode ({n=1})
 *          FromBaseTable ({n=0, table=SUPPLIER,1248})
 *
 *   Subquery n=17: expression?=false, invariant?=true, correlated?=true
 *     ProjectRestrictNode ({quals=[(S_SUPPKEY[4:1] = PS_SUPPKEY[16:1])], n=17})
 *       ProjectRestrictNode ({quals=[(PS_AVAILQTY[9:3] > subq=15)], n=16})
 *         JoinNode ({n=9, preds=[(PS_PARTKEY[9:1] = P_PARTKEY[9:4])], exe=SORTMERGE})
 *           ProjectRestrictNode ({n=8})
 *             FromBaseTable ({n=7, table=PART,1264})
 *           ProjectRestrictNode ({quals=[true], n=6})
 *             FromBaseTable ({n=5, table=PARTSUPP,1232})
 *
 * The predicate targeted by this visitor is the one on PRNode n=17. Its optimial
 * placement is on FBT n=5. (Moving the predicate to n=5 can drastically reduce the
 * size of the join at n=9 and consequently the number of times subquery n=15 needs
 * to be executed.)
 *
 * The logic is to look for these kinds of predicates on the root node of a subquery
 * result set. They're identified by two operands, one of which is correlated and the
 * other is a column reference. Follow the column reference through appropriate nodes
 * until it (a) bottoms out on a BaseColumn on a FBT node or (b) is involved in an
 * expression (which, for now, we'll consdier as opaque). Appropriate nodes are all
 * but aggregates and unions (we could push down to both sides of a union, but we
 * won't for now).
 *
 * @author P Trolard
 *         Date: 03/03/2014
 */
public class CorrelatedPushDown extends AbstractSpliceVisitor {
    private static Logger LOG = Logger.getLogger(CorrelatedPushDown.class);

    public Visitable visit(SubqueryNode subq) throws StandardException {
        if (!(subq.getResultSet() instanceof ProjectRestrictNode)){
            // bail
            return subq;
        }
        ProjectRestrictNode pr = (ProjectRestrictNode) subq.getResultSet();
        PredicateList preds = RSUtils.getPreds(pr);
        if (preds.size() != 1) {
            // bail
            return subq;
        }
        Predicate pred = (Predicate) preds.getOptPredicate(0);
        List<ValueNode> colRefs = binaryOperands.apply(pred);
        if (colRefs.size() != 2) {
            // bail
            return subq;
        }
        /* revisit, thinking about non-colrefs or colrefs w/o ResultCols underneath */
        com.google.common.base.Predicate<ValueNode> isUncorrelated =
            RSUtils.refPointsTo(pr);
        Collection<ValueNode> uncorrelated =
            Collections2.filter(colRefs, isUncorrelated);

        if (uncorrelated.size() != 1){
            // bail
            return subq;
        }

        boolean didPush = pushdownPredWithColumn(pr, pred, uncorrelated.iterator().next());
        if (didPush){
            preds.removeOptPredicate(pred);
        }

        return subq;
    }


    public static final Function<Predicate,List<ValueNode>> binaryOperands = new Function<Predicate, List<ValueNode>>() {
        @Override
        public List<ValueNode> apply(Predicate pred) {
            ValueNode operator = pred.getAndNode().getLeftOperand();
            if (operator instanceof BinaryRelationalOperatorNode) {
                BinaryRelationalOperatorNode boperator = (BinaryRelationalOperatorNode) operator;
                return (List<ValueNode>)boperator.getChildren();
            }
            return null;
        }
    };

    public boolean pushdownPredWithColumn(ResultSetNode rsn, Predicate pred, ValueNode colRef)
            throws StandardException {
        try {
            ResultColumn rc = RSUtils.refToRC.apply(colRef);
            List<Pair<Integer, Integer>> chain = ColumnUtils.rsnChain(rc);
            Pair<Integer, Integer> lastLink = chain.get(chain.size() - 1);
            List<ResultSetNode> subTree = RSUtils.getSelfAndDescendants(rsn);
            Map<Integer, ResultSetNode> nodeMap = zipMap(Iterables.transform(subTree, RSUtils.rsNum), subTree);
            ResultSetNode targetRSN = nodeMap.get(lastLink.getFirst());
            rc.setResultSetNumber(lastLink.getFirst());
            rc.setVirtualColumnId(lastLink.getSecond());
            ((Optimizable)targetRSN).pushOptPredicate(pred);
        } catch (StandardException e) {
            LOG.warn("Exception pushing down topmost subquery predicate:", e);
            return false;
        }
        return true;
    }

    public static <K,V> Map<K,V> zipMap(Iterable<K> keys, Iterable<V> vals) {
        Map<K,V> m = new HashMap<K, V>();
        Iterator<V> valsIterator = vals.iterator();
        for (K key: keys){
            if (valsIterator.hasNext()) {
                m.put(key, valsIterator.next());
            }
        }
        return m;
    }

}
