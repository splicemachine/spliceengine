package com.splicemachine.derby.impl.ast;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.impl.sql.compile.*;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Find columns used in equijoin predicates and set the {left,right}HashKeys field on the JoinNode.
 * <p/>
 * Dependency: relies on join predicates having been pulled up from leaf nodes and attached
 * directly to join in the MSJJoinConditionVisitor.
 *
 * @author P Trolard
 *         Date: 18/10/2013
 */

public class FindHashJoinColumns extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(FindHashJoinColumns.class);

    @Override
    public JoinNode visit(final JoinNode node) throws StandardException {
        if (!RSUtils.isHashableJoin(((Optimizable) node.getRightResultSet()).getTrulyTheBestAccessPath())) {
            return node;
        }
        Set<Predicate> equiJoinPreds =
                Sets.filter(Sets.newHashSet(PredicateUtils.PLtoList(node.joinPredicates)),
                        PredicateUtils.isEquiJoinPred);
        Pair<List<Integer>, List<Integer>> indices = findHashIndices(node, equiJoinPreds);
        LOG.info(String.format("Hash key indices found for Join n=%s: %s", node.getResultSetNumber(), indices));
        node.leftHashKeys = Ints.toArray(indices.getFirst());
        node.rightHashKeys = Ints.toArray(indices.getSecond());

        for (Predicate p : equiJoinPreds) {
            // cleanup joinPredicates, as equijoin preds are handled by hashing
            //node.joinPredicates.removeOptPredicate(p);
        }

        return node;
    }

    @Override
    public JoinNode visit(HalfOuterJoinNode node) throws StandardException {
        return visit((JoinNode) node);
    }

    public static com.google.common.base.Predicate<ColumnReference> pointsTo(final ResultSetNode rsn) {
        return new com.google.common.base.Predicate<ColumnReference>() {
            @Override
            public boolean apply(ColumnReference cr) {
                try {
                    Set<Integer> rsns = Sets.newHashSet(Iterables.transform(RSUtils.getSelfAndDescendants(rsn), RSUtils.rsNum));
                    return rsns.contains(cr.getSourceResultColumn().getResultSetNumber());
                } catch (StandardException se) {
                    throw new RuntimeException(se);
                }
            }
        };
    }

    public static Integer translateToIndexOnNode(ColumnReference cr, ResultColumnList rcl)
            throws StandardException {
        Map<Pair<Integer, Integer>, ResultColumn> chainMap = ColumnUtils.rsnChainMap(rcl);
        Pair<Integer, Integer> colCoord = ColumnUtils.RSCoordinate(cr.getSourceResultColumn());
        if (chainMap.containsKey(colCoord)) {
            return chainMap.get(colCoord).getVirtualColumnId() - 1; // translate from 1-based to 0-based index
        } else {
            throw new RuntimeException(String.format("Unable to find ColRef %s in RCL %s", cr, rcl));
        }
    }

    public static Pair<List<Integer>, List<Integer>> findHashIndices(final JoinNode node, Collection<Predicate> equiJoinPreds)
            throws StandardException {
        List<Integer> leftIndices = Lists.newArrayListWithCapacity(equiJoinPreds.size());
        List<Integer> rightIndices = Lists.newArrayListWithCapacity(equiJoinPreds.size());
        com.google.common.base.Predicate<ColumnReference> isLeftRef = pointsTo(node.getLeftResultSet());
        ResultColumnList leftRCL = node.getLeftResultSet().getResultColumns();
        ResultColumnList rightRCL = node.getRightResultSet().getResultColumns();

        for (Predicate p : equiJoinPreds) {
            for (ColumnReference cr : RSUtils.collectNodes(p, ColumnReference.class)) {
                if (isLeftRef.apply(cr)) {
                    leftIndices.add(translateToIndexOnNode(cr, leftRCL));
                } else {
                    rightIndices.add(translateToIndexOnNode(cr, rightRCL));
                }
            }
        }

        return new Pair<List<Integer>, List<Integer>>(leftIndices, rightIndices);
    }
}
