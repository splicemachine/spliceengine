package com.splicemachine.db.impl.ast;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.Predicate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Find columns used in equijoin predicates and set the {left,right}HashKeys field on the JoinNode.
 * <p/>
 * Dependency: relies on join predicates having been pulled up from leaf nodes and attached
 * directly to join in the JoinConditionVisitor.
 *
 * @author P Trolard
 *         Date: 18/10/2013
 */

public class FindHashJoinColumns extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(FindHashJoinColumns.class);

    @Override
    public JoinNode visit(JoinNode node) throws StandardException {
        if (!RSUtils.isHashableJoin(((Optimizable) node.getRightResultSet()).getTrulyTheBestAccessPath())) {
            return node;
        }
        Set<Predicate> equiJoinPreds =
                Sets.filter(Sets.newLinkedHashSet(PredicateUtils.PLtoList(node.joinPredicates)),
                        PredicateUtils.isEquiJoinPred);

        Pair<List<Integer>, List<Integer>> indices = findHashIndices(node, equiJoinPreds);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Hash key indices found for Join n=%s: %s", node.getResultSetNumber(), indices));
        node.leftHashKeys = Ints.toArray(indices.getLeft());
        node.rightHashKeys = Ints.toArray(indices.getRight());
        return node;
    }

    @Override
    public JoinNode visit(HalfOuterJoinNode node) throws StandardException {
        return visit((JoinNode) node);
    }

    public static Integer translateToIndexOnList(ColumnReference cr, ResultColumnList rcl)
            throws StandardException {
        Map<Pair<Integer, Integer>, ResultColumn> chainMap = ColumnUtils.rsnChainMap(rcl);
        boolean hasSubqueryNode = false;
        if (cr.getSource().getExpression() instanceof VirtualColumnNode) {
            VirtualColumnNode n = (VirtualColumnNode) cr.getSource().getExpression();
            if (n.getSourceResultSet() instanceof FromSubquery) {
                hasSubqueryNode = true;
            }
        }
        Pair<Integer, Integer> colCoord = ColumnUtils.RSCoordinate(cr.getSourceResultColumn()!=null&&!hasSubqueryNode?cr.getSourceResultColumn():cr.getOrigSourceResultColumn());
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
        com.google.common.base.Predicate<ResultColumn> isLeftRef = RSUtils.pointsTo(node.getLeftResultSet());
        ResultColumnList leftRCL = node.getLeftResultSet().getResultColumns();
        ResultColumnList rightRCL = node.getRightResultSet().getResultColumns();

        for (Predicate p : equiJoinPreds) {
            for (ColumnReference cr : RSUtils.collectNodes(p, ColumnReference.class)) {
                if (isLeftRef.apply(cr.getSourceResultColumn()!=null?cr.getSourceResultColumn():cr.getOrigSourceResultColumn())) {
                    leftIndices.add(translateToIndexOnList(cr, leftRCL));
                } else {
                    rightIndices.add(translateToIndexOnList(cr, rightRCL));
                }
            }
        }

        return Pair.of(leftIndices, rightIndices);
    }
}
