package com.splicemachine.derby.impl.ast;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.derby.impl.sql.compile.MergeSortJoinStrategy;
import com.splicemachine.derby.impl.sql.compile.NestedLoopJoinStrategy;
import com.splicemachine.derby.impl.sql.compile.OrderByNode;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.JoinStrategy;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.AggregateNode;
import org.apache.derby.impl.sql.compile.ColumnReference;
import org.apache.derby.impl.sql.compile.DistinctNode;
import org.apache.derby.impl.sql.compile.FromBaseTable;
import org.apache.derby.impl.sql.compile.HalfOuterJoinNode;
import org.apache.derby.impl.sql.compile.HashableJoinStrategy;
import org.apache.derby.impl.sql.compile.IndexToBaseRowNode;
import org.apache.derby.impl.sql.compile.IntersectOrExceptNode;
import org.apache.derby.impl.sql.compile.JoinNode;
import org.apache.derby.impl.sql.compile.PredicateList;
import org.apache.derby.impl.sql.compile.ProjectRestrictNode;
import org.apache.derby.impl.sql.compile.ResultColumn;
import org.apache.derby.impl.sql.compile.ResultSetNode;
import org.apache.derby.impl.sql.compile.RowResultSetNode;
import org.apache.derby.impl.sql.compile.UnionNode;
import org.apache.derby.impl.sql.compile.ValueNode;
import org.apache.derby.impl.sql.compile.VirtualColumnNode;

import javax.annotation.Nullable;


/**
 * Utilities for Derby's ResultSetNodes
 *
 * @author P Trolard
 *         Date: 18/10/2013
 */
public class RSUtils {


    /**
     * Return any instances of clazz at or below node
     */
    public static <N> List<N> collectNodes(Visitable node, Class<N> clazz)
            throws StandardException {
        return CollectNodes.collector(clazz).collect(node);
    }

    public static <N> List<N> collectNodesUntil(Visitable node, Class<N> clazz,
                                                Predicate<? super Visitable> pred)
            throws StandardException {
        return CollectNodes.collector(clazz).until(pred).collect(node);
    }

    public static <N> List<N> collectExpressionNodes(ResultSetNode node, Class<N> clazz)
            throws StandardException {
        // define traversal axis to be the node itself (so we can get to its descendants) or,
        // our real target, non-ResultSetNodes
        Predicate<Object> onAxis = Predicates.or(Predicates.equalTo((Object)node),
                                                    Predicates.not(isRSN));
        return CollectNodes.collector(clazz).onAxis(onAxis).collect(node);
    }


    public static final Function<ResultSetNode,Integer> rsNum = new Function<ResultSetNode, Integer>() {
        @Override
        public Integer apply(ResultSetNode rsn) {
            return rsn.getResultSetNumber();
        }
    };

    /**
     * Return list of node and its ResultSetNode descendants, as returned by depth-first,
     * pre-order traversal. Does not descend into expression nodes (therefore doesn't
     * consider ResultSetNodes in subqueries descendents).
     */
    public static List<ResultSetNode> getSelfAndDescendants(ResultSetNode rsn) throws StandardException {
        return CollectNodes.collector(ResultSetNode.class).onAxis(isRSN).collect(rsn);
    }

    /**
     * Return immediate (ResultSetNode) children of node
     */
    public static List<ResultSetNode> getChildren(ResultSetNode node)
            throws StandardException {
        Predicate<Object> self = Predicates.equalTo((Object)node);
        Predicate<Object> notSelfButRS = Predicates.and(Predicates.not(self), isRSN);
        return CollectNodes.<ResultSetNode>collector(notSelfButRS)
            .onAxis(self)
            .collect(node);
    }

    public static final Predicate<Object> isRSN = Predicates.instanceOf(ResultSetNode.class);

    public static final Set<?> binaryRSNs = ImmutableSet.of(JoinNode.class, HalfOuterJoinNode.class,
            UnionNode.class, IntersectOrExceptNode.class);

    public static final Function<Object,Class<?>> classOf = new Function<Object, Class<?>>() {
        @Override
        public Class<?> apply(@Nullable Object input) {
            return input == null ? null : input.getClass();
        }
    };

    public static final Predicate<Object> isBinaryRSN =
            Predicates.compose(Predicates.in(binaryRSNs), classOf);

    // leafRSNs might need VTI eventually
    public static final Set<?> leafRSNs = ImmutableSet.of(FromBaseTable.class, RowResultSetNode.class);

    public static List<ResultSetNode> nodesUntilBinaryNode(ResultSetNode rsn) throws StandardException {
        return CollectNodes.collector(ResultSetNode.class)
                   .onAxis(isRSN)
                   .until(isBinaryRSN)
                   .collect(rsn);
    }

    /**
     * Returns the leaves for a query plan subtree
     */
    public static List<ResultSetNode> getLeafNodes(ResultSetNode rsn)
            throws StandardException {
        List<ResultSetNode> rsns = getSelfAndDescendants(rsn);
        List<ResultSetNode> leaves = new LinkedList<>();
        for (ResultSetNode r : rsns) {
            if (leafRSNs.contains(r.getClass())) {
                leaves.add(r);
            }
        }
        return leaves;
    }

    public static final Predicate<ResultSetNode> rsnHasPreds =
            Predicates.or(Predicates.instanceOf(ProjectRestrictNode.class), Predicates.instanceOf(FromBaseTable.class),
                    Predicates.instanceOf(IndexToBaseRowNode.class));

    public static PredicateList getPreds(FromBaseTable t) throws StandardException {
        PredicateList pl = new PredicateList();
        t.pullOptPredicates(pl);
        for (int i = 0, s = pl.size(); i < s; i++) {
            OptimizablePredicate p = pl.getOptPredicate(i);
            t.pushOptPredicate(p);
        }
        return pl;
    }

    public static PredicateList getPreds(ProjectRestrictNode pr) throws StandardException {
        return pr.restrictionList != null ? pr.restrictionList : new PredicateList();
    }

    public static PredicateList getPreds(IndexToBaseRowNode in) throws StandardException {
        return in.restrictionList != null ? in.restrictionList : new PredicateList();
    }

    public static boolean isMSJ(AccessPath ap){
        return (ap != null && ap.getJoinStrategy().getClass() == MergeSortJoinStrategy.class);
    }

    public static boolean isNLJ(AccessPath ap){
        return (ap != null && ap.getJoinStrategy().getClass() == NestedLoopJoinStrategy.class);
    }

    public static boolean isHashableJoin(AccessPath ap){
        if(ap==null) return false;
        JoinStrategy strategy = ap.getJoinStrategy();
        return strategy instanceof HashableJoinStrategy;
    }

    public static boolean isSinkingJoin(AccessPath ap){
        return isMSJ(ap);
    }

    public static Predicate<ResultColumn> pointsTo(ResultSetNode rsn)
            throws StandardException {
        final Set<Integer> rsns = Sets.newHashSet(Iterables.transform(getSelfAndDescendants(rsn), rsNum));
        return new Predicate<ResultColumn>() {
            @Override
            public boolean apply(@Nullable ResultColumn rc) {
                return rc !=null && rsns.contains(rc.getResultSetNumber());
            }
        };
    }

    public static final Function<ValueNode,ResultColumn> refToRC = new Function<ValueNode, ResultColumn>() {
        @Override
        public ResultColumn apply(@Nullable ValueNode vn) {
            if (vn instanceof ColumnReference) {
                ColumnReference cr = (ColumnReference) vn;
                return cr.getSource();
            } else if (vn instanceof VirtualColumnNode) {
                VirtualColumnNode vcn = (VirtualColumnNode) vn;
                return vcn.getSourceColumn();
            }
            return null;
        }
    };

    public static Predicate<ValueNode> refPointsTo(ResultSetNode rsn)
            throws StandardException {
        return Predicates.compose(pointsTo(rsn), refToRC);
    }

    public static AccessPath ap(JoinNode j){
         return ((Optimizable) j.getRightResultSet())
                 .getTrulyTheBestAccessPath();
    }

    public static Map<Class<?>,String> sinkingNames =
        ImmutableMap.<Class<?>, String>of(
			JoinNode.class, "join",
			HalfOuterJoinNode.class, "join",
			AggregateNode.class, "aggregate",
			DistinctNode.class, "distinct",
			OrderByNode.class, "sort");

    public final static Set<?> sinkers =
        ImmutableSet.of(
			JoinNode.class,
			HalfOuterJoinNode.class,
			AggregateNode.class,
			DistinctNode.class,
			OrderByNode.class);

    public final static Predicate<ResultSetNode> isSinkingNode = new Predicate<ResultSetNode>(){
        @Override
        public boolean apply(ResultSetNode rsn) {
            return sinkers.contains(rsn.getClass()) &&
            	(!(rsn instanceof JoinNode) || RSUtils.isSinkingJoin(RSUtils.ap((JoinNode)rsn)));
        }
    };

    public static Iterable<ResultSetNode> sinkingChildren(ResultSetNode node)
        throws StandardException {
        return Iterables.filter(RSUtils.getSelfAndDescendants(node), isSinkingNode);
    }

    public static boolean hasSinkingChildren(ResultSetNode node)
        throws StandardException {
        List<ResultSetNode> sinks = Lists.newLinkedList(sinkingChildren(node));
        return (sinks != null && sinks.size() > 0);
    }
}
