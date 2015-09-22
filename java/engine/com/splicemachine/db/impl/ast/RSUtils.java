package com.splicemachine.db.impl.ast;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.*;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.AccessPath;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicate;
import com.splicemachine.db.iapi.sql.compile.Visitable;


/**
 * Utilities for Derby's ResultSetNodes
 *
 * @author P Trolard Date: 18/10/2013
 */
public class RSUtils {

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // functions
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public static final Function<ResultSetNode, Integer> rsNum = new Function<ResultSetNode, Integer>() {
        @Override
        public Integer apply(ResultSetNode rsn) {
            return rsn.getResultSetNumber();
        }
    };

    public static final Function<Object, Class<?>> classOf = new Function<Object, Class<?>>() {
        @Override
        public Class<?> apply(Object input) {
            return input == null ? null : input.getClass();
        }
    };

    public static final Function<ValueNode, ResultColumn> refToRC = new Function<ValueNode, ResultColumn>() {
        @Override
        public ResultColumn apply(ValueNode vn) {
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

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // predicates
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public static final Predicate<Object> isRSN = Predicates.instanceOf(ResultSetNode.class);

    public static final Predicate<ResultSetNode> rsnHasPreds =
            Predicates.or(Predicates.instanceOf(ProjectRestrictNode.class), Predicates.instanceOf(FromBaseTable.class),
                    Predicates.instanceOf(IndexToBaseRowNode.class));


    public final static Predicate<ResultSetNode> isSinkingNode = new Predicate<ResultSetNode>() {
        @Override
        public boolean apply(ResultSetNode rsn) {
            return sinkers.contains(rsn.getClass()) &&
                    (!(rsn instanceof JoinNode) || RSUtils.isSinkingJoin(RSUtils.ap((JoinNode) rsn)));
        }
    };

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // collections
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public static final Set<?> binaryRSNs = ImmutableSet.of(
            JoinNode.class,
            HalfOuterJoinNode.class,
            UnionNode.class,
            IntersectOrExceptNode.class);

    public static final Predicate<Object> isBinaryRSN =
            Predicates.compose(Predicates.in(binaryRSNs), classOf);

    // leafRSNs might need VTI eventually
    public static final Set<?> leafRSNs = ImmutableSet.of(
            FromBaseTable.class,
            RowResultSetNode.class);

    public static Map<Class<?>, String> sinkingNames =
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

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // methods
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * Return any instances of clazz at or below node
     */
    public static <N> List<N> collectNodes(Visitable node, Class<N> clazz) throws StandardException {
        return CollectNodes.collector(clazz).collect(node);
    }

    /**
     * Visit the node in question and all descendants of the node until the specified predicate evaluates to true.
     * Note that if the predicate evaluates to true for the passed top node parameter then no nodes will be visited.
     */
    public static <N> List<N> collectNodesUntil(Visitable node, Class<N> clazz,
                                                Predicate<? super Visitable> pred) throws StandardException {
        return CollectNodes.collector(clazz).until(pred).collect(node);
    }

    public static <N> List<N> collectExpressionNodes(ResultSetNode node, Class<N> clazz) throws StandardException {
        // define traversal axis to be the node itself (so we can get to its descendants) or,
        // our real target, non-ResultSetNodes
        Predicate<Object> onAxis = Predicates.or(Predicates.equalTo((Object) node), Predicates.not(isRSN));
        return CollectNodes.collector(clazz).onAxis(onAxis).collect(node);
    }

    /**
     * Return list of node and its ResultSetNode descendants, as returned by depth-first, pre-order traversal. Does not
     * descend into expression nodes (therefore doesn't consider ResultSetNodes in subqueries descendants).
     */
    public static List<ResultSetNode> getSelfAndDescendants(ResultSetNode rsn) throws StandardException {
        return CollectNodes.collector(ResultSetNode.class).onAxis(isRSN).collect(rsn);
    }

    /**
     * Return immediate (ResultSetNode) children of node
     */
    public static List<ResultSetNode> getChildren(ResultSetNode node) throws StandardException {
        Predicate<Object> self = Predicates.equalTo((Object) node);
        Predicate<Object> notSelfButRS = Predicates.and(Predicates.not(self), isRSN);
        return CollectNodes.<ResultSetNode>collector(notSelfButRS)
                .onAxis(self)
                .collect(node);
    }

    public static List<ResultSetNode> nodesUntilBinaryNode(ResultSetNode rsn) throws StandardException {
        return CollectNodes.collector(ResultSetNode.class)
                .onAxis(isRSN)
                .until(isBinaryRSN)
                .collect(rsn);
    }

    /**
     * Returns the leaves for a query plan subtree
     */
    public static List<ResultSetNode> getLeafNodes(ResultSetNode rsn) throws StandardException {
        List<ResultSetNode> rsns = getSelfAndDescendants(rsn);
        List<ResultSetNode> leaves = new LinkedList<>();
        for (ResultSetNode r : rsns) {
            if (leafRSNs.contains(r.getClass())) {
                leaves.add(r);
            }
        }
        return leaves;
    }

    /**
     * CAUTION: This method modifies the FromBaseTable parameter.
     */
    public static PredicateList getPreds(FromBaseTable t) throws StandardException {
        PredicateList pl = new PredicateList();
        t.pullOptPredicates(pl);
        for (int i = 0, s = pl.size(); i < s; i++) {
            OptimizablePredicate p = pl.getOptPredicate(i);
            t.pushOptPredicate(p);
        }
        PredicateList storeRestrictionList = t.storeRestrictionList;
        for (int i = 0; i < storeRestrictionList.size(); ++i) {
            OptimizablePredicate pred = storeRestrictionList.getOptPredicate(i);
            if (!contains(pl, pred)) {
                pl.addOptPredicate(pred);
            }
        }
        return pl;
    }

    private static boolean contains(PredicateList pl, OptimizablePredicate pred) {
        for (int i = 0; i < pl.size(); ++i) {
            OptimizablePredicate p = pl.getOptPredicate(i);
            if (p == pred) {
                return true;
            }
        }
        return false;
    }

    public static PredicateList getPreds(ProjectRestrictNode pr) throws StandardException {
        return pr.restrictionList != null ? pr.restrictionList : new PredicateList();
    }

    public static PredicateList getPreds(IndexToBaseRowNode in) throws StandardException {
        return in.restrictionList != null ? in.restrictionList : new PredicateList();
    }

    public static boolean isMSJ(AccessPath ap) {
        return (ap != null && ap.getJoinStrategy().getJoinStrategyType().equals(JoinStrategy.JoinStrategyType.MERGE_SORT));
    }

    public static boolean isNLJ(AccessPath ap) {
        return (ap != null && ap.getJoinStrategy().getJoinStrategyType().equals(JoinStrategy.JoinStrategyType.NESTED_LOOP));
    }

    public static boolean isHashableJoin(AccessPath ap) {
        if (ap == null) return false;
        JoinStrategy strategy = ap.getJoinStrategy();
        return strategy instanceof HashableJoinStrategy;
    }

    public static boolean isSinkingJoin(AccessPath ap) {
        return isMSJ(ap);
    }

    public static Predicate<ResultColumn> pointsTo(ResultSetNode rsn) throws StandardException {
        final Set<Integer> rsns = Sets.newHashSet(Iterables.transform(getSelfAndDescendants(rsn), rsNum));
        return new Predicate<ResultColumn>() {
            @Override
            public boolean apply(ResultColumn rc) {
                return rc != null && rsns.contains(rc.getResultSetNumber());
            }
        };
    }

    public static Predicate<ValueNode> refPointsTo(ResultSetNode rsn) throws StandardException {
        return Predicates.compose(pointsTo(rsn), refToRC);
    }

    public static AccessPath ap(JoinNode j) {
        return ((Optimizable) j.getRightResultSet()).getTrulyTheBestAccessPath();
    }

    public static Iterable<ResultSetNode> sinkingChildren(ResultSetNode node) throws StandardException {
        return Iterables.filter(RSUtils.getSelfAndDescendants(node), isSinkingNode);
    }

    public static boolean hasSinkingChildren(ResultSetNode node) throws StandardException {
        List<ResultSetNode> sinks = Lists.newLinkedList(sinkingChildren(node));
        return (sinks != null && sinks.size() > 0);
    }
}
