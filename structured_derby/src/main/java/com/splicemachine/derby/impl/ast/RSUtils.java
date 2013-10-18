package com.splicemachine.derby.impl.ast;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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
        CollectNodesVisitor v = new CollectNodesVisitor(clazz);
        node.accept(v);
        return (List<N>) v.getList();
    }

    /**
     * Return list of node and its ResultSetNode descendants, as returned by depth-first, pre-order traversal
     */
    public static List<ResultSetNode> getSelfAndDescendants(ResultSetNode rsn) throws StandardException {
        return collectNodes(rsn, ResultSetNode.class);
    }

    /**
     * Return immediate (ResultSetNode) children of node
     */
    public static List<ResultSetNode> getChildren(ResultSetNode node)
            throws StandardException {
        CollectChildrenVisitor v = new CollectChildrenVisitor();
        node.accept(v);
        return v.getChildren();
    }

    public static Set binaryRSNs = ImmutableSet.of(JoinNode.class, HalfOuterJoinNode.class,
            UnionNode.class, IntersectOrExceptNode.class);

    public static Set leafRSNs = ImmutableSet.of(FromBaseTable.class, RowResultSetNode.class);

    /**
     * If rsn subtree contains a node with 2 children, return the node above
     * it, else return the leaf node
     */
    public static ResultSetNode getLastNonBinaryNode(ResultSetNode rsn) throws StandardException {
        List<ResultSetNode> rsns = getSelfAndDescendants(rsn);
        for (List<ResultSetNode> pair : Iterables.paddedPartition(rsns, 2)) {
            if (pair.get(1) != null && binaryRSNs.contains(pair.get(1).getClass())) {
                return pair.get(0);
            }
        }
        return rsns.get(rsns.size() - 1);
    }

    /**
     * Returns the leaves for a query plan subtree
     */
    public static List<ResultSetNode> getLeafNodes(ResultSetNode rsn)
            throws StandardException {
        List<ResultSetNode> rsns = getSelfAndDescendants(rsn);
        List<ResultSetNode> leaves = new LinkedList<ResultSetNode>();
        for (ResultSetNode r : rsns) {
            if (leafRSNs.contains(r.getClass())) {
                leaves.add(r);
            }
        }
        return leaves;
    }

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


}
