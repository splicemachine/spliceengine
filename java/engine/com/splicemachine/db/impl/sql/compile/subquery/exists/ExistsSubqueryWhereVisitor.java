package com.splicemachine.db.impl.sql.compile.subquery.exists;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.ColumnUtils;
import com.splicemachine.db.impl.sql.compile.AndNode;
import com.splicemachine.db.impl.sql.compile.BinaryRelationalOperatorNode;
import com.splicemachine.db.impl.sql.compile.OrNode;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import com.splicemachine.db.impl.sql.compile.subquery.CorrelatedBronPredicate;
import com.splicemachine.db.impl.sql.compile.subquery.CorrelatedEqualityBronPredicate;
import org.apache.log4j.Logger;

/**
 * Determines if the where clause of an exists subquery is one that we can flatten.
 *
 * EXAMPLE: Why can flatten exists but not not-exists subqueries with type C predicates?  Consider columns A.a1 and B.b1
 * where A.a1 contains [1,9] and B.b1 contains [1,8]:
 *
 * <pre>
 *
 * this         : select a1 from A where EXISTS (select 1 from B where a1=b1 and a1=5);  -- returns '5'
 * equivalent to: select a1 from A where a1=5 and EXISTS (select 1 from B where a1=b1);  -- returns '5'
 *
 * however for NOT-EXISTS
 *
 * this             : select a1 from A where NOT EXISTS (select 1 from B where a1=b1 and a1=5);  -- all in A but '5'
 * NOT equivalent to: select a1 from A where a1!=5 and NOT EXISTS (select 1 from B where a1=b1); -- returns '9'
 *
 * </pre>
 */
class ExistsSubqueryWhereVisitor implements Visitor {

    private static Logger LOG = Logger.getLogger(ExistsSubqueryWhereVisitor.class);

    /* We flatten exists subqueries with either type of BRON */
    private final CorrelatedEqualityBronPredicate correlatedEqualityBronPredicate;
    /* For EXISTS subqueries we can move this type of predicate up one level (but not for NOT EXISTS subqueries). */
    private final CorrelatedBronPredicate correlatedBronPredicate;

    private final boolean isNotExistsSubquery;

    private boolean foundUnsupported;


    /**
     * @param subqueryLevel       The level of the subquery we are considering flattening in the enclosing predicate
     * @param isNotExistsSubquery Are we looking at a NOT EXISTS subquery.
     */
    public ExistsSubqueryWhereVisitor(int subqueryLevel, boolean isNotExistsSubquery) {
        this.isNotExistsSubquery = isNotExistsSubquery;
        this.correlatedEqualityBronPredicate = new CorrelatedEqualityBronPredicate(subqueryLevel - 1);
        this.correlatedBronPredicate = new CorrelatedBronPredicate(subqueryLevel - 1);
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (node instanceof AndNode) {
            /* do nothing */
        } else if (node instanceof BinaryRelationalOperatorNode) {
            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode) node;

            /* We can NOT move this type of predicate from a NOT EXISTS subquery, thus subquery cannot be flattened. */
            boolean correlatedBron = correlatedBronPredicate.apply(bron);
            if (isNotExistsSubquery && correlatedBron) {
                foundUnsupported = true;
                return node;
            }

            if (!(correlatedEqualityBronPredicate.apply(bron) || correlatedBron)) {
                foundUnsupported = ColumnUtils.isSubtreeCorrelated(node);
            }
        } else {
            /* Current node is not an AndNode or BinaryRelationalOperatorNode -- ok as long as there are no
             * correlated predicates belows us, we can move/flatten any type of uncorrelated subtree */
            foundUnsupported = node instanceof OrNode || ColumnUtils.isSubtreeCorrelated(node);
        }
        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        boolean isAndNode = node instanceof AndNode;
        return !isAndNode;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return foundUnsupported;
    }

    public boolean isFoundUnsupported() {
        return foundUnsupported;
    }

}