package com.splicemachine.db.impl.sql.compile.subquery.exists;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.ColumnUtils;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.subquery.CorrelatedBronPredicate;
import com.splicemachine.db.impl.sql.compile.subquery.CorrelatedEqualityBronPredicate;
import com.splicemachine.db.impl.sql.compile.subquery.FlatteningUtils;
import org.apache.log4j.Logger;

import java.util.List;

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
    private final CorrelatedEqualityBronPredicate typeDPredicate;
    /* For EXISTS subqueries we can move this type of predicate up one level (but not for NOT EXISTS subqueries). */
    private final CorrelatedBronPredicate typeCPredicate;

    private final boolean isNotExistsSubquery;

    /* If true indicates that we found something in the subquery where clause that cannot be flattened in any case. */
    private boolean foundUnsupported;

    /* For UNION subqueries we only flatten if typeDCount = 1 and typeCCount = 0 */
    private int typeCCount;

    private List<ColumnReference> typeDCorrelatedColumnReference = Lists.newArrayList();

    private int outerNestingLevel;

    /**
     * @param subqueryLevel       The level of the subquery we are considering flattening in the enclosing predicate
     * @param isNotExistsSubquery Are we looking at a NOT EXISTS subquery.
     */
    public ExistsSubqueryWhereVisitor(int subqueryLevel, boolean isNotExistsSubquery) {
        this.isNotExistsSubquery = isNotExistsSubquery;
        this.outerNestingLevel = subqueryLevel - 1;
        this.typeDPredicate = new CorrelatedEqualityBronPredicate(this.outerNestingLevel);
        this.typeCPredicate = new CorrelatedBronPredicate(this.outerNestingLevel);
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (node instanceof AndNode) {
            /* do nothing */
        } else if (node instanceof BinaryRelationalOperatorNode) {
            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode) node;

            boolean correlatedBron = typeCPredicate.apply(bron);

            /*
             * CASE 1: a1 = constant;  where a1 is a result col from the outer query.  If this is a NOT-EXISTS then the
             * predicate cannot be moved and this subquery cannot be flattened.  For EXISTS subqueries it is ok.
             */
            if (correlatedBron) {
                typeCCount++;
                foundUnsupported = isNotExistsSubquery;
                return node;
            }

            /*
             * CASE 2: a1 = b1; where one CR is at subquery level and one from outer query.
             */
            if (typeDPredicate.apply(bron)) {
                typeDCorrelatedColumnReference.add(FlatteningUtils.findColumnReference(bron, outerNestingLevel ));
                return node;
            }

            /*
             * CASE 3: anything else is OK as long as not correlated.
             */
            foundUnsupported = ColumnUtils.isSubtreeCorrelated(node);

        } else {
            /* Current node is not an AndNode or BinaryRelationalOperatorNode -- ok as long as there are no
             * correlated predicates belows us, we can move/flatten any type of uncorrelated subtree */
            foundUnsupported = node instanceof OrNode || ColumnUtils.isSubtreeCorrelated(node);
        }
        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        // AndNode -> skipChildren = false
        // Else    -> skipChildren = true
        return !(node instanceof AndNode);
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

    public List<ColumnReference> getTypeDCorrelatedColumnReference() {
        return typeDCorrelatedColumnReference;
    }

    public int getTypeCCount() {
        return typeCCount;
    }
}