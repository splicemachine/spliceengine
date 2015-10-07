package com.splicemachine.db.impl.sql.compile.subquery.exists;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.AbstractSpliceVisitor;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.subquery.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * The strategy for the four types of exists subqueries this class will flatten:
 *
 * <pre>
 * CORRELATED EXISTS:
 * ------------------
 *
 * FROM: select * from A where exists (select 1 from B where a1=b1).
 * TO  : select * from A, (select b1 from B group by b1) foo where a1=foo.b1;
 *
 * UN-CORRELATED EXISTS:
 * ---------------------
 *
 * FROM: select * from A where exists (select 1 from B where b1 > 100).
 * TO  : select * from A ,(select 1 from B where b1 > 100 group by 1) foo where a1=foo.b1;
 *
 * CORRELATED NOT-EXISTS:
 * ----------------------
 *
 * FROM: select * from A where not exists (select 1 from B where a1=b1).
 * TO  : select * from A left join (select b1 from B group by b1) foo on a1=foo.b1 where foo.b1 is null;
 *
 * UN-CORRELATED NOT-EXISTS:
 * ------------------------
 *
 * FROM: select * from A where not exists (select 1 from B).
 * TO  : select * from A left join (select 1 bar from B group by 1) foo on TRUE where bar is null;
 * </pre>
 *
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 *
 * Four types of predicates we consider in the context of exists/not-exists subquery flattening.  Documents here will
 * refer to these by name "type C predicates", for example.
 *
 * <pre>
 *
 * select A.* from A where
 *   exists( select 1 from B where a1=b1 and
 *      exists ( select 1 from C where b1=c1 and c1 > 100 and c1=a1 and b1=100)
 *
 * (A) Uncorrelated.  Example c1 > 100
 * (B) Correlated spanning multi level.  Example c1=a1.  Subqueries with these are not currently flattened.
 * (C) Correlated spanning 1 level, referencing 1 level.  Example b1=100.  We move these as part of flattening.
 * (D) Correlated spanning 1 level, referencing 2 levels. Examples a1=b1, b1=c1. Become join criteria in flattening.
 *
 * </pre>
 */
public class ExistsSubqueryFlatteningVisitor extends AbstractSpliceVisitor implements Visitor {

    private static Logger LOG = Logger.getLogger(ExistsSubqueryFlatteningVisitor.class);

    private final int originalNestingLevel;
    private int flattenedCount = 0;
    private SubqueryNodeFactory nf;

    public ExistsSubqueryFlatteningVisitor(int originalNestingLevel) {
        this.originalNestingLevel = originalNestingLevel;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return false;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {

        /*
         * Stop if this is not a select node
         */
        if (!(node instanceof SelectNode)) {
            return node;
        }
        SelectNode topSelectNode = (SelectNode) node;

        /*
         * Stop if there are no subqueries.
         */
        if (topSelectNode.getWhereSubquerys() == null || topSelectNode.getWhereSubquerys().isEmpty()) {
            return node;
        }

        /*
         * Normalize the outer select node.  This performs a portion of the work we normally don't do until
         * the preprocess phase (first part of optimize phase really) but we need our outer query to be in CNF with not
         * elimination performed.
         */
        topSelectNode.setWhereClause(SelectNode.normExpressions(topSelectNode.getWhereClause()));

        /*
         * Stop if there are no subqueries we handle.
         */
        List<SubqueryNode> subqueryList = topSelectNode.getWhereSubquerys().getNodes();
        List<SubqueryNode> handledSubqueryList = Lists.newArrayList(Iterables.filter(subqueryList, new ExistsSubqueryPredicate(topSelectNode)));
        if (handledSubqueryList.isEmpty()) {
            return node;
        }

        /*
         * Flatten where applicable.
         *
         * We must do NOT-exists flattening first because of the current limitation that we can only flatten
         * NOT-EXISTS into a single table outer FromList.  We can flatten multiple NOT-EXISTS however because each one
         * just puts a new join node in the outer from list. After that is complete we can flatten EXISTS or other
         * subquery types without limitation.
         */
        CompilerContext cpt = topSelectNode.getCompilerContext();
        cpt.setNumTables(cpt.getNumTables() + handledSubqueryList.size());
        nf = new SubqueryNodeFactory(topSelectNode.getContextManager(), topSelectNode.getNodeFactory());

        Collections.sort(handledSubqueryList, new SubqueryComparator());
        for (SubqueryNode subqueryNode : handledSubqueryList) {
            flatten(topSelectNode, subqueryNode);
        }

        /*
         * Finally remove the flattened subquery nodes from the outer select node.
         */
        for (SubqueryNode subqueryNode : handledSubqueryList) {
            topSelectNode.getWhereSubquerys().removeElement(subqueryNode);
        }

        return node;
    }

    /**
     * Perform the actual flattening (we start to mutate the tree at this point).
     */
    private void flatten(SelectNode outerSelectNode,
                         SubqueryNode subqueryNode) throws StandardException {

        /*
         * Misc variables used throughout.
         */
        ResultSetNode subqueryResultSet = subqueryNode.getResultSet();
        SelectNode subquerySelectNode = (SelectNode) subqueryResultSet;
        NodeFactory nodeFactory = subquerySelectNode.getNodeFactory();
        ContextManager contextManager = outerSelectNode.getContextManager();
        int outerNestingLevel = outerSelectNode.getNestingLevel();

        /*
         * The following lines collect two types of correlated predicates from the subquery where clause while removing
         * them.
         */
        ValueNode subqueryWhereClause = subquerySelectNode.getWhereClause();
        List<BinaryRelationalOperatorNode> correlatedSubqueryPredsD = new ArrayList<>();
        List<BinaryRelationalOperatorNode> correlatedSubqueryPredsC = new ArrayList<>();
        subqueryWhereClause = FlatteningUtils.findCorrelatedSubqueryPredicates(subqueryWhereClause, correlatedSubqueryPredsD, new CorrelatedEqualityBronPredicate(outerNestingLevel));
        subqueryWhereClause = FlatteningUtils.findCorrelatedSubqueryPredicates(subqueryWhereClause, correlatedSubqueryPredsC, new CorrelatedBronPredicate(outerNestingLevel));
        subquerySelectNode.setWhereClause(subqueryWhereClause);
        subquerySelectNode.setOriginalWhereClause(subqueryWhereClause);

        /*
         * For each correlated predicate generate a GroupByColumn.  We add the GroupByColumn to the subquery result
         * columns so that it can be referenced by the outer query.  For EXISTS and NOT-EXISTS we don't care about the
         * subquery's original result columns so we clear them [ where exists (select a,b,c...) is same as
         * where exists (select 1 ... )].
         */
        subquerySelectNode.getResultColumns().getNodes().clear();
        GroupByUtil.addGroupByNodes(outerSelectNode, subquerySelectNode, correlatedSubqueryPredsD);

        /*
         * Build the new FromSubquery and insert it into outer select.
         */
        FromSubquery fromSubquery = addFromSubquery(
                outerSelectNode,
                subqueryNode,
                subquerySelectNode,
                subqueryResultSet,
                correlatedSubqueryPredsD, nodeFactory, contextManager);

        /*
         * Add correlated predicates from subquery to outer query where clause.
         */
        outerSelectNode.setWhereClause(SubqueryReplacement.replaceSubqueryWithTrue(outerSelectNode.getWhereClause(), fromSubquery));

        for (int i = 0; i < correlatedSubqueryPredsD.size(); i++) {
            BinaryRelationalOperatorNode pred = correlatedSubqueryPredsD.get(i);

            ColumnReference colRef = FromSubqueryColRefFactory.build(outerNestingLevel, fromSubquery,
                    i, outerSelectNode.getNodeFactory(), contextManager);

            FromSubqueryColRefFactory.replace(pred, colRef, outerNestingLevel + 1);


            /*
             * Finally add the predicate to the outer query.
             */
            FlatteningUtils.deCorrelate(pred);
            outerSelectNode.setWhereClause(FlatteningUtils.addPredToTree(outerSelectNode.getWhereClause(), pred));
        }

        /*
         * Move type C correlated preds to outer where clause.  NOT-EXISTS subqueries with type C preds are NOT flattened.
         */
        for (BinaryRelationalOperatorNode pred : correlatedSubqueryPredsC) {
            FlatteningUtils.deCorrelate(pred);
            outerSelectNode.setWhereClause(FlatteningUtils.addPredToTree(outerSelectNode.getWhereClause(), pred));
        }

        /* Update 'Original' where clause property -- not sure if this is necessary */
        outerSelectNode.setOriginalWhereClause(outerSelectNode.getWhereClause());
    }

    /**
     * Create a new FromSubquery node and add it to the outer query.  This method contains most of the logic that is
     * specific to either EXISTS or NOT EXISTS.
     */
    private FromSubquery addFromSubquery(SelectNode outerSelectNode,
                                         SubqueryNode subqueryNode,
                                         SelectNode subquerySelectNode,
                                         ResultSetNode subqueryResultSet,
                                         List<BinaryRelationalOperatorNode> correlatedSubqueryPredsD,
                                         NodeFactory nodeFactory,
                                         ContextManager contextManager) throws StandardException {

        /*
         * New FromSubquery
         */
        ResultColumnList newRcl = subquerySelectNode.getResultColumns().copyListAndObjects();
        newRcl.genVirtualColumnNodes(subquerySelectNode, subquerySelectNode.getResultColumns());
        FromSubquery fromSubquery = nf.buildFromSubqueryNode(outerSelectNode, subqueryNode, subqueryResultSet, newRcl, getSubqueryAlias());
        int outerNestingLevel = outerSelectNode.getNestingLevel();

        /*
         * For EXISTS subqueries just add the FromSubquery to the outer query and we are done.
         */
        if (!subqueryNode.isNOT_EXISTS()) {
            outerSelectNode.getFromList().addFromTable(fromSubquery);
        }

        /*
         * For NOT-EXISTS subqueries we do more work.
         */
        else {

            /*
             * Create a JOIN clause that is And-TRUE or contains subquery predicates, depending on if the subquery is
             * correlated.
             */
            ValueNode joinClause = nf.buildAndNode();
            if (correlatedSubqueryPredsD.isEmpty()) {
                /*
                 * Join clause -- in uncorrelated case is left join (from subquery) on TRUE
                 */
                joinClause = nf.buildBooleanTrue();
                /*
                 * Add right-side IS NULL predicate to outer query.
                 */
                ColumnReference isNullColRef = FromSubqueryColRefFactory.build(outerNestingLevel, fromSubquery, 0, nodeFactory, contextManager);
                IsNullNode rightIsNull = nf.buildIsNullNode(isNullColRef);
                outerSelectNode.setWhereClause(FlatteningUtils.addPredToTree(outerSelectNode.getWhereClause(), rightIsNull));

            } else {
                for (int i = 0; i < correlatedSubqueryPredsD.size(); i++) {
                    /*
                     * Join clause -- add to join clause for each correlated predicate.
                     */
                    BinaryRelationalOperatorNode pred = correlatedSubqueryPredsD.get(i);
                    ColumnReference joinColRef = FromSubqueryColRefFactory.build(outerNestingLevel, fromSubquery, i, nodeFactory, contextManager);
                    FromSubqueryColRefFactory.replace(pred, joinColRef, outerNestingLevel + 1);
                    FlatteningUtils.addPredToTree(joinClause, pred);
                    /*
                     * Add right-side IS NULL predicate to outer query.
                     */
                    ColumnReference isNullColRef = FromSubqueryColRefFactory.build(outerNestingLevel, fromSubquery, i, nodeFactory, contextManager);
                    IsNullNode rightIsNull = nf.buildIsNullNode(isNullColRef);
                    outerSelectNode.setWhereClause(FlatteningUtils.addPredToTree(outerSelectNode.getWhereClause(), rightIsNull));
                }
            }


            /*
             * Left join outer tables with new FromSubquery.
             */
            HalfOuterJoinNode outerJoinNode = nf.buildOuterJoinNode(outerSelectNode.getFromList(), fromSubquery, joinClause);

            /*
             * Clear the predicates in the case of NOT-EXISTS so that they are ONLY added to the new join clause and
             * not the outer select's where clause.
             */
            correlatedSubqueryPredsD.clear();

            /*
             * Insert the new FromSubquery into to origSelectNode's From list.
             */
            outerSelectNode.getFromList().getNodes().clear();
            outerSelectNode.getFromList().addFromTable(outerJoinNode);
        }
        return fromSubquery;
    }

    private String getSubqueryAlias() {
        return String.format("ExistsFlatSub-%s-%s", originalNestingLevel, ++flattenedCount);
    }

}