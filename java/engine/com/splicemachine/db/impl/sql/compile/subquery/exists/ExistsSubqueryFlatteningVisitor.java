package com.splicemachine.db.impl.sql.compile.subquery.exists;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.AbstractSpliceVisitor;
import com.splicemachine.db.impl.ast.PredicateUtils;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.subquery.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;


/**
 *
 */
public class ExistsSubqueryFlatteningVisitor extends AbstractSpliceVisitor implements Visitor {

    private static Logger LOG = Logger.getLogger(ExistsSubqueryFlatteningVisitor.class);

    private int flattenedSubquery = 1;

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

        /**
         * Stop if this is not a select node
         */
        if (!(node instanceof SelectNode)) {
            return node;
        }
        SelectNode topSelectNode = (SelectNode) node;

        /**
         * Stop if there are no subqueries.
         */
        if (topSelectNode.getWhereSubquerys() == null || topSelectNode.getWhereSubquerys().isEmpty()) {
            return node;
        }

        /*
         * Stop if there are no subqueries we handle.
         */
        List<SubqueryNode> subqueryList = topSelectNode.getWhereSubquerys().getNodes();
        List<SubqueryNode> handledSubqueryList = Lists.newArrayList(Iterables.filter(subqueryList, new ExistsSubqueryPredicate()));
        if (handledSubqueryList.isEmpty()) {
            return node;
        }

        /*
         * Flatten where applicable.
         */
        CompilerContext cpt = topSelectNode.getCompilerContext();
        cpt.setNumTables(cpt.getNumTables() + handledSubqueryList.size());

        for (SubqueryNode subqueryNode : handledSubqueryList) {
            flatten(topSelectNode, subqueryNode);
        }

        /*
         * Finally remove the flattened subquery nodes from the top select node.
         */
        for (SubqueryNode subqueryNode : handledSubqueryList) {
            topSelectNode.getWhereSubquerys().removeElement(subqueryNode);
        }

        return node;
    }

    /**
     * Perform the actual flattening (we start to mutate the tree at this point).
     */
    private void flatten(SelectNode topSelectNode,
                         SubqueryNode subqueryNode) throws StandardException {

        ResultSetNode subqueryResultSet = subqueryNode.getResultSet();
        SelectNode subquerySelectNode = (SelectNode) subqueryResultSet;

        /**
         * The following lines collect correlated predicates from the subquery where clause while removing them.
         */
        ValueNode subqueryWhereClause = subquerySelectNode.getWhereClause();
        List<BinaryRelationalOperatorNode> correlatedSubqueryPreds = new ArrayList<>();
        subqueryWhereClause = FlatteningUtils.findCorrelatedSubqueryPredicates(subqueryWhereClause, correlatedSubqueryPreds, topSelectNode.getNestingLevel());
        subquerySelectNode.setWhereClause(subqueryWhereClause);
        subquerySelectNode.setOriginalWhereClause(subqueryWhereClause);

        int originalSubqueryResultColSize = subquerySelectNode.getResultColumns().size();

        /*
         * For each correlated predicate generate a GroupByColumn
         */
        GroupByUtil.addGroupByNodes(topSelectNode, subquerySelectNode, correlatedSubqueryPreds);

        ResultColumnList newRcl = subquerySelectNode.getResultColumns().copyListAndObjects();
        newRcl.genVirtualColumnNodes(subquerySelectNode, subquerySelectNode.getResultColumns());

        FromSubquery fromSubquery = (FromSubquery) subquerySelectNode.getNodeFactory().getNode(C_NodeTypes.FROM_SUBQUERY,
                subqueryResultSet,
                subqueryNode.getOrderByList(),
                subqueryNode.getOffset(),
                subqueryNode.getFetchFirst(),
                subqueryNode.hasJDBClimitClause(),
                getSubqueryAlias(),
                newRcl,
                null,
                topSelectNode.getContextManager());

        /*
         * Insert the new FromSubquery into to origSelectNode's From list.
         */
        fromSubquery.changeTableNumber(topSelectNode.getCompilerContext().getNextTableNumber());
        topSelectNode.getFromList().addFromTable(fromSubquery);


        /*
         * Add correlated predicates from subquery to outer query where clause.
         */
        ValueNode newTopWhereClause = SubqueryReplacement.replace(topSelectNode.getWhereClause(), fromSubquery);
        for (int i = 0; i < correlatedSubqueryPreds.size(); i++) {
            BinaryRelationalOperatorNode pred = correlatedSubqueryPreds.get(i);

            int fromSubqueryColIndex = i + originalSubqueryResultColSize;
            ColumnReference colRef = FromSubqueryColRefFactory.build(topSelectNode.getNestingLevel(), fromSubquery,
                    fromSubqueryColIndex, topSelectNode.getNodeFactory(), topSelectNode.getContextManager());

            /*
             * Modify one side of the predicate to contain the new FromSubquery ref.
             */
            ColumnReference leftOperand = (ColumnReference) pred.getLeftOperand();
            int subqueryNestingLevel = topSelectNode.getNestingLevel() + 1;
            if (PredicateUtils.isLeftColRef(pred, subqueryNestingLevel)) {
                pred.setLeftOperand(pred.getRightOperand());
                leftOperand.setNestingLevel(leftOperand.getSourceLevel());
                pred.setRightOperand(colRef);
            } else if (PredicateUtils.isRightColRef(pred, subqueryNestingLevel)) {
                leftOperand.setNestingLevel(leftOperand.getSourceLevel());
                pred.setRightOperand(colRef);
            }

            /*
             * Finally add the predicate to the outer query.
             */
            newTopWhereClause = FlatteningUtils.addPredToTree(newTopWhereClause, pred);
        }

        topSelectNode.setOriginalWhereClause(newTopWhereClause);
        topSelectNode.setWhereClause(newTopWhereClause);
    }

    private String getSubqueryAlias() {
        return "ExistsFlattenedSubquery" + flattenedSubquery++;
    }
}