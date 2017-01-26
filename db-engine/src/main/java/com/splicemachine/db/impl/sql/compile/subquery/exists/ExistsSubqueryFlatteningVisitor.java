/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile.subquery.exists;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.AbstractSpliceVisitor;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.subquery.*;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Lists;

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

    public static final String EXISTS_TABLE_ALIAS_PREFIX = "ExistsFlatSubquery-";

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
        int subqueryNestingLevel = outerNestingLevel + 1;

        List<SelectNode> selectNodeList = Lists.newArrayList();
        List<UnionNode> unionNodeList = FlatteningUtils.findSameLevelUnionNodes(subqueryNode);
        if (!unionNodeList.isEmpty()) {
            List<SelectNode> selectsInUnion = FlatteningUtils.findSameLevelSelectNodes(subqueryNode);
            selectNodeList.addAll(selectsInUnion);
            /* Annoyingly derby further increments the nesting level for selects in a union subquery.  If the
             * outer query is nesting level 0 then the subquery select will be nesting level 1 and will have as
             * descendants a tree of UnionNode/SelectNode all at level 2 */
            subqueryNestingLevel = subqueryNestingLevel + 1;
        } else {
            selectNodeList.add(subquerySelectNode);
        }

        List<BinaryRelationalOperatorNode> correlatedSubqueryPredsC = new ArrayList<>();
        List<BinaryRelationalOperatorNode> correlatedSubqueryPredsD = new ArrayList<>();

        /**
         * We do the following logic either for the SelectNode directly under the SubqueryNode or for each SelectNode
         * in a union.
         */
        for (SelectNode sn : selectNodeList) {

            /* Clear these each time.  If the subquery is NOT a union we will only iterate once.  If the subquery is
             * a union we only need one set of predicates (we have asserted in ExistsSubqueryPredicate that the
             * predicates of all union selects are symmetric). */
            correlatedSubqueryPredsC.clear();
            correlatedSubqueryPredsD.clear();

            /*
             * The following lines collect two types of correlated predicates from the subquery where clause while removing
             * them.
             */
            ValueNode subqueryWhereClause = sn.getWhereClause();
            subqueryWhereClause = FlatteningUtils.findCorrelatedSubqueryPredicates(subqueryWhereClause, correlatedSubqueryPredsC, new CorrelatedBronPredicate(outerNestingLevel));
            subqueryWhereClause = FlatteningUtils.findCorrelatedSubqueryPredicates(subqueryWhereClause, correlatedSubqueryPredsD, new CorrelatedEqualityBronPredicate(outerNestingLevel));
            sn.setWhereClause(subqueryWhereClause);
            sn.setOriginalWhereClause(subqueryWhereClause);

            /*
             * For each correlated predicate generate a GroupByColumn.  We add the GroupByColumn to the subquery result
             * columns so that it can be referenced by the outer query.  For EXISTS and NOT-EXISTS we don't care about the
             * subquery's original result columns so we clear them [ where exists (select a,b,c...) is same as
             * where exists (select 1 ... )].
             */
            sn.getResultColumns().getNodes().clear();
            GroupByUtil.addGroupByNodes(sn, correlatedSubqueryPredsD);

        }

        /* If there are Unions we have changed the RCs of every select below a union--regenerate the Union RCs. */
        for (UnionNode unionNode : unionNodeList) {
            unionNode.bindResultColumns(unionNode.getFromList());
        }

        /*
         * Build the new FromSubquery and insert it into outer select.
         */
        FromSubquery fromSubquery = addFromSubquery(
                outerSelectNode,
                subqueryNode.isEXISTS(),
                unionNodeList.isEmpty() ? subqueryResultSet : unionNodeList.get(0),
                subqueryNestingLevel,
                correlatedSubqueryPredsD, nodeFactory, contextManager);

        /*
         * Replace subquery with True in outer query.
         */
        outerSelectNode.setWhereClause(SubqueryReplacement.replaceSubqueryWithTrue(outerSelectNode.getWhereClause(), subqueryNode));

        /*
         * Add correlated predicates from subquery to outer query where clause.
         */
        for (int i = 0; i < correlatedSubqueryPredsD.size(); i++) {
            BinaryRelationalOperatorNode pred = correlatedSubqueryPredsD.get(i);

            ColumnReference colRef = FromSubqueryColRefFactory.build(outerNestingLevel, fromSubquery,
                    i, outerSelectNode.getNodeFactory(), contextManager);

            FromSubqueryColRefFactory.replace(pred, colRef, subqueryNestingLevel);


            /*
             * Finally add the predicate to the outer query.
             */
            FlatteningUtils.decrementColRefNestingLevel(pred);
            outerSelectNode.setWhereClause(FlatteningUtils.addPredToTree(outerSelectNode.getWhereClause(), pred));
        }

        /*
         * Move type C correlated preds to outer where clause.  NOT-EXISTS subqueries with type C preds are NOT flattened.
         */
        for (BinaryRelationalOperatorNode pred : correlatedSubqueryPredsC) {
            FlatteningUtils.decrementColRefNestingLevel(pred);
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
                                         boolean isExistsSubquery,
                                         ResultSetNode subqueryResultSet,
                                         int subqueryNestingLevel,
                                         List<BinaryRelationalOperatorNode> correlatedSubqueryPredsD,
                                         NodeFactory nodeFactory,
                                         ContextManager contextManager) throws StandardException {

        /*
         * New FromSubquery
         */
        ResultColumnList newRcl = subqueryResultSet.getResultColumns().copyListAndObjects();
        newRcl.genVirtualColumnNodes(subqueryResultSet, subqueryResultSet.getResultColumns());
        FromSubquery fromSubquery = nf.buildFromSubqueryNode(outerSelectNode, subqueryResultSet, newRcl, getSubqueryAlias());
        int outerNestingLevel = outerSelectNode.getNestingLevel();

        /*
         * For EXISTS subqueries just add the FromSubquery to the outer query and we are done.
         */
        if (isExistsSubquery) {
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
                    FromSubqueryColRefFactory.replace(pred, joinColRef, subqueryNestingLevel);
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
        return String.format(EXISTS_TABLE_ALIAS_PREFIX + "%s-%s", originalNestingLevel, ++flattenedCount);
    }

}