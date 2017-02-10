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

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.db.impl.sql.compile.subquery.exists.ExistsSubqueryFlatteningVisitor;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Predicates;
import org.spark_project.guava.collect.Lists;

import java.util.List;

/**
 * Visitor that enables an update or delete statement to operate successfully over a sink operation (e.g. merge sort
 * join). Vanilla Derby handles this by adding a CurrentRowLocationNode as a ResultColumn to the top most
 * ProjectRestrictNode.
 *
 * In order for this to work in Splice with our sink operations, a CurrentRowLocationNode is also used, but on the lower
 * level ProjectRestrictNodes, in particular those in the path to the leftmost leaf node. The row location of each row
 * to be updated can then be propagated to the top level update or delete statement.
 *
 * This visitor is a no-op except for an update or delete statement above a sink operation, in which case the query plan
 * tree is mutated to provide the support described above. No additional nodes are added, but existing nodes like
 * ProjectRestrictNodes are modified.
 *
 * @author Walt Koetke
 * @see com.splicemachine.db.impl.sql.compile.CurrentRowLocationNode
 */
public class RowLocationColumnVisitor extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(RowLocationColumnVisitor.class);

    @Override
    public Visitable visit(DeleteNode node) throws StandardException {
        return doVisit(node);
    }

    @Override
    public Visitable visit(UpdateNode node) throws StandardException {
        return doVisit(node);
    }

    private Visitable doVisit(DMLStatementNode dmlNode) throws StandardException {

        // Only continue if the operation is over a sink (e.g. update over merge join)
        if (!RSUtils.hasSinkingChildren(dmlNode.getResultSetNode())) {
            return dmlNode;
        }

        // STEP 1: identify the topmost Project Restrict Node and get a reference to its ResultColumn containing a
        // CurrentRowLocationNode.

        String rowLocationResultColName = (dmlNode instanceof UpdateNode) ? UpdateNode.COLUMNNAME : DeleteNode.COLUMNNAME;

        // Fetch the closest PRN descendant
        ProjectRestrictNode topProjectRestrictNode = findTopProjectRestrictNode(dmlNode);

        // Stash aside the row location result column from this upper PRN
        ResultColumn prnUpperRowLocCol = findRowLocationResultColumn(dmlNode, rowLocationResultColName, topProjectRestrictNode);

        // STEP 2: find leftmost path from topmost PRN to leaf

        List<ResultSetNode> pathToLeaf = findPathToLeaf(dmlNode, topProjectRestrictNode);

        // STEP 3: prepare each node in the 'left path' (from step 2) to handle row location properly

        updateTree(dmlNode, rowLocationResultColName, prnUpperRowLocCol, pathToLeaf);

        return dmlNode;
    }

    /**
     *
     */
    private static void updateTree(DMLStatementNode dmlNode,
                                   String rowLocationResultColName,
                                   ResultColumn prnUpperRowLocCol,
                                   List<ResultSetNode> pathToLeaf) throws StandardException {

        final int LAST_NODE_INDEX = pathToLeaf.size() - 1;

        for (int i = LAST_NODE_INDEX; i >= 0; i--) {

            ResultSetNode currentNode = pathToLeaf.get(i);

            //
            // CASE 1: Join
            //
            if (currentNode instanceof JoinNode) {
                // We have added a column on the left side of the join.  Renumber columns in the join result set
                // itself.  Among other things this will shift right by 1 all columns from the right side.
                JoinNode joinNode = (JoinNode) currentNode;
                joinNode.rebuildRCL();

                fixAnyRightSideIsNullColumnReferencesFromNotExistsSubqueryFlattening(dmlNode, joinNode);
            }
            //
            // CASE 2: We are at a ResultSetNode above the leaf (probably above a FromBaseTable). We should be below
            // the the sinking operation(s) and we don't have a CurrentRowLocationNode at this depth. Clone the
            // one derby adds top the top ProjectRestrictNode and add it here.
            //
            else if (i == LAST_NODE_INDEX) {
                ResultColumn rowLocationResultColumnClone = prnUpperRowLocCol.cloneMe();
                rowLocationResultColumnClone.setResultSetNumber(currentNode.getResultSetNumber());
                currentNode.getResultColumns().addResultColumn(rowLocationResultColumnClone);
            }
            //
            // CASE 3: Similar to case 4 below but we are now at the TOP ProjectRestrictNode just below the UPDATE/DELETE.
            // Instead of *adding* a VirtualColumnNode replace the CurrentRowLocationNode added by derby with a
            // VirtualColumnNode.
            //
            else if (i == 0) {
                ResultSetNode nodeBelowMe = pathToLeaf.get(i + 1);
                ResultColumn rowLocResultCol = nodeBelowMe.getResultColumns().getResultColumn(rowLocationResultColName);
                int columnId = prnUpperRowLocCol.getVirtualColumnId();
                VirtualColumnNode virtualColumnNode = createVirtualColumnNode(dmlNode, columnId, nodeBelowMe, rowLocResultCol);
                prnUpperRowLocCol.setExpression(virtualColumnNode);
            }
            //
            // CASE 4: We are at a ResultSetNode that is (a) NOT a join node; and (b) NOT the final node in the path;
            // and not the first node in the path.  Lets add a VirtualColumnNode to reference the ResultColumn having a
            // CurrentRowLocationNode we have either added below this node (or which results from the join below this
            // node).
            //
            else {
                ResultColumnList currentResultColumns = currentNode.getResultColumns();

                ResultSetNode nodeBelowMe = pathToLeaf.get(i + 1);
                ResultColumn rowLocResultCol = nodeBelowMe.getResultColumns().getResultColumn(rowLocationResultColName);
                int columnId = currentResultColumns.size() + 1;
                VirtualColumnNode virtualColumnNode = createVirtualColumnNode(dmlNode, columnId, nodeBelowMe, rowLocResultCol);

                ResultColumn newRC = currentResultColumns.getResultColumn(currentResultColumns.size());
                newRC = newRC.cloneMe();
                newRC.setExpression(virtualColumnNode);
                newRC.setName(rowLocationResultColName);
                currentResultColumns.addResultColumn(newRC);

            }

        }
    }


    /**
     * Start at the specified ProjectRestrictNode and traverse the tree (always going left) collecting nodes until we
     * reach a node that has no children (should be a FromBaseTable). That leaf node is not included in the returned
     * list. The passed topProjectRestrictNode will always be the first element in the returned list.
     *
     * Here we are collecting the path of nodes from the top of the tree to the FromBaseTable of the table being
     * updated. This assumes that the target table of our update/delete will always be found by going left at nodes with
     * multiple children.
     */
    private static List<ResultSetNode> findPathToLeaf(DMLStatementNode dmlNode,
                                                      ProjectRestrictNode topProjectRestrictNode) throws StandardException {
        ResultSetNode currentNode = topProjectRestrictNode;
        List<ResultSetNode> pathToLeaf = Lists.newArrayList();
        while (true) {
            List<ResultSetNode> children = RSUtils.getChildren(currentNode); // only returns RSN children
            if (children.size() > 1) {
                // We assume this is a binary node like a JoinNode
                if (!(RSUtils.binaryRSNs.contains(currentNode.getClass()))) {
                    error("Unexpectedly found node with %s children but of non binary type %s", children.size(), currentNode.getClass());
                }
                pathToLeaf.add(currentNode);
                currentNode = getLeftChildNode(currentNode);
            } else if (children.size() == 1) {
                pathToLeaf.add(currentNode);
                currentNode = children.get(0);
            } else {
                // Assume leaf node, so we are done
                if (!(RSUtils.leafRSNs.contains(currentNode.getClass()))) {
                    error("Leaf node had unexpected type %s", currentNode.getClass());
                }
                break;
            }
        }

        if (pathToLeaf.size() < 1) {
            error("Unexpectedly found pathToLeaf list was empty when visiting node %s", dmlNode);
        }
        return pathToLeaf;
    }


    private static void error(String msg, Object... args) {
        throw new RuntimeException(String.format(msg, args));
    }

    private static ResultSetNode getLeftChildNode(ResultSetNode node) {
        assert node instanceof TableOperatorNode;
        return ((TableOperatorNode) node).getLeftResultSet();
    }


    /**
     * Return the ResultColumn having name '###RowLocationToUpdate', or '###RowLocationToDelete'
     */
    private static ResultColumn findRowLocationResultColumn(DMLStatementNode dmlNode,
                                                            String rowLocationResultColName,
                                                            ProjectRestrictNode prnUpper) {
        ResultColumnList prnUpperRCL = prnUpper.getResultColumns();
        assert prnUpperRCL != null;
        ResultColumn prnUpperRowLocCol = prnUpperRCL.getResultColumn(rowLocationResultColName);
        if (prnUpperRowLocCol == null) {
            error("Unable to find the row location ResultColumn in upper ProjectRestrictNode for node %s", dmlNode);
        }
        return prnUpperRowLocCol;
    }

    /**
     * Return the first ProjectRestrictNode decedent of the specified DMLStatementNode.
     */
    private static ProjectRestrictNode findTopProjectRestrictNode(DMLStatementNode node) throws StandardException {
        List<ProjectRestrictNode> prnAll = RSUtils.collectNodes(node.getResultSetNode(), ProjectRestrictNode.class);
        if (prnAll == null || prnAll.isEmpty()) {
            error("Unable to fetch descendant ProjectRestrictNodes for node %s", node);
        }
        ProjectRestrictNode prnUpper = prnAll.get(0);
        if (prnUpper == null) {
            error("Unable to fetch upper ProjectRestrictNode for node %s", node);
        }
        return prnUpper;
    }


    /**
     * Create new VirtualColumnNode for referencing the ResultColumn with for row location below us.
     */
    private static VirtualColumnNode createVirtualColumnNode(DMLStatementNode dmlNode,
                                                             int columnId,
                                                             ResultSetNode sourceResultSetNode,
                                                             ResultColumn sourceResultColumn) throws StandardException {
        assert sourceResultColumn != null;

        NodeFactory nodeFactory = dmlNode.getNodeFactory();
        ContextManager contextManager = dmlNode.getContextManager();

        return (VirtualColumnNode) nodeFactory.getNode(
                C_NodeTypes.VIRTUAL_COLUMN_NODE,
                sourceResultSetNode,        // source result set: my child node
                sourceResultColumn,         // source ResultColumn
                columnId,                   // expects 1-based index for this column
                contextManager);
    }

    /**
     * If we arrive here we know we have added a CurrentRowLocation column to the result columns of the left side of the
     * passed join.   We have updated the join's result columns to account for this (renumber them incrementing columns
     * on right by one).  But we have not yet adjusted any column references above the join.  Do that now.
     *
     * This situation was probably not possible before splice added not-exists subquery flattening.
     * <pre>
     * FROM:UPDATE over [select * A where not exists (select 1 from B where a1=b1)]
     * TO:UPDATE over [select * from A * left join (select b1 from B group by b1) foo on a1=foo.b1 where foo.b1 is
     * null;
     * </pre>
     *
     * In the first query the subquery could not be given an alias and its result column not referenced in the outer
     * query.  Now however we add the isNull(foo.b1) predicate to the outer query as part of not-exists subquery
     * flattening.
     */
    private static void fixAnyRightSideIsNullColumnReferencesFromNotExistsSubqueryFlattening(
            DMLStatementNode node,
            JoinNode joinNode) throws StandardException {

        /* Find column references starting at the DML node but don't go below the current join node. */
        CollectingVisitor<IsNullNode> collectingVisitorVisitor = new CollectingVisitor<>(new NotExistsFlatteningIsNullPredicate());
        org.spark_project.guava.base.Predicate<? super Visitable> skipPredicate = Predicates.<Visitable>equalTo(joinNode);
        SkippingVisitor skippingVisitor = new SkippingVisitor(collectingVisitorVisitor, skipPredicate);
        node.acceptChildren(skippingVisitor);
        List<IsNullNode> isNullNodes = collectingVisitorVisitor.getCollected();

        /* Collect the corresponding ColumnReferences if any */
        List<ColumnReference> columnReferences = Lists.newArrayList();
        for (IsNullNode isNullNode : isNullNodes) {
            columnReferences.add((ColumnReference) isNullNode.getOperand());
        }

        for (ColumnReference foundColumnRef : columnReferences) {

            int leftResultColCount = joinNode.getLeftResultSet().getResultColumns().size();
            int joinResultColCount = joinNode.getResultColumns().size();

            /* Iterate over ResultColumns from right side of join only--only those have been shifted by adding
             * RolLocationColumn ResultColumn to left side below the join. */
            for (int position = leftResultColCount + 1; position <= joinResultColCount; position++) {
                ResultColumn rightSideRC = joinNode.getResultColumns().getResultColumn(position);
                boolean equalColNames = rightSideRC.getName().equals(foundColumnRef.getColumnName());
                boolean expectedMismatch = (foundColumnRef.getColumnNumber() + 1 == rightSideRC.getVirtualColumnId());
                if (equalColNames && expectedMismatch) {
                    foundColumnRef.setColumnNumber(rightSideRC.getVirtualColumnId());
                    foundColumnRef.setSource(rightSideRC);
                }
            }
        }
    }

    /**
     * Returns true for a given IsNullNode if it has this shape:
     *
     * IsNullNode -> ColumnReference
     *
     * Where the ColumnReference is for a table created as part of not exists subquery flattening.
     */
    private static class NotExistsFlatteningIsNullPredicate implements org.spark_project.guava.base.Predicate<Visitable> {
        @Override
        public boolean apply(Visitable visitable) {

            // IsNullNode
            if (!(visitable instanceof IsNullNode)) {
                return false;
            }

            // ColumnReference
            IsNullNode isNullNode = (IsNullNode) visitable;
            if (!(isNullNode.getOperand() instanceof ColumnReference)) {
                return false;
            }

            // References Not-Exists subquery flattening col
            ColumnReference columnReference = (ColumnReference) isNullNode.getOperand();
            String tableName = columnReference.getTableName();
            return tableName != null && tableName.startsWith(ExistsSubqueryFlatteningVisitor.EXISTS_TABLE_ALIAS_PREFIX);

        }
    }

}
