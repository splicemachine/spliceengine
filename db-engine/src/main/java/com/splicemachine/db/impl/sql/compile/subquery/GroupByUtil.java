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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.impl.ast.CorrelatedColRefCollectingVisitor;
import com.splicemachine.db.impl.ast.PredicateUtils;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.List;

import static com.splicemachine.db.iapi.sql.compile.C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE;
import static com.splicemachine.db.impl.sql.compile.AndNode.newAndNode;
import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INTERNAL_ERROR;

public class GroupByUtil {

    /**
     * Transform:
     *
     * <pre>
     *     select A.*
     *       from A
     *       where EXISTS( select 1 from B where b1=a1);
     * </pre>
     *
     * To:
     *
     * <pre>
     *     select A.*
     *       from A
     *       where EXISTS( select 1,b1 from B where b1=a1 group by by b1 );
     * </pre>
     *
     * (actually the correlated subquery predicate(s) are removed from the subquery tree before we get here but I left
     * one in the above example for clarity)
     */
    public static ColumnReference addGroupByNodes(SelectNode subquerySelectNode,
                                       List<BinaryRelationalOperatorNode> correlatedSubqueryPreds,
                                       List<BinaryRelationalOperatorNode> correlatedSubqueryInequalityPreds,
                                       int outerNestingLevel,
                                       SelectNode outerSelectNode) throws StandardException {

        /*
         * Nominal case: subquery is correlated, has one or more correlated predicates.  We will group by the subquery
         * columns that are compared to the outer query columns.
         */
        ColumnReference outerColumnReference = null;
        if (!correlatedSubqueryPreds.isEmpty()) {
            int subqueryNestingLevel = subquerySelectNode.getNestingLevel();

            for (BinaryRelationalOperatorNode bro : correlatedSubqueryPreds) {
                if (PredicateUtils.isLeftColRef(bro, subqueryNestingLevel)) {
                    addGroupByNodes(subquerySelectNode, bro.getLeftOperand());
                    outerColumnReference = (ColumnReference)bro.getRightOperand();
                } else if (PredicateUtils.isRightColRef(bro, subqueryNestingLevel)) {
                    addGroupByNodes(subquerySelectNode, bro.getRightOperand());
                    outerColumnReference = (ColumnReference)bro.getLeftOperand();
                }
            }
        }
        else if (!correlatedSubqueryInequalityPreds.isEmpty()) {
            for (BinaryRelationalOperatorNode bro : correlatedSubqueryInequalityPreds) {
                CorrelatedColRefCollectingVisitor correlatedColRefCollectingVisitor
                    = new CorrelatedColRefCollectingVisitor<>(1, outerNestingLevel);
                bro.accept(correlatedColRefCollectingVisitor);
                if (correlatedColRefCollectingVisitor.getCollected().isEmpty())
                    throw new IllegalArgumentException("Did not find correlated column ref on either side of BRON");

                ColumnReference subqueryColRef, correlatedColumnReference;
                correlatedColumnReference =
                    (ColumnReference)correlatedColRefCollectingVisitor.getCollected().get(0);

                FromTable fromTable;
                String tableName = correlatedColumnReference.getTableName();
                String schemaName = correlatedColumnReference.getSchemaName();
                if (tableName == null || schemaName == null) {
                    fromTable = outerSelectNode.getFromList().getFromTableByTableNumber(correlatedColumnReference.getTableNumber());
                    correlatedColumnReference.setTableNameNode(fromTable.getTableName());
                }
                else
                    fromTable = outerSelectNode.getFromList().
                                     getFromTableByName(correlatedColumnReference.getTableName(),
                                                        correlatedColumnReference.getSchemaName(), true);
                // subqueryColRef is the version of the correlatedColumnReference pushed into the join
                //                in the new generated outer_table.col = inner_table.col predicate.
                // outerColumnReference is the version of the correlatedColumnReference
                //                      in the new generated outer_table.col = inner_table.col predicate,
                //                      but cloned so that it has memory separate from the original
                //                      correlated inequality predicate.
                subqueryColRef = (ColumnReference)correlatedColumnReference.getClone();
                outerColumnReference = (ColumnReference)correlatedColumnReference.getClone();

                if (fromTable instanceof FromBaseTable) {
                    FromBaseTable fromBaseTable = (FromBaseTable)fromTable;
                    FromTable alreadyPushedTable =
                        subquerySelectNode.getFromList().
                         getFromTableByName(fromBaseTable.getTableName().getTableName(),
                                            fromBaseTable.getTableName().getSchemaName(), true);

                    if (alreadyPushedTable == null) {
                        FromBaseTable pushedFromBaseTable = fromBaseTable.shallowClone();
                        pushedFromBaseTable =
                            (FromBaseTable) subquerySelectNode.getFromList().
                                            bindExtraTableAndAddToFromClause(pushedFromBaseTable);
                        pushedFromBaseTable.setLevel(subquerySelectNode.getNestingLevel());
                    }
                    subqueryColRef = (ColumnReference) subquerySelectNode.bindExtraExpressions(subqueryColRef);

                    BinaryRelationalOperatorNode bron =
                        (BinaryRelationalOperatorNode)
                        subqueryColRef.getNodeFactory().getNode(
                            BINARY_EQUALS_OPERATOR_NODE,
                            subqueryColRef,
                            outerColumnReference,
                            subqueryColRef.getContextManager());

                    bron.bindExpression(subquerySelectNode.getFromList(), null, null);
                    // rebind one of the columns to refer to the outer table.
                    outerSelectNode.bindExtraExpressions(outerColumnReference);

                    // Append new predicate to correlatedSubqueryPredsD as we need
                    // to process the generated equality condition in the same manner
                    // as in subqueries with correlated equality join conditions.
                    correlatedSubqueryPreds.add(bron);

                    // Re-bind the correlated column reference in bro, so it points
                    // to the column in copy of the table that was pushed into
                    // the subquery.
                    subquerySelectNode.bindExtraExpressions(correlatedColumnReference);
                    AndNode andNode = newAndNode(bro, true);
                    appendAndConditionToWhereClause(subquerySelectNode, andNode);

                    addGroupByNodes(subquerySelectNode, subqueryColRef);
                }
                else {
                    throw StandardException.newException(LANG_INTERNAL_ERROR,
                        "Unable to build new predicate during subquery flattening.");
                }
            }
        }
        /*
         * Special case, subquery not correlated. Instead our transformation becomes:
         * <pre>
         * FROM: select A.* from A where EXISTS( select b1 from B );
         *  TO : select A.* from A where EXISTS( select b1, 1 from B group by 1 );
         * </pre>
         */
        else {
            ConstantNode one = (ConstantNode) subquerySelectNode.getNodeFactory().getNode(C_NodeTypes.INT_CONSTANT_NODE, 1, subquerySelectNode.getContextManager());
            addGroupByNodes(subquerySelectNode, one);
        }
        return outerColumnReference;
    }

    private static void appendAndConditionToWhereClause(SelectNode selectNode, AndNode andNode) throws StandardException {
        if (selectNode.getWhereClause() == null) {
            selectNode.setWhereClause(andNode);
            return;
        }
        ValueNode whereClause = selectNode.getWhereClause();
        if (!(whereClause instanceof AndNode)) {
            AndNode newWhereClause = newAndNode(whereClause, true);
            newWhereClause.setRightOperand(andNode);
            selectNode.setWhereClause(newWhereClause);
        }
        else {
            AndNode whereClauseAnd = (AndNode)whereClause;
            while (whereClauseAnd.getRightOperand() instanceof AndNode)
                whereClauseAnd = (AndNode)whereClauseAnd.getRightOperand();
            if (whereClauseAnd.getRightOperand().isBooleanTrue()) {
                whereClauseAnd.setRightOperand(andNode);
            }
            else {
                AndNode newAndNode = newAndNode(whereClauseAnd.getRightOperand(), true);
                newAndNode.setRightOperand(andNode);
                whereClauseAnd.setRightOperand(newAndNode);
            }
        }
    }

    /**
     * Create new ResultColumn, GroupByList (if necessary) and GroupByColumn and add them to the subquery.
     *
     * @param groupByCol This can be a ColumnReference in the case that we are grouping by correlated predicates, or a
     *                   ValueNode (1) if we are grouping by 1.
     */
    public static void addGroupByNodes(SelectNode subquerySelectNode, ValueNode groupByCol) throws StandardException {

        //
        // PART 1: Add column ref to subquery result columns
        //
        ResultColumn rc = newResultColumn(subquerySelectNode.getNodeFactory(), subquerySelectNode.getContextManager(), groupByCol);

        if (groupByCol instanceof ColumnReference) {
            ColumnReference colRef = (ColumnReference) groupByCol;
            if (colRef.getTableNameNode() != null) {
                rc.setSourceSchemaName(colRef.getTableNameNode().getSchemaName());
                rc.setSourceTableName(colRef.getTableNameNode().getTableName());
            }
        } else {
            /* We are grouping by 1, give the column a name.  This is just for the benefit of EXPLAIN plan readability. */
            rc.setName("subqueryGroupByCol");
            rc.setNameGenerated(true);
        }
        rc.setReferenced();
        rc.markAsGroupingColumn();
        rc.setResultSetNumber(subquerySelectNode.getResultSetNumber());
        subquerySelectNode.getResultColumns().addResultColumn(rc);

        //
        // PART 2: Add the GroupByList and GroupByColumn
        //

        // Create GroupByList if there isn't already one in the subquery.
        if (subquerySelectNode.getGroupByList() == null) {
            GroupByList groupByList = new GroupByList(subquerySelectNode.getContextManager());
            subquerySelectNode.setGroupByList(groupByList);
        }

        GroupByColumn groupByColumn = newGroupByColumn(subquerySelectNode.getNodeFactory(), groupByCol, rc);

        groupByColumn.setColumnPosition(rc.getVirtualColumnId());

        subquerySelectNode.getGroupByList().addGroupByColumn(groupByColumn);
    }

    // - - - -
    // nodes
    // - - - -

    private static ResultColumn newResultColumn(NodeFactory nodeFactory, ContextManager contextManager, ValueNode groupByCol) throws StandardException {
        ResultColumn rc = (ResultColumn) nodeFactory.getNode(
                C_NodeTypes.RESULT_COLUMN,
                groupByCol.getColumnName(),
                groupByCol,
                contextManager);
        rc.setColumnDescriptor(null);
        return rc;
    }

    private static GroupByColumn newGroupByColumn(NodeFactory nodeFactory, ValueNode groupByCol, ResultColumn rc) throws StandardException {
        return (GroupByColumn) nodeFactory.getNode(
                C_NodeTypes.GROUP_BY_COLUMN,
                groupByCol,
                rc.getContextManager());
    }

}
