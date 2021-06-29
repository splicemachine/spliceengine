/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.List;

import static com.splicemachine.db.impl.sql.compile.PredicateSimplificationVisitor.isBooleanTrue;


/**
 * Perform query rewrites on the AST in the post-binding phase.
 * Typically the goal of rewrites is performance improvement by
 * eliminating unnecessary work, while maintaining a semantically
 * equivalent tree to the original.
 */
public class QueryRewriteVisitor extends AbstractSpliceVisitor {

    public QueryRewriteVisitor() {
    }

    @Override
    public Visitable defaultVisit(Visitable node) throws StandardException {
        if(node instanceof SetOperatorNode) {
            SetOperatorNode setOp = (SetOperatorNode)node;
            if (setOp.isAll())
                return node;
            // Convert unreferenced FromTables into EXISTS conditions in the WHERE clause.
            // Since duplicates will be removed, there is no use joining
            // all rows which would end up being removed by duplicate removal.
            convertUnreferencedFromTablesToEXISTS(setOp.getLeftResultSet());
            convertUnreferencedFromTablesToEXISTS(setOp.getRightResultSet());
        }
        return node;
    }

    private void convertUnreferencedFromTablesToEXISTS(ResultSetNode resultSet) throws StandardException {
        if (!(resultSet instanceof SelectNode))
            return;
        SelectNode selectNode = (SelectNode) resultSet;

        // Aggregation or window functions are affected by the number of rows,
        // so we can't eliminate the join in these cases.
        if (selectNode.hasWindowFunction() || selectNode.hasAggregation())
            return;

        // Cannot reduce rows if there is a nondeterministic function
        // such as random(), as each row may get a different value that
        // the distinct sort would not eliminate.
        if (selectNode.getResultColumns() != null) {
            CollectNodesVisitor cnv = new CollectNodesVisitor(MethodCallNode.class);
            selectNode.getResultColumns().accept(cnv);
            List<MethodCallNode> methodList = cnv.getList();
            for (MethodCallNode methodCallNode : methodList) {
                if (!methodCallNode.isDeterministic())
                    return;
            }
        }

        FromList fromList = resultSet.getFromList();
        for (int i = fromList.size() - 1; i >= 0; i--) {
            FromTable ft = (FromTable) (fromList.elementAt(i));

            // TODO:  If the inner table of left or right outer join
            //        has no referenced columns, replace the join
            //        with the outer table.
            //        Something like this:
            /**
            if (ft instanceof HalfOuterJoinNode) {
                HalfOuterJoinNode hoj = (HalfOuterJoinNode) ft;
                ResultSetNode innerTable, outerTable;
                if (hoj.isRightOuterJoin()) {
                    innerTable = hoj.getLeftResultSet();
                    outerTable = hoj.getRightResultSet();
                }
                else {
                    innerTable = hoj.getRightResultSet();
                    outerTable = hoj.getLeftResultSet();
                }
                if (!innerTable.getResultColumns().hasReferencedColumn() &&
                    (outerTable instanceof FromTable)) {
                    fromList.setElementAt(outerTable, i);
                    selectNode.bindResultColumns(parent.getFromList());
                }
            }
            **/

            ResultColumnList rcl = ft.getResultColumns();
            if (!rcl.hasReferencedColumn()) {
                // Remove the table from the FROM clause and add:
                // WHERE EXISTS (select 1 from table) to the where clause, if
                // there is more than one table in the FROM clause, or,
                // if there is only one table left in the FROM clause, convert
                // the query to a TOP 1 query.
                if (fromList.size() == 1) {
                    NumericConstantNode
                      numberOne = new NumericConstantNode(selectNode.getContextManager(),
                                                            C_NodeTypes.INT_CONSTANT_NODE,
                                                            Integer.valueOf(1));
                    selectNode.setFetchFirst(numberOne);
                }
                else {
                    fromList.removeElementAt(i);
                    constructExistsCondition(selectNode, ft);
                }
            }
        }
    }

    private void constructExistsCondition(SelectNode selectNode, FromTable fromTable) throws StandardException {
        ContextManager cm = fromTable.getContextManager();

        NumericConstantNode numberOne = new NumericConstantNode(cm, C_NodeTypes.INT_CONSTANT_NODE, Integer.valueOf(1));

        ResultColumn rc = new ResultColumn((String)null, numberOne, cm);
        ResultColumnList resultColumns = new ResultColumnList(cm);
        resultColumns.addResultColumn(rc);

        FromList fromList = new FromList(true, fromTable, cm);

        SelectNode newSelectNode = new SelectNode(resultColumns, fromList, null, cm);

        SubqueryNode existsSubquery = new SubqueryNode(newSelectNode,
                                        ReuseFactory.getInteger(SubqueryNode.EXISTS_SUBQUERY),
                                        null, null, null, null, true, cm);
        AndNode andNode = newAndNode(existsSubquery, false);
        andNode = (AndNode)selectNode.bindExtraExpressions(andNode);
        appendAndConditionToWhereClause(selectNode, andNode);
    }

    private void appendAndConditionToWhereClause(SelectNode selectNode, AndNode andNode) throws StandardException {
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
            if (isBooleanTrue(whereClauseAnd.getRightOperand())) {
                whereClauseAnd.setRightOperand(andNode);
            }
            else {
                AndNode newAndNode = newAndNode(whereClauseAnd.getRightOperand(), true);
                newAndNode.setRightOperand(andNode);
                whereClauseAnd.setRightOperand(newAndNode);
            }
        }
    }

    public static AndNode newAndNode(ValueNode left, boolean doPostBindFixup) throws StandardException {
        ValueNode trueNode = new BooleanConstantNode(Boolean.TRUE,left.getContextManager());
        AndNode   andNode = new AndNode(left, trueNode, left.getContextManager());
        if (doPostBindFixup)
            andNode.postBindFixup();
        return andNode;
    }

    /**
     * @return {@code false}, since the entire tree should be visited
     */
    public boolean stopTraversal() {
        return false;
    }

    /**
     * Only visit children that are result sets.
     *
     * @return true/false
     */
    @Override
    public boolean skipChildren(Visitable node)
    {
        return !(node instanceof ResultSetNode) &&
               !(node instanceof StatementNode) &&
               !(node instanceof FromList);
    }

}
