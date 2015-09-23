package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.impl.ast.PredicateUtils;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.List;

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
    public static void addGroupByNodes(SelectNode outerSelectNode,
                                       SelectNode subquerySelectNode,
                                       List<BinaryRelationalOperatorNode> correlatedSubqueryPreds) throws StandardException {

        /*
         * Nominal case: subquery is correlated, has one or more correlated predicates.  We will group by the subquery
         * columns that are compared to the outer query columns.
         */
        if (!correlatedSubqueryPreds.isEmpty()) {
            int subqueryNestingLevel = outerSelectNode.getNestingLevel() + 1;

            for (BinaryRelationalOperatorNode bro : correlatedSubqueryPreds) {
                if (PredicateUtils.isLeftColRef(bro, subqueryNestingLevel)) {
                    addGroupByNodes(subquerySelectNode, bro.getLeftOperand());
                } else if (PredicateUtils.isRightColRef(bro, subqueryNestingLevel)) {
                    addGroupByNodes(subquerySelectNode, bro.getRightOperand());
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
            ValueNode one = (ValueNode) subquerySelectNode.getNodeFactory().getNode(C_NodeTypes.INT_CONSTANT_NODE, 0, subquerySelectNode.getContextManager());
            addGroupByNodes(subquerySelectNode, one);
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
            /* We are grouping by 1, give he column a name.  This just for the benefit of EXPLAIN plan readability. */
            rc.setName("subqueryGroupByCol");
            rc.setNameGenerated(true);
        }
        rc.markAsGroupingColumn();
        rc.setVirtualColumnId(subquerySelectNode.getResultColumns().size() + 1);
        subquerySelectNode.getResultColumns().addElement(rc);

        //
        // PART 2: Add the GroupByList and GroupByColumn
        //

        // Create GroupByList if there isn't already one in the subquery.
        if (subquerySelectNode.getGroupByList() == null) {
            GroupByList groupByList = newGroupByList(subquerySelectNode.getNodeFactory(), subquerySelectNode.getContextManager());
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

    private static GroupByList newGroupByList(NodeFactory nodeFactory, ContextManager contextManager) throws StandardException {
        return (GroupByList) nodeFactory.getNode(C_NodeTypes.GROUP_BY_LIST, contextManager);
    }

    private static GroupByColumn newGroupByColumn(NodeFactory nodeFactory, ValueNode groupByCol, ResultColumn rc) throws StandardException {
        return (GroupByColumn) nodeFactory.getNode(
                C_NodeTypes.GROUP_BY_COLUMN,
                groupByCol,
                rc.getContextManager());
    }

}
