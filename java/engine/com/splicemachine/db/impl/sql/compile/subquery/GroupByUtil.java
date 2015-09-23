package com.splicemachine.db.impl.sql.compile.subquery;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
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
     * it in the above example for clarity)
     */
    public static void addGroupByNodes(SelectNode outerSelectNode,
                                       SelectNode subquerySelectNode,
                                       List<BinaryRelationalOperatorNode> correlatedSubqueryPreds) throws StandardException {

        int subqueryNestingLevel = outerSelectNode.getNestingLevel() + 1;

        for (BinaryRelationalOperatorNode bro : correlatedSubqueryPreds) {
            if (PredicateUtils.isLeftColRef(bro, subqueryNestingLevel)) {
                addGroupByNodes(subquerySelectNode, (ColumnReference) bro.getLeftOperand());
            } else if (PredicateUtils.isRightColRef(bro, subqueryNestingLevel)) {
                addGroupByNodes(subquerySelectNode, (ColumnReference) bro.getRightOperand());
            }
        }
    }

    /**
     * @param colRef is the subquery column that is part of a correlated predicate.
     */
    public static void addGroupByNodes(SelectNode subquerySelectNode, ColumnReference colRef) throws StandardException {

        //
        // PART 1: Add column ref to subquery result columns
        //
        ResultColumn rc = (ResultColumn) subquerySelectNode.getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                colRef.getColumnName(),
                colRef,
                subquerySelectNode.getContextManager());
        rc.setColumnDescriptor(null);

        if (colRef.getTableNameNode() != null) {
            rc.setSourceSchemaName(colRef.getTableNameNode().getSchemaName());
            rc.setSourceTableName(colRef.getTableNameNode().getTableName());
            rc.setSourceTableName(colRef.getTableNameNode().getTableName());
        }
        rc.markAsGroupingColumn();
        rc.setVirtualColumnId(subquerySelectNode.getResultColumns().size() + 1);
        subquerySelectNode.getResultColumns().addElement(rc);

        //
        // PART 2: Add the GroupByList and GroupByColumn
        //

        // Create GroupByList if there isn't already one in the subquery.
        if (subquerySelectNode.getGroupByList() == null) {
            subquerySelectNode.setGroupByList((GroupByList) subquerySelectNode.getNodeFactory().getNode(C_NodeTypes.GROUP_BY_LIST,
                    subquerySelectNode.getContextManager()));

        }

        GroupByColumn gbc = (GroupByColumn) subquerySelectNode.getNodeFactory().getNode(
                C_NodeTypes.GROUP_BY_COLUMN,
                colRef,
                rc.getContextManager());

        gbc.setColumnPosition(rc.getVirtualColumnId());

        subquerySelectNode.getGroupByList().addGroupByColumn(gbc);
    }
}
