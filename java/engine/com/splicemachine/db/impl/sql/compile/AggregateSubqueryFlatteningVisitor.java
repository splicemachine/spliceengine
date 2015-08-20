package com.splicemachine.db.impl.sql.compile;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.AbstractSpliceVisitor;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


/**
 * Subquery flattening for where-subqueries with aggregates.  Splice added this class.  Previously derby explicitly did
 * not attempt to flatten where-subqueries with aggregates.
 * <p/>
 * How it works: We move the aggregate subquery into the enclosing query's FromList, add a GroupBy node and a table
 * alias, and update column references. From there we let derby further flatten the FromSubquery into a join.  The
 * result is an execution-time plan that has no subqueries.
 * <p/>
 * See nested class DoWeHandlePredicate for a list of current restrictions on the query types we support.
 * <p/>
 * In the context of this class "top" means the outer, or enclosing, select for the subquery(s) we are currently
 * attempting to flatten.
 * <p/>
 * See also:
 * SubqueryNode.flattenToNormalJoin()
 * SubqueryNode.flattenToExistsJoin()
 * FromSubquery.flatten()
 */
public class AggregateSubqueryFlatteningVisitor extends AbstractSpliceVisitor implements Visitor {

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
        return node instanceof SelectNode && !(((SelectNode) node).whereClause instanceof BinaryOperatorNode);
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {

        /**
         * Stop if this is not a select node with a BinaryOperator where
         */
        if (!(node instanceof SelectNode) || !(((SelectNode) node).whereClause instanceof BinaryOperatorNode)) {
            return node;
        }

        SelectNode topSelectNode = (SelectNode) node;

        /**
         * Stop if there are no subqueries.
         */
        if (topSelectNode.getWhereSubquerys() == null || topSelectNode.getWhereSubquerys().size() == 0) {
            return node;
        }

        /*
         * Stop if there are no subqueries we handle.
         */
        List<SubqueryNode> subqueryList = topSelectNode.getWhereSubquerys().getNodes();
        List<SubqueryNode> handledSubqueryList = Lists.newArrayList(Iterables.filter(subqueryList, new AggregateSubqueryPredicate()));
        if (handledSubqueryList.isEmpty()) {
            return node;
        }

        /*
         * Update table numbers.
         */
        for (SubqueryNode subqueryNode : subqueryList) {
            if (subqueryNode.resultSet instanceof SelectNode) {
                updateTableNumbers((SelectNode) subqueryNode.resultSet, handledSubqueryList.size(), ((SelectNode) subqueryNode.resultSet).getNestingLevel());
            }
        }
        for(QueryTreeNode qtn : topSelectNode.fromList){
            if (qtn instanceof FromSubquery) {
                if(((FromSubquery) qtn).getSubquery() instanceof SelectNode){
                    SelectNode sn = (SelectNode)((FromSubquery) qtn).getSubquery();
                    updateTableNumbers(sn, handledSubqueryList.size(), sn.getNestingLevel());
                }
            }
        }

        /*
         * Flatten where applicable.
         */
        CompilerContext cpt = topSelectNode.getCompilerContext();
        cpt.setNumTables(cpt.getNumTables() + handledSubqueryList.size());

        for (SubqueryNode subqueryNode : handledSubqueryList) {
            List<BinaryRelationalOperatorNode> predsToSwitch = new ArrayList<>();
            flatten(topSelectNode, subqueryNode, predsToSwitch);
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
     * Perform the actual flattening.
     */
    private void flatten(SelectNode topSelectNode,
                         SubqueryNode subqueryNode,
                         List<BinaryRelationalOperatorNode> predsToSwitch) throws StandardException {

        ResultSetNode subqueryResultSet = subqueryNode.getResultSet();
        SelectNode subquerySelectNode = (SelectNode) subqueryResultSet;

        ValueNode subqueryWhereClause = subquerySelectNode.getWhereClause();

        subqueryWhereClause = findCorrelatedSubqueryPredicates(subqueryWhereClause, predsToSwitch, topSelectNode.getNestingLevel());

        subquerySelectNode.whereClause = subqueryWhereClause;
        subquerySelectNode.originalWhereClause = subqueryWhereClause;

        for (BinaryRelationalOperatorNode bro : predsToSwitch) {
            if (bro.getLeftOperand() instanceof ColumnReference &&
                    ((ColumnReference) bro.getLeftOperand()).getSourceLevel() == topSelectNode.getNestingLevel() + 1) {
                addGroupByColumnNode(subquerySelectNode, (ColumnReference) bro.getLeftOperand());
            } else if (bro.getRightOperand() instanceof ColumnReference &&
                    ((ColumnReference) bro.getRightOperand()).getSourceLevel() == topSelectNode.getNestingLevel() + 1) {
                addGroupByColumnNode(subquerySelectNode, (ColumnReference) bro.getRightOperand());
            }
        }

        ResultColumnList newRcl = subquerySelectNode.resultColumns.copyListAndObjects();
        newRcl.genVirtualColumnNodes(subquerySelectNode, subquerySelectNode.resultColumns);

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
        FromList fl = topSelectNode.fromList;
        int ind = fl.size() - 1;
        while (!(fl.elementAt(ind) instanceof FromTable)) {
            ind--;
        }
        FromTable ft = (FromTable) fl.elementAt(ind);
        fromSubquery.tableNumber = ft.tableNumber + 1;
        fromSubquery.level = ft.getLevel();
        fl.addFromTable(fromSubquery);

        /*
         * Modify the top where clause to reference new FromTable node
         */
        ValueNode newTopWhereClause = switchPredReference(topSelectNode.whereClause, fromSubquery, topSelectNode.getNestingLevel());
        for (int i = 0; i < predsToSwitch.size(); i++) {
            BinaryRelationalOperatorNode pred = predsToSwitch.get(i);
            ColumnReference cf = (ColumnReference) pred.getNodeFactory().getNode(C_NodeTypes.COLUMN_REFERENCE,
                    fromSubquery.resultColumns.elementAt(i + 1).name,
                    fromSubquery.getTableName(),
                    pred.getContextManager());
            cf.setSource(fromSubquery.resultColumns.elementAt(i + 1));
            cf.setTableNumber(fromSubquery.tableNumber);
            cf.setColumnNumber(fromSubquery.resultColumns.elementAt(i + 1).getVirtualColumnId());
            cf.setNestingLevel(topSelectNode.getNestingLevel());
            cf.setSourceLevel(topSelectNode.getNestingLevel());
            if (pred.getLeftOperand() instanceof ColumnReference &&
                    ((ColumnReference) pred.getLeftOperand()).getSourceLevel() == topSelectNode.getNestingLevel() + 1) {
                pred.setLeftOperand(pred.getRightOperand());
                ((ColumnReference) pred.getLeftOperand()).setNestingLevel(((ColumnReference) pred.getLeftOperand()).getSourceLevel());
                pred.setRightOperand(cf);
            } else if (pred.getRightOperand() instanceof ColumnReference &&
                    ((ColumnReference) pred.getRightOperand()).getSourceLevel() == topSelectNode.getNestingLevel() + 1) {
                ((ColumnReference) pred.getLeftOperand()).setNestingLevel(((ColumnReference) pred.getLeftOperand()).getSourceLevel());
                pred.setRightOperand(cf);
            }

            newTopWhereClause = addPredToTree(newTopWhereClause, pred);
        }

        topSelectNode.originalWhereClause = newTopWhereClause;
        topSelectNode.whereClause = newTopWhereClause;
    }

    private static void addGroupByColumnNode(SelectNode subquerySelectNode, ColumnReference colRef) throws StandardException {
        ResultColumn rc = (ResultColumn) subquerySelectNode.getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                colRef.columnName,
                colRef,
                subquerySelectNode.getContextManager());
        rc.columnDescriptor = null;
        if (colRef.tableName != null) {
            rc.sourceSchemaName = colRef.tableName.schemaName;
            rc.sourceTableName = colRef.tableName.tableName;
            rc.tableName = colRef.tableName.tableName;
        }
        rc.isGroupingColumn = true;
        rc.setVirtualColumnId(subquerySelectNode.getResultColumns().size() + 1);
        subquerySelectNode.getResultColumns().addElement(rc);

        // Create GroupByList if there isn't already one in the subquery.
        if (subquerySelectNode.groupByList == null) {
            subquerySelectNode.groupByList = (GroupByList) subquerySelectNode.getNodeFactory().getNode(C_NodeTypes.GROUP_BY_LIST,
                    subquerySelectNode.getContextManager());

        }

        GroupByColumn gbc = (GroupByColumn) subquerySelectNode.getNodeFactory().getNode(
                C_NodeTypes.GROUP_BY_COLUMN,
                colRef,
                rc.getContextManager());
        gbc.columnPosition = rc.getVirtualColumnId();
        subquerySelectNode.groupByList.addGroupByColumn(gbc);
    }

    public static ValueNode switchPredReference(ValueNode node,
                                                FromSubquery fsq, int level) throws StandardException {
        if (node instanceof BinaryOperatorNode) {
            BinaryOperatorNode root = (BinaryOperatorNode) node;
            ValueNode left = root.getLeftOperand();
            ValueNode right = root.getRightOperand();
            ColumnReference cf = (ColumnReference) fsq.getNodeFactory().getNode(C_NodeTypes.COLUMN_REFERENCE,
                    fsq.resultColumns.elementAt(0).name,
                    fsq.getTableName(),
                    fsq.getContextManager());
            cf.setSource(fsq.resultColumns.elementAt(0));
            cf.setTableNumber(fsq.tableNumber);
            cf.setColumnNumber(fsq.resultColumns.elementAt(0).getVirtualColumnId());
            cf.setNestingLevel(level);
            cf.setSourceLevel(level);
            if (left instanceof SubqueryNode && ((SubqueryNode) left).getResultSet() == fsq.getSubquery()) {
                root.setLeftOperand(cf);
                return root;
            } else if (right instanceof SubqueryNode && ((SubqueryNode) right).getResultSet() == fsq.getSubquery()) {
                root.setRightOperand(cf);
                return root;
            } else {
                left = switchPredReference(left, fsq, level);
                right = switchPredReference(right, fsq, level);
                root.setLeftOperand(left);
                root.setRightOperand(right);
                return root;
            }
        } else if (node instanceof ColumnReference) {
            return node;
        }
        return node;
    }

    private static ValueNode addPredToTree(ValueNode node,
                                           BinaryRelationalOperatorNode pred) throws StandardException {
        if (node instanceof AndNode) {
            AndNode root = (AndNode) node;
            if (root.getLeftOperand() instanceof BinaryRelationalOperatorNode) {
                return (AndNode) node.getNodeFactory().getNode(C_NodeTypes.AND_NODE,
                        root,
                        pred,
                        node.getContextManager());
            } else {
                root.setLeftOperand(addPredToTree(root.getLeftOperand(), pred));
                return root;
            }

        } else {
            return (AndNode) node.getNodeFactory().getNode(C_NodeTypes.AND_NODE,
                    node,
                    pred,
                    node.getContextManager());
        }
    }

    /**
     * Collects (in the passed list) correlation predicates (as BinaryRelationalOperatorNodes) for the subquery and
     * also removes them from the subquery where clause. The logic here is highly dependent on the shape of the
     * where-clause subtree which we assert in AggregateSubqueryWhereVisitor.
     */
    private static ValueNode findCorrelatedSubqueryPredicates(ValueNode root,
                                                              List<BinaryRelationalOperatorNode> predToSwitch,
                                                              int level) {
        if (root instanceof AndNode) {
            AndNode andNode = (AndNode) root;
            ValueNode left = findCorrelatedSubqueryPredicates(andNode.getLeftOperand(), predToSwitch, level);
            ValueNode right = findCorrelatedSubqueryPredicates(andNode.getRightOperand(), predToSwitch, level);
            if (left == null) {
                return right;
            } else if (right == null) {
                return left;
            }
            andNode.setLeftOperand(left);
            andNode.setRightOperand(right);
            return root;
        } else if (root instanceof BinaryRelationalOperatorNode) {
            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode) root;
            ValueNode left = bron.getLeftOperand();
            ValueNode right = bron.getRightOperand();
            if (left instanceof ColumnReference && (((ColumnReference) left).getSourceLevel() == level)
                    || right instanceof ColumnReference && (((ColumnReference) right).getSourceLevel() == level)) {
                predToSwitch.add(bron);
                return null;
            }
        }
        return root;
    }

    public static void updateTableNumbers(SelectNode node, int diff, int level) {

        for (QueryTreeNode qtn : node.getFromList()) {
            if (qtn instanceof FromTable) {
                ((FromTable) qtn).tableNumber += diff;
                if(qtn instanceof FromSubquery) {
                    ResultSetNode rsn = ((FromSubquery) qtn).subquery;
                    if (rsn instanceof SelectNode) {
                        updateTableNumbers((SelectNode) rsn, diff, level);
                    }
                }
            }
        }
        if(node.getSelectSubquerys() != null){
            for(SubqueryNode sbn : node.getSelectSubquerys()){
                if(sbn.resultSet instanceof SelectNode){
                    updateTableNumbers((SelectNode)sbn.resultSet, diff, level);
                }
                if(sbn.leftOperand != null && sbn.leftOperand instanceof ColumnReference){
                    ColumnReference ref = (ColumnReference)sbn.leftOperand;
                    if (ref.getSourceLevel() >= level) {
                        ref.setTableNumber(ref.getTableNumber() + diff);
                    }
                }
            }
        }
        if(node.getWhereSubquerys() != null){
            for(SubqueryNode sbn : node.getWhereSubquerys()){
                if(sbn.resultSet instanceof SelectNode){
                    updateTableNumbers((SelectNode)sbn.resultSet, diff, level);
                }
                if(sbn.leftOperand != null && sbn.leftOperand instanceof ColumnReference){
                    ColumnReference ref = (ColumnReference)sbn.leftOperand;
                    if (ref.getSourceLevel() >= level) {
                        ref.setTableNumber(ref.getTableNumber() + diff);
                    }
                }
            }
        }
        if(node.havingSubquerys != null){
            for(SubqueryNode sbn : node.havingSubquerys){
                if(sbn.resultSet instanceof SelectNode){
                    updateTableNumbers((SelectNode)sbn.resultSet, diff, level);
                }
                if(sbn.leftOperand != null && sbn.leftOperand instanceof ColumnReference){
                    ColumnReference ref = (ColumnReference)sbn.leftOperand;
                    if (ref.getSourceLevel() >= level) {
                        ref.setTableNumber(ref.getTableNumber() + diff);
                    }
                }
            }
        }
        if (node.selectAggregates != null) {
            for (AggregateNode agn : node.selectAggregates) {
                if (agn.operand instanceof ColumnReference) {
                    ColumnReference ref = (ColumnReference) agn.operand;
                    if (ref.getSourceLevel() >= level) {
                        ref.setTableNumber(ref.getTableNumber() + diff);
                    }
                }
            }
        }
        if (node.whereAggregates != null) {
            for (AggregateNode agn : node.whereAggregates) {
                if (agn.operand instanceof ColumnReference) {
                    ColumnReference ref = (ColumnReference) agn.operand;
                    if (ref.getSourceLevel() >= level) {
                        ref.setTableNumber(ref.getTableNumber() + diff);
                    }
                }
            }
        }
        if (node.havingAggregates != null) {
            for (AggregateNode agn : node.havingAggregates) {
                if (agn.operand instanceof ColumnReference) {
                    ColumnReference ref = (ColumnReference) agn.operand;
                    if (ref.getSourceLevel() >= level) {
                        ref.setTableNumber(ref.getTableNumber() + diff);
                    }
                }
            }
        }
        if (node.whereClause != null) {
            updateWhereClauseColRef(node.whereClause, diff, level);
        }

        if (node.orderByList != null) {
            for (OrderedColumn obc : node.orderByList) {
                if (obc.getColumnExpression() instanceof ColumnReference) {
                    ColumnReference ref = (ColumnReference) obc.getColumnExpression();
                    if (ref.getSourceLevel() >= level) {
                        ref.setTableNumber(ref.getTableNumber() + diff);
                    }
                }

            }
        }
        for (ResultColumn rc : node.resultColumns) {
            if (rc.expression instanceof ColumnReference) {
                ColumnReference ref = (ColumnReference) rc.expression;
                if (ref.getSourceLevel() >= level) {
                    ref.setTableNumber(ref.getTableNumber() + diff);
                }
            }
        }


    }

    private String getSubqueryAlias() {
        String ret = "FlattenedSubquery" + flattenedSubquery;
        flattenedSubquery++;
        return ret;
    }

    private static void updateWhereClauseColRef(ValueNode node, int diff, int level) {
        if (node instanceof BinaryRelationalOperatorNode) {
            ValueNode left = ((BinaryRelationalOperatorNode) node).getLeftOperand();
            ValueNode right = ((BinaryRelationalOperatorNode) node).getRightOperand();
            if (left instanceof ColumnReference &&
                    ((ColumnReference) left).getSourceLevel() >= level) {
                ((ColumnReference) left).setTableNumber(((ColumnReference) left).getTableNumber() + diff);
            }
            else if (left instanceof CastNode){
                ValueNode opn = ((CastNode) left).castOperand;
                if(opn instanceof ColumnReference && ((ColumnReference) opn).getSourceLevel() >= level){
                    ((ColumnReference) opn).setTableNumber(((ColumnReference) opn).getTableNumber() + diff);
                }
            }
            if (right instanceof ColumnReference &&
                    ((ColumnReference) right).getSourceLevel() >= level) {
                ((ColumnReference) right).setTableNumber(((ColumnReference) right).getTableNumber() + diff);
            }
            else if (right instanceof CastNode){
                ValueNode opn = ((CastNode) right).castOperand;
                if(opn instanceof ColumnReference && ((ColumnReference) opn).getSourceLevel() >= level){
                    ((ColumnReference) opn).setTableNumber(((ColumnReference) opn).getTableNumber() + diff);
                }
            }
        } else if (node instanceof AndNode) {
            updateWhereClauseColRef(((AndNode) node).getLeftOperand(), diff, level);
            updateWhereClauseColRef(((AndNode) node).getRightOperand(), diff, level);
        } else if (node instanceof UnaryOperatorNode) {
            ValueNode opn = ((UnaryOperatorNode) node).operand;
            if (opn instanceof ColumnReference &&
                    ((ColumnReference) opn).getSourceLevel() >= level) {
                ((ColumnReference) opn).setTableNumber(((ColumnReference) opn).getTableNumber() + diff);
                ((UnaryOperatorNode) node).operand = opn;
            }
        } else if (node instanceof TernaryOperatorNode) {
            ValueNode receiver = ((TernaryOperatorNode) node).receiver;
            ValueNode left = ((TernaryOperatorNode) node).leftOperand;
            ValueNode right = ((TernaryOperatorNode) node).rightOperand;
            if (receiver instanceof ColumnReference && ((ColumnReference) receiver).getSourceLevel() >= level) {
                ((ColumnReference) receiver).setTableNumber(((ColumnReference) receiver).getTableNumber() + diff);
            }
            if (left instanceof ColumnReference && ((ColumnReference) left).getSourceLevel() >= level) {
                ((ColumnReference) left).setTableNumber(((ColumnReference) left).getTableNumber() + diff);
            }
            if (right instanceof ColumnReference && ((ColumnReference) right).getSourceLevel() >= level) {
                ((ColumnReference) right).setTableNumber(((ColumnReference) right).getTableNumber() + diff);
            }

            ((TernaryOperatorNode) node).receiver = receiver;
            ((TernaryOperatorNode) node).leftOperand = left;
            ((TernaryOperatorNode) node).rightOperand = right;
        } else if (node instanceof CastNode){
            ValueNode opn = ((CastNode) node).castOperand;
            if(opn instanceof ColumnReference && ((ColumnReference) opn).getSourceLevel() >= level){
                ((ColumnReference) opn).setTableNumber(((ColumnReference) opn).getTableNumber() + diff);
            }
        }
    }

}