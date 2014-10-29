package org.apache.derby.impl.sql.compile;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.derby.iapi.error.StandardException;

/**
 * Used only inside of {@link org.apache.derby.impl.sql.compile.WindowResultSetNode}, this class maps
 * columns from the SELECT clause to any matching columns in the window definition's OVER() clause,
 * the key columns.
 *
 * @author Jeff Cunningham
 *         Date: 10/8/14
 */
public class WindowColumnMapping {
    private final List<ParentRef> parentRefs;

    /**
     * Create parent references and attach any key columns from the window function
     * definition's over() clause.
     * @param parentRCL the parent result set column list (the select clause).
     * @param overColumns the window function definition's over() clause columns
     *                   (partition and over by clauses).
     * @throws StandardException
     */
    public WindowColumnMapping(ResultColumnList parentRCL, List<OrderedColumn> overColumns)
        throws StandardException {
        parentRefs = createAndAssociateParentRefs(parentRCL, overColumns);
    }

    /**
     * Get the parent result set columns which may or may not have references to
     * window function key columns (the over() clause columns).
     * @return all columns from the parent result set (the select clause).
     */
    public List<ParentRef> getParentColumns() {
        return parentRefs;
    }

    /**
     * Find any <code>keyColumns</code> that are not referenced by any <code>parentRef</code>s.
     * <p/>
     * These key columns will have to be added to the WindowResultSetNode's projection so
     * that they will be available as row keys to sort rows during the WindowOperation.
     * @param parentRefs the parent result set columns that might be carrying references
     *                   to key columns in the window over() clause.
     * @param keyColumns the de-duplicated partition and over by columns (the key columns)
     *                   in the window over() clause.
     * @return any over() clause key columns that are not referenced by parent columns.
     * May be empty but not null.
     */
    public static List<OrderedColumn> findMissingKeyNodes(List<ParentRef> parentRefs,
                                                      List<OrderedColumn> keyColumns) {
        Set<OrderedColumn> parentReferencedOCs = collectChildren(parentRefs);
        List<OrderedColumn> missingKeys = new ArrayList<OrderedColumn>(keyColumns.size());
        for (OrderedColumn oc : keyColumns) {
            if (! parentReferencedOCs.contains(oc)) {
                missingKeys.add(oc);
            }
        }
        return missingKeys;
    }

    /**
     * Determine if an operand column is not present in either the children of the
     * <code>parentRefs</code> or in the <code>keyColumns</code>.
     * @param expression the column expression for which we're searching.
     * @param parentRefs the parent result set columns that might be carrying references
     *                   to operand columns.
     * @param keyColumns the de-duplicated partition and over by columns (the key columns)
     *                   in the window over() clause.
     * @return <code>true</code> if the given <code>expression</code> is not present in
     * either <code>parentRefs</code> or <code>keyColumns</code>.
     */
    public static boolean operandMissing(ValueNode expression, List<ParentRef> parentRefs,
                                         List<OrderedColumn> keyColumns) {
        Set<ValueNode> referencedNodes = collectChildExpressions(parentRefs);
        for (OrderedColumn oc : keyColumns) {
            // TODO: this will not find RC->CR-RC->... columns. We don't search for these presently.
            referencedNodes.add(oc.getColumnExpression());
        }
        return ! referencedNodes.contains(expression);
    }

    /**
     * Find <code>keyCol</code> in <code>overColumns</code> and replace its expression
     * with the given expression.
     * @param keyCol the column for which we want to replace its expression.
     * @param expression the expression we want to use for replacement.
     * @param overColumns the window function definition's over() clause columns
     *                   (partition and over by clauses).
     * @throws StandardException
     */
    public static void replaceColumnExpression(OrderedColumn keyCol, ValueNode expression,
                                               List<OrderedColumn> overColumns) throws StandardException {
        List<OrderedColumn> keys = findMatchingKeys(keyCol, overColumns);
        assert ! keys.isEmpty(): "No matching key col for "+keyCol;
        for (OrderedColumn key : keys) {
            key.init(expression);
        }
    }

    /**
     * Reset the the key column position numbers of the parent referenced key columns.
     * Also reset the position numbers of the key columns in the over() clause to keep
     * them in sync.
     * <p/>
     * This is done when projecting columns from the WindowResultSetNode.
     * @param parent the parent column reference for which to reset the referenced key
     *               columns.
     * @param overColumns the window definition's over() clause columns (all). All matching
     *                   column instances will have their column positions reset.
     * @param columnPosition the new column position of the referenced keys.
     * @throws StandardException
     */
    public static void resetOverColumnsPositionByParent(ParentRef parent, List<OrderedColumn> overColumns,
                                                        int columnPosition) throws StandardException {
        for (OrderedColumn child : parent.children) {
            resetColumnPosition(child, columnPosition);
            // keep over clause columns in sync
            resetOverColumnsPositionByKey(child, overColumns, columnPosition);
        }
    }

    /**
     * Reset the column position of the <code>keyCol</code> and synchronize any matching
     * <code>overColumns</code> during projection of the WindowResultSetNode.
     * @param keyCol a key column reference to a parent column reference which we are
     *               projecting in a different column.
     * @param overColumns the window definition's over() clause columns (all). All matching
     *                   column instances will have their column positions reset.
     * @param columnPosition the new key column position in the projection.
     * @throws StandardException
     */
    public static void resetOverColumnsPositionByKey(OrderedColumn keyCol, List<OrderedColumn> overColumns,
                                                     int columnPosition) throws StandardException {
        resetColumnPosition(keyCol, columnPosition);
        List<OrderedColumn> keys = findMatchingKeys(keyCol, overColumns);
        assert ! keys.isEmpty(): "No matching key col for "+keyCol;
        for (OrderedColumn key : keys) {
            resetColumnPosition(key, columnPosition);
        }
    }

    /**
     * Reset the column position of the <code>operand</code> and synchronize any matching
     * <code>operands</code> during projection of the WindowResultSetNode.
     * @param operand a function operand column reference to a parent column reference which we are
     *               projecting in a different column.
     * @param operands the window definition's operand columns (all). All matching
     *                   column instances will have their column positions reset.
     * @param columnPosition the new column position in the projection.
     * @throws StandardException
     */
    public static void resetOverColumnsPositionByKey(ValueNode operand, ValueNode[] operands,
                                                     int columnPosition) throws StandardException {
        resetColumnPosition(operand, columnPosition);
        List<ValueNode> missingOperands = findMatchingKeys(operand, operands);
        assert ! missingOperands.isEmpty(): "No matching operands col for "+operand;
        for (ValueNode missingOperand : missingOperands) {
            resetColumnPosition(missingOperand, columnPosition);
        }
    }

    //========================================
    // Utilities
    //========================================

    private List<ParentRef> createAndAssociateParentRefs(ResultColumnList parentRCL,
                                                         List<OrderedColumn> overColumn) throws StandardException {
        int parentRCLSize = (parentRCL != null ? parentRCL.size() : 0);
        List<ParentRef> parentRefList = new ArrayList<ParentRef>(parentRCLSize);
        for (int i=0; i<parentRCLSize; i++) {
            ResultColumn rc = parentRCL.getResultColumn(i+1);
            ValueNode node = rc.getExpression();
            String id = rc.exposedName;
            ParentRef parentRef = new ParentRef(rc, id);
            parentRefList.add(parentRef);
            if (node instanceof ColumnReference || node instanceof VirtualColumnNode) {
                // limit matching parent expressions to one of the types of the key columns
                for (OrderedColumn key : overColumn) {
                    if (equivalentToOverColumn(node, key.getColumnExpression())) {
                        parentRef.children.add(key);
                    }
                }
            }
        }
        return parentRefList;
    }

    private static Set<OrderedColumn> collectChildren(List<ParentRef> parentRefs) {
        Set<OrderedColumn> parentReferencedOCs = new LinkedHashSet<OrderedColumn>(parentRefs.size());
        for (ParentRef pr : parentRefs) {
            parentReferencedOCs.addAll(pr.children);
        }
        return parentReferencedOCs;
    }

    private static Set<ValueNode> collectChildExpressions(List<ParentRef> parentRefs) {
        Set<ValueNode> parentReferencedOCs = new LinkedHashSet<ValueNode>(parentRefs.size());
        for (ParentRef pr : parentRefs) {
            parentReferencedOCs.addAll(pr.getChildExpressions());
        }
        return parentReferencedOCs;
    }

    private static List<OrderedColumn> findMatchingKeys(OrderedColumn child, List<OrderedColumn> keyColumns) {
        List<OrderedColumn> canContainDuplicates = new ArrayList<OrderedColumn>(keyColumns.size());
        for (OrderedColumn oc : keyColumns) {
            boolean equiv = false;

            // OrderedColumns can either be instances of GroupByColumn or OrderByColumn
            // Either both are the same type, compare directly, ...
            if ((child instanceof GroupByColumn && oc instanceof GroupByColumn) ||
                (child instanceof OrderByColumn && oc instanceof OrderByColumn)) {
                    equiv = oc.equals(child);
            } else {
                // or one of each, compare their expressions
                equiv = child.getColumnExpression().equals(oc.getColumnExpression());
            }

            if (equiv) {
                canContainDuplicates.add(oc);
            }
        }
        return canContainDuplicates;
    }

    private static List<ValueNode> findMatchingKeys(ValueNode child, ValueNode[] keyColumns) {
        List<ValueNode> canContainDuplicates = new ArrayList<ValueNode>(keyColumns.length);
        for (ValueNode oc : keyColumns) {
            if (child.equals(oc)) {
                canContainDuplicates.add(oc);
            }
        }
        return canContainDuplicates;
    }

    private static void resetColumnPosition(OrderedColumn column, int columnPosition) {
        resetColumnPosition(column.getColumnExpression(), columnPosition);
        column.setColumnPosition(columnPosition);
    }

    private static void resetColumnPosition(ValueNode node, int columnPosition) {
        if (node instanceof ColumnReference) {
            ((ColumnReference) node).setColumnNumber(columnPosition);
        } else if (node instanceof VirtualColumnNode) {
            ((VirtualColumnNode) node).columnId = columnPosition;
            ResultColumn rc = node.getSourceResultColumn();
            if (rc != null) {
                rc.setVirtualColumnId(columnPosition);
            }
        }
    }

    /**
     * Check to see if a given query tree node is equivalent to an order by column expression.<br/>
     * The query tree node, <code>recursNode</code>, hierarchy is descended until a match, if any,
     * is made.
     * <p/>
     * Note that currently the calling code limits the <code>recursNode</code> type to be either
     * a <code>ColumnReference</code> node or a <code>VirtualColumnNode</code>. There are three
     * reasons for this; the calling code is only concerned with matches of this type (ORDER BY
     * columns), for performance reasons in the calling code, and the fact that the Derby query
     * node hierarchy is so "diverse" that the variety of methods to call and the number of return
     * types so great, in order to descend the hierarchy, would make this method too cumbersome to
     * test and maintain. Keep is simple.
     *
     * @param recursNode the node in in question whose hierarchy is recursively descended.
     * @param ref the node we want to match with.
     * @return <code>true</code> if the two nodes point to the same underlying column.
     * @throws StandardException thrown by <code>node1.isEquivalent(node2)</code>.
     */
    private boolean equivalentToOverColumn(ValueNode recursNode, ValueNode ref) throws StandardException {
        if (recursNode == null) return false;
        if (ref.isEquivalent(recursNode)) return true;

        ValueNode rc = null;
        if (recursNode instanceof ColumnReference) {
            rc = recursNode.getSourceResultColumn();
        } else if (recursNode instanceof VirtualColumnNode) {
            rc = ((VirtualColumnNode)recursNode).getSourceColumn();
        }
        return rc != null &&
            ((ResultColumn) rc).getExpression() != null &&
            equivalentToOverColumn(((ResultColumn) rc).getExpression(), ref);
    }

    //=============================================
    // Nested class. Used in WindowResultSetNode
    //=============================================

    public static class ParentRef {
        final String id;
        final ResultColumn col;
        final ValueNode ref;
        final List<OrderedColumn> children;

        ParentRef(ResultColumn col, String id) {
            this.id = id;
            children = new ArrayList<OrderedColumn>(10);
            this.col = col;
            this.ref = col.getExpression();
        }

        public List<ValueNode> getChildExpressions() {
            List<ValueNode> childrenExps = new ArrayList<ValueNode>(10);
            for (OrderedColumn oc : children) {
                childrenExps.add(oc.getColumnExpression());
            }
            return childrenExps;
        }
    }
}
