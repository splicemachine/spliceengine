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
     * @param keyColumns the window function definition's over() clause columns
     *                   (partition and over by clauses).
     * @throws StandardException
     */
    public WindowColumnMapping(ResultColumnList parentRCL, List<OrderedColumn> keyColumns)
        throws StandardException {
        parentRefs = createAndAssociateParentRefs(parentRCL, keyColumns);
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
     * @param keyColumns the partition and over by columns in the window over() clause.
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
     * Reset the the key column position numbers of the parent referenced key columns.
     * Also reset the position numbers of the key columns in the over() clause to keep
     * them in sync.
     * <p/>
     * This is done when projecting columns from the WindowResultSetNode.
     * @param parent the parent column reference for which to reset the referenced key
     *               columns.
     * @param keyColumns the window definition's key columns. Any matching keys will
     *                   have their column positions reset.
     * @param columnPosition the new column position of the referenced keys.
     * @throws StandardException
     */
    public static void resetOverColumnsPositionByParent(ParentRef parent, List<OrderedColumn> keyColumns,
                                                        int columnPosition) throws StandardException {
        for (OrderedColumn child : parent.children) {
            resetColumnPosition(child, columnPosition);
            OrderedColumn key = findMatchingKey(keyColumns, child);
            assert key != null: "No matching key col for "+child;
            resetColumnPosition(key, columnPosition);
        }
    }

    /**
     * Reset the column position of the <code>keyCol</code> and synchronize any matching
     * <code>keyColumns</code> during projection of the WindowResultSetNode.
     * @param keyCol a key column reference to a parent column reference which we are
     *               projecting in a different column.
     * @param keyColumns the window definition's over() clause key columns to reset any
     *                   matching key's position.
     * @param columnPosition the new key columns position in the projection.
     * @throws StandardException
     */
    public static void resetOverColumnsPositionByKey(OrderedColumn keyCol, List<OrderedColumn> keyColumns,
                                                     int columnPosition) throws StandardException {
        resetColumnPosition(keyCol, columnPosition);
        OrderedColumn key = findMatchingKey(keyColumns, keyCol);
        assert key != null: "No matching key col for "+keyCol;
        resetColumnPosition(key, columnPosition);
    }

    //========================================
    // Utilities
    //========================================

    private List<ParentRef> createAndAssociateParentRefs(ResultColumnList parentRCL,
                                                         List<OrderedColumn> keyColumns) throws StandardException {
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
                    for (OrderedColumn key : keyColumns) {
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

    private static OrderedColumn findMatchingKey(List<OrderedColumn> keyColumns, OrderedColumn child) {
        for (OrderedColumn oc : keyColumns) {
            if (oc.equals(child)) {
                return oc;
            }
        }
        return null;
    }

    private static void resetColumnPosition(OrderedColumn column, int columnPosition) {
        ValueNode node = column.getColumnExpression();
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
    }
}
