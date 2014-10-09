package org.apache.derby.impl.sql.compile;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.derby.iapi.error.StandardException;

/**
 * Used only inside of {@link org.apache.derby.impl.sql.compile.WindowResultSetNode}, this class maps
 * columns from the SELECT clause to any matching columns in the window definition's OVER() clause.
 *
 * @author Jeff Cunningham
 *         Date: 10/8/14
 */
public class WindowColumnMapping {
    private final Map<String, List<ParentRef>> parentRefs;
    private final Map<String, List<OverRef>> overRefs;
    private final List<KeyRef> keyRefs;

    public WindowColumnMapping(ResultColumnList parentRCL, Partition partition, OrderByList orderByList) throws StandardException {
        overRefs = createOverRefs(partition, orderByList);
        parentRefs = createAndAssociateParentRefs(parentRCL, overRefs);
        keyRefs = createAndAssociateKeyRefs(overRefs);
    }

    public List<ValueNode> getParentColumns() {
        List<ValueNode> parents = new ArrayList<ValueNode>(parentRefs.size());
        for (Map.Entry<String, List<ParentRef>> parentRefEntry : parentRefs.entrySet()) {
            for (ParentRef parentRef : parentRefEntry.getValue()) {
                parents.add(parentRef.ref);
            }
        }
        return parents;
    }

    /**
     * Only parent columns of type ColumnReference or VirtualColumnNode. No AggregateNodes, etc.
     * @return reference columns
     */
    public List<ValueNode> getParentReferenceColumns() {
        List<ValueNode> parents = new ArrayList<ValueNode>(parentRefs.size());
        for (Map.Entry<String, List<ParentRef>> parentRefEntry : parentRefs.entrySet()) {
            for (ParentRef parentRef : parentRefEntry.getValue()) {
                ValueNode node = parentRef.ref;
                if (ColumnReference.class.isInstance(node) || VirtualColumnNode.class.isInstance(node)) {
                    parents.add(node);
                }
            }
        }
        return parents;
    }

    public List<OrderedColumn> getPartitionColumns() {
        return getOrderedNodes(true);
    }

    public List<OrderedColumn> getOrderByColumns() {
        return getOrderedNodes(false);
    }

    public List<OrderedColumn> getKeyColumns() {
        List<OrderedColumn> keyColumns = new ArrayList<OrderedColumn>(keyRefs.size());
        if (! keyRefs.isEmpty()) {
            for (KeyRef keyRef : keyRefs) {
                OverRef overRef = keyRef.getFirstOverRef();
                if (overRef == null) {
                    throw new RuntimeException("Key column is an orphan.");
                }
                keyColumns.add(overRef.ref);
            }
        }
        return keyColumns;
    }

    public List<ValueNode> getMissingKeyNodes() {
        List<ValueNode> missingKeys = new ArrayList<ValueNode>(keyRefs.size());
        if (! keyRefs.isEmpty()) {
            for (KeyRef keyRef : keyRefs) {
                if (! keyRef.hasParent()) {
                    OverRef overRef = keyRef.getFirstOverRef();
                    if (overRef == null) {
                        throw new RuntimeException("Key column is an orphan.");
                    }
                    missingKeys.add(overRef.ref.getColumnExpression());
                }
            }
        }
        return missingKeys;
    }

    public void resetOverColumnsPositionByParent(ValueNode parent, int columnPosition) {
        // there can be more than one occurrence of the same col in select
        for (ParentRef parentRef : findParentRefs(parent)) {
            if (parentRef.hasChildren()) {
                for (OverRef overRef : parentRef.children) {
                    resetColumnPosition(overRef.ref, columnPosition);
                }
            }
        }
    }

    public void resetOverColumnsPositionByOverColumn(ValueNode overColumn, int columnPosition) {
        // there can be more than one over column if, for instance, the same column
        // is listed in the partition and order by clauses
        for (OverRef overRef : findOverRefs(overColumn)) {
            resetColumnPosition(overRef.ref, columnPosition);
        }
    }

    //========================================
    // Utilities
    //========================================

    private Map<String,List<OverRef>> createOverRefs(Partition partition, OrderByList orderByList) {
        int partitionSize = (partition != null ? partition.size() : 0);
        int orderByListSize = (orderByList != null ? orderByList.size() : 0);
        Map<String,List<OverRef>> oveRefMap = new LinkedHashMap<String, List<OverRef>>(partitionSize+orderByListSize);
        // partition columns
        for (int i=0; i<partitionSize; i++) {
            OrderedColumn column = (OrderedColumn) partition.elementAt(i);
            String id = createID(column.getColumnExpression());
            List<OverRef> overRefsList = oveRefMap.get(id);
            if (overRefsList == null) {
                overRefsList = new ArrayList<OverRef>(5);
                oveRefMap.put(id, overRefsList);
            }
            overRefsList.add(new OverRef(column, id, true));
        }
        // order by columns
        for (int i=0; i<orderByListSize; ++i) {
            OrderedColumn column = (OrderedColumn) orderByList.elementAt(i);
            String id = createID(column.getColumnExpression());
            List<OverRef> overRefsList = oveRefMap.get(id);
            if (overRefsList == null) {
                overRefsList = new ArrayList<OverRef>(5);
                oveRefMap.put(id, overRefsList);
            }
            overRefsList.add(new OverRef(column, id, false));
        }
        return oveRefMap;
    }

    private Map<String,List<ParentRef>> createAndAssociateParentRefs(ResultColumnList parentRCL,
                                                                     Map<String,List<OverRef>> overRefs) throws StandardException {
        // FIXME: there's gotta be a better way to match column expressions...
        int parentRCLSize = (parentRCL != null ? parentRCL.size() : 0);
        Map<String,List<ParentRef>> parentRefs1 = new LinkedHashMap<String, List<ParentRef>>(parentRCLSize);
        for (int i=0; i<parentRCLSize; i++) {
            ValueNode node = parentRCL.getResultColumn(i+1).getExpression();
            String id = createID(node);
            ParentRef parentRef = new ParentRef(node, id);
            if (node instanceof ColumnReference || node instanceof VirtualColumnNode) {
                // limit matching nodes to one of the types of the over columns
                // TODO: what about arbitrary column expressions in over()? Ruling out aggregate expressions?
                for (List<OverRef> overRefList: overRefs.values()) {
                    for (OverRef overRef : overRefList) {
                        if (equivalantToOverColumn(node, overRef.ref.getColumnExpression())) {
                            parentRef.children.add(overRef);
                            overRef.parentRefs.add(parentRef);
                        }
                    }
                }
            }
            List<ParentRef> parentRefList = parentRefs1.get(id);
            if (parentRefList == null) {
                parentRefList = new ArrayList<ParentRef>(5);
                parentRefs1.put(id, parentRefList);
            }
            parentRefList.add(parentRef);
        }
        return parentRefs1;
    }

    private List<KeyRef> createAndAssociateKeyRefs(Map<String, List<OverRef>> overRefs) {
        List<KeyRef> keyRefs1 = new ArrayList<KeyRef>(overRefs.size());
        Map<String,List<OverRef>> keys = new LinkedHashMap<String,List<OverRef>>(overRefs.size());
        for (List<OverRef> overRefList : overRefs.values()) {
            for (OverRef overRef : overRefList) {
                List<OverRef> refs = keys.get(overRef.id);
                if (refs == null) {
                    refs = new ArrayList<OverRef>(5);
                    keys.put(overRef.id, refs);
                }
                refs.add(overRef);
            }
        }
        for (Map.Entry<String,List<OverRef>> keyEntries : keys.entrySet()) {
            KeyRef keyRef = new KeyRef(keyEntries.getKey());
            for (OverRef overRef : keyEntries.getValue()) {
                keyRef.overRefs.add(overRef);
            }
            keyRefs1.add(keyRef);
        }
        return keyRefs1;
    }

    private List<OrderedColumn> getOrderedNodes(boolean isInPartition) {
        List<OrderedColumn> orderedColumns = new ArrayList<OrderedColumn>(overRefs.size());
        for (List<OverRef> overRefList : overRefs.values()) {
            for (OverRef overRef : overRefList) {
                if (overRef.inPartition == isInPartition) {
                    orderedColumns.add(overRef.ref);
                }
            }
        }
        return orderedColumns;
    }

    private List<ParentRef> findParentRefs(ValueNode parent) {
        List<ParentRef> matchingRefs = new ArrayList<ParentRef>(2);
        String id = createID(parent);
        for (ParentRef parentRef : parentRefs.get(id)) {
            matchingRefs.add(parentRef);
        }
        return matchingRefs;
    }

    private List<OverRef> findOverRefs(ValueNode overColumn) {
        List<OverRef> matchingRefs = new ArrayList<OverRef>(5);
        String id = createID(overColumn);
        for (OverRef overRef : overRefs.get(id)) {
            matchingRefs.add(overRef);
        }
        return matchingRefs;
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
     * Create an ID for any given node by finding our way to the
     * bottom of the reference tree. That should disambiguate
     * between columns that are effectively the same.<br/>
     * Note we can't use column position because we're potentially
     * changing that during the lifetime of this object.
     *
     * @param node the column to identify
     * @return the definitive column identifier.
     */
    private String createID(ValueNode node) {
        if (node == null) return null;
        if (node instanceof BaseColumnNode) return node.getColumnName();
        ResultColumn src = node.getSourceResultColumn();
        if (src == null) {
            if (node instanceof ResultColumn) {
                return ((ResultColumn)node).getName();
            } else if (node instanceof AggregateNode) {
                return ((AggregateNode)node).getAggregateName();
            }
            // resetting src to recurs
            else if (node instanceof ColumnReference) {
                if (((ColumnReference)node).getGeneratedToReplaceAggregate() ||
                    ((ColumnReference)node).getGeneratedToReplaceWindowFunctionCall()) {
                    return node.getColumnName();
                }
                src = ((ColumnReference) node).getSource();
            } else if (node instanceof VirtualColumnNode) {
                src = ((VirtualColumnNode) node).getSourceColumn();
            } else {
                throw new RuntimeException("Node is of type: "+node);
            }
        }
        // recurs until we hit the bottom
        String name = createID(src);
        if (name == null) {
            throw new RuntimeException("Unable to create ID for node: "+node.getColumnName());
        }
        return name;
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
    private boolean equivalantToOverColumn(ValueNode recursNode, ValueNode ref) throws StandardException {
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
            equivalantToOverColumn(((ResultColumn) rc).getExpression(), ref);
    }

    //========================================
    // Internal classes
    //========================================

    private static abstract class Ref {
        final String id;
        Ref(String id) {
            this.id = id;
        }
        @Override
        public int hashCode() {
            return this.id.hashCode();
        }
        @Override
        public boolean equals(Object o) {
            return this == o || !(o == null || getClass() != o.getClass()) && id.equals(((Ref) o).id);
        }
    }

    private static class ParentRef extends Ref {
        final ValueNode ref;
        final List<OverRef> children;

        ParentRef(ValueNode ref, String id) {
            super(id);
            children = new ArrayList<OverRef>(10);
            this.ref = ref;
        }

        boolean hasChildren() {
            return (! children.isEmpty());
        }
    }

    private static class OverRef extends Ref {
        final OrderedColumn ref;
        final boolean inPartition;
        final List<ParentRef> parentRefs;

        OverRef(OrderedColumn ref, String id, boolean inPartition) {
            super(id);
            parentRefs = new ArrayList<ParentRef>(10);
            this.ref = ref;
            this.inPartition = inPartition;
        }

        boolean hasParent() {
            return (! parentRefs.isEmpty());
        }
    }

    private static class KeyRef extends Ref {
        final List<OverRef> overRefs;

        KeyRef(String firstRefId) {
            super(firstRefId);
            overRefs = new ArrayList<OverRef>(10);
        }

        boolean hasParent() {
            for (OverRef overRef : overRefs) {
                if (overRef.hasParent()) {
                    return true;
                }
            }
            return false;
        }

        public OverRef getFirstOverRef() {
            OverRef overRef = null;
            if (! overRefs.isEmpty()) {
                overRef = overRefs.get(0);
            }
            return overRef;
        }
    }
}
