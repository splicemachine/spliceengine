package com.splicemachine.db.impl.sql.compile;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

/**
 * This class aggregates objects that make up a Window Over() clause.<br/>
 * This class is immutable and so functions with equivalent over() clauses
 * can share a single instance.<br/>
 * This also allows window functions that are defined over the same space
 * to be grouped and processed together.
 *
 * @author Jeff Cunningham
 *         Date: 9/8/14
 */
public class OverClause extends QueryTreeNode {

    /**
     * The partition by list if the window definition contains a <window partition
     * clause>, else null.
     */
    private final Partition partition;

    /**
     * The order by list if the window definition contains a <window order
     * clause>, else null.
     */
    private final OrderByList orderByClause;

    /**
     * The window frame.
     */
    private final WindowFrameDefinition frameDefinition;

    private final int hashCode;

    public Partition getPartition() {
        return partition;
    }

    public OrderByList getOrderByClause() {
        return orderByClause;
    }

    /**
     * Use this method exclusively to get the key columns for a given window
     * definition. These key columns will be used to build a row key to sort
     * rows incoming to the window function.
     * <p/>
     * Key columns are comprised of the concatenation of Partition columns and
     * OrderBy columns, in the form of this <b>set</b> {[P1,P2,...][O1,O2,...]}, where Pn
     * is Partition element n and On is OrderBy element n. As a set, duplicate columns are
     * not included.  This is fine since we don't need to "re-sort" rows on a column.
     *
     * @return the ordered set of de-duplicated key columns.
     */
    public List<OrderedColumn> getKeyColumns() {
        int partitionSize = (partition != null ? partition.size() : 0);
        int orderByListSize = (orderByClause != null ? orderByClause.size() : 0);
        List<OrderedColumn> keyCols = new ArrayList<OrderedColumn>(partitionSize+orderByListSize);
        Set<ValueNode> addedNodes = new HashSet<ValueNode>(partitionSize+orderByListSize);

        // partition columns
        for (int i=0; i<partitionSize; i++) {
            OrderedColumn oc = (OrderedColumn) partition.elementAt(i);
            if (! addedNodes.contains(oc.getColumnExpression())) {
                keyCols.add(oc);
                addedNodes.add(oc.getColumnExpression());
            }
        }

        // order by columns
        for (int i=0; i<orderByListSize; ++i) {
            OrderedColumn oc = (OrderedColumn) orderByClause.elementAt(i);
            if (! addedNodes.contains(oc.getColumnExpression())) {
                keyCols.add(oc);
                addedNodes.add(oc.getColumnExpression());
            }
        }

        return keyCols;
    }

    /**
     * Use this method to get all columns, with possible duplicates,
     *
     * @return the list of all over() columns in order {[P1,P2,...][O1,O2,...]},
     * where Pn is a Partition column and On is an OrderBy column.
     */
    public List<OrderedColumn> getOverColumns() {
        int partitionSize = (partition != null ? partition.size() : 0);
        int orderByListSize = (orderByClause != null ? orderByClause.size() : 0);
        List<OrderedColumn> keyCols = new ArrayList<OrderedColumn>(partitionSize+orderByListSize);

        // partition columns
        for (int i=0; i<partitionSize; i++) {
            keyCols.add((OrderedColumn) partition.elementAt(i));
        }

        // order by columns
        for (int i=0; i<orderByListSize; ++i) {
            keyCols.add((OrderedColumn) orderByClause.elementAt(i));
        }

        return keyCols;
    }

    public WindowFrameDefinition getFrameDefinition() {
        return frameDefinition;
    }

    /**
     * java.lang.Object override.
     * @see QueryTreeNode#toString
     */
    @Override
    public String toString() {
        return ("over:\n" +
            "  partition: " + partition + "\n" +
            "  orderby: " + printOrderByList() + "\n" +
            "  "+frameDefinition + "\n");
    }


    /**
     * QueryTreeNode override. Prints the sub-nodes of this object.
     * @see QueryTreeNode#printSubNodes
     *
     * @param depth     The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        if (SanityManager.DEBUG) {
            super.printSubNodes(depth);

            if (partition != null) {
                printLabel(depth, "partition: " + depth);
                partition.treePrint(depth + 1);
            }

            if (orderByClause != null) {
                printLabel(depth, "orderByList: " + depth);
                orderByClause.treePrint(depth + 1);
            }

            if (frameDefinition != null) {
                printLabel(depth, "frame: " + depth);
                frameDefinition.treePrint(depth + 1);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OverClause that = (OverClause) o;

        try {
            return isEquivalent(that);
        } catch (StandardException e) {
            // ignore
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    private int calculateHashCode() {
        int result = partition != null ? partition.hashCode() : 0;
        result = 31 * result + (orderByClause != null ? orderByHashcode() : 0);
        result = 31 * result + frameDefinition.hashCode();
        return result;
    }

    private int orderByHashcode() {
        int result = 31;
        for (int i=0; i<orderByClause.size(); i++) {
            result = 31 * result +
                (this.orderByClause.getOrderByColumn(i).getColumnExpression().getColumnName() == null ? 0 :
                    this. orderByClause.getOrderByColumn(i).getColumnExpression().getColumnName().hashCode());
        }
        return result;
    }

    /**
     * @return true if the window specifications are equivalent; no need to create
     * more than one window then.
     */
    public boolean isEquivalent(OverClause other) throws StandardException {
        if (this == other) return true;
        if (other == null) return false;

        if (frameDefinition != null ? !frameDefinition.isEquivalent(other.frameDefinition) : other.frameDefinition != null) return false;
        if (orderByClause != null ? !isEquivalent(orderByClause, other.orderByClause) : other.orderByClause != null) return false;
        if (partition != null ? !partition.isEquivalent(other.partition) : other.partition != null) return false;

        return true;
    }

    private boolean isEquivalent(OrderByList thisOne, OrderByList thatOne) throws StandardException {
        // implemented to compare OrderByLists for equivalence
        if (thisOne == thatOne) return true;
        if ((thisOne != null && thatOne == null) || (thisOne == null)) return false;
        if (thisOne.allAscending() != thatOne.allAscending()) return false;
        if (thisOne.size() != thatOne.size()) return false;

        for (int i=0; i<thatOne.size(); i++) {
            ValueNode thisResultCol = thisOne.getOrderByColumn(i).getColumnExpression();
            ValueNode thatResultCol = thatOne.getOrderByColumn(i).getColumnExpression();
            if (thisResultCol != null) {
                if (thatResultCol != null) {
                    if (!thisResultCol.isEquivalent(thatResultCol)) {
                        return false;
                    }
                } else return false;
            } else if (thatResultCol != null) return false;
        }
        return true;
    }

    private OverClause(ContextManager contextManager, Partition partition, OrderByList orderByClause, WindowFrameDefinition frameDefinition) {
        setContextManager(contextManager);
        this.partition = partition;
        this.orderByClause = orderByClause;
        this.frameDefinition = frameDefinition;
        this.hashCode = calculateHashCode();
    }

    private String printOrderByList() {
        if (orderByClause == null) {
            return "";
        }
        StringBuilder buf = new StringBuilder("\n");
        for (int i=0; i<orderByClause.size(); ++i) {
            OrderByColumn col = orderByClause.getOrderByColumn(i);
            buf.append("    column_name: ").append(col.getColumnExpression().getColumnName()).append("\n");
            // Lang col indexes are 1-based, storage col indexes are zero-based
            buf.append("    columnid: ").append(col.getColumnPosition()).append("\n");
            buf.append("    ascending: ").append(col.isAscending()).append("\n");
            buf.append("    nullsOrderedLow: ").append(col.isAscending()).append("\n");
        }
        return buf.toString();
    }

    public static class Builder {
        private final ContextManager contextManager;
        private Partition partition;
        private OrderByList orderByClause;
        private WindowFrameDefinition frameDefinition;

        public Builder(ContextManager contextManager) {
            this.contextManager = contextManager;
        }

        public OverClause build() {
            return new OverClause(contextManager, partition, orderByClause, frameDefinition);
        }

        public Builder setPartition(Partition partition) {
            this.partition = partition;
            if (this.partition != null) {
                this.partition.setContextManager(this.contextManager);
            }
            return this;
        }

        public Builder setOrderByClause(OrderByList orderByList) {
            this.orderByClause = orderByList;
            if (this.orderByClause != null) {
                this.orderByClause.setContextManager(this.contextManager);
            }
            return this;
        }

        public Builder setFrameDefinition(WindowFrameDefinition frameDefinition) {
            this.frameDefinition = frameDefinition;
            if (this.frameDefinition != null) {
                this.frameDefinition.setContextManager(this.contextManager);
            }
            return this;
        }
    }
}
