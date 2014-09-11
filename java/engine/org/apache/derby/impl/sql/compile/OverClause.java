package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.sanity.SanityManager;

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

        if (!frameDefinition.equals(that.frameDefinition)) return false;
        if (orderByClause != null ? !orderByClause.equals(that.orderByClause) : that.orderByClause != null) return false;
        if (partition != null ? !partition.equals(that.partition) : that.partition != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    private int calculateHashCode() {
        int result = partition != null ? partition.hashCode() : 0;
        result = 31 * result + (orderByClause != null ? orderByClause.hashCode() : 0);
        result = 31 * result + frameDefinition.hashCode();
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
            ResultColumn thisResultCol = thisOne.getOrderByColumn(i).getResultColumn();
            ResultColumn thatResultCol = thatOne.getOrderByColumn(i).getResultColumn();
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
