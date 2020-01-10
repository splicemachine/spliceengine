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

package com.splicemachine.db.impl.sql.compile;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.util.ArrayList;
import java.util.List;

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
        List<OrderedColumn> keyCols = new ArrayList<>(partitionSize+orderByListSize);

        // A Multimap may have multiple values per key.
        // If V is a ColumnReference with zero-length column name, the underlying
        // source expressions are compared for equivalence when Multimap.contains is called.
        // Otherwise, just table number and column name are compared.
        final Multimap<Integer, ValueNode> rowkeyParts =
               ArrayListMultimap.create(partitionSize+orderByListSize, 1);

        // partition columns
        for (int i=0; i<partitionSize; i++) {
            OrderedColumn oc = partition.elementAt(i);
            if (! rowkeyParts.containsEntry(oc.getColumnExpression().hashCode(),
                                            oc.getColumnExpression())) {
                keyCols.add(oc);
                rowkeyParts.put(oc.getColumnExpression().hashCode(),
                                oc.getColumnExpression());
            }
        }

        // order by columns
        for (int i=0; i<orderByListSize; ++i) {
            OrderedColumn oc = orderByClause.elementAt(i);
            if (! rowkeyParts.containsEntry(oc.getColumnExpression().hashCode(),
                  oc.getColumnExpression())) {
                keyCols.add(oc);
                rowkeyParts.put(oc.getColumnExpression().hashCode(),
                                oc.getColumnExpression());
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
        List<OrderedColumn> keyCols = new ArrayList<>(partitionSize+orderByListSize);

        // partition columns
        for (int i=0; i<partitionSize; i++) {
            keyCols.add(partition.elementAt(i));
        }

        // order by columns
        for (int i=0; i<orderByListSize; ++i) {
            keyCols.add(orderByClause.elementAt(i));
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
        return (" partition: " + partition + "  orderby: " + printOrderByList() + "  "+frameDefinition);
    }

    @Override
    public String toHTMLString() {
        return "Partition: " + (partition != null ? partition.toHTMLString() : "") +
            "<br/> OrderBy: " + printOrderByList() + "<br/> Frame: " + frameDefinition.toHTMLString();

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

        if (frameDefinition != null ? !frameDefinition.isEquivalent(other.frameDefinition) : other.frameDefinition != null)
            return false;
        return (orderByClause != null ? isEquivalent(orderByClause, other.orderByClause) : other.orderByClause == null) && (partition != null ? partition.isEquivalent(other.partition) : other.partition == null);

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
        StringBuilder buf = new StringBuilder();
        for (int i=0; i<orderByClause.size(); ++i) {
            OrderByColumn col = orderByClause.getOrderByColumn(i);
            buf.append(col.getColumnExpression().getColumnName()).append('(');
            buf.append(col.getColumnPosition()).append("),");
        }
        if (buf.length() > 0) {
            buf.setLength(buf.length()-1);
        }

        return buf.toString();
    }

    /**
     * Over clause builder used by SQL grammar (generated parser).
     */
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
