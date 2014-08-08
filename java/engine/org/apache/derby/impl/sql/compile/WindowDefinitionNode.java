/*

   Derby - Class org.apache.derby.impl.sql.compile.WindowDefinitionNode

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.compile;

import java.util.Vector;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * This class represents an OLAP window definition.
 */
public final class WindowDefinitionNode extends WindowNode
{
    /**
     * True of the window definition was inlined.
     */
    private boolean inlined;

    /**
     * The partition by list if the window definition contains a <window partition
     * clause>, else null.
     */
    private Partition partition;

    /**
     * The order by list if the window definition contains a <window order
     * clause>, else null.
     */
    private OrderByList orderByList;

    /**
     * The window frame.
     */
    private WindowFrameDefinition frameExtent;

    /**
     * Initializer.
     *
     * @param arg1 The window name, null if in-lined definition
     * @param arg2 GROUP BY list (partition)
     * @param arg3 ORDER BY list
     * @param arg4 frame
     * @exception StandardException
     */
    public void init(Object arg1,
                     Object arg2,
                     Object arg3,
                     Object arg4)
        throws StandardException
    {
        String name = (String)arg1;

        partition = (Partition)arg2;
        orderByList = (OrderByList)arg3;
        frameExtent = (WindowFrameDefinition)arg4;

        if (name != null) {
            super.init(arg1);
            inlined = false;
        } else {
            super.init("IN-LINE_WINDOW");
            inlined = true;
        }
    }

    public void bind(SelectNode target) throws StandardException {
        // bind partition
        if (partition != null) {
            Vector partitionAggregateVector = new Vector();
            partition.bindGroupByColumns(target, partitionAggregateVector);
        }

        // bind order by
        if (orderByList != null) {
            FromList fromList = target.getFromList();
            for (int i=0; i<orderByList.size(); ++i) {
                OrderByColumn obc = orderByList.getOrderByColumn(i);
                obc.getColumnExpression().bindExpression(fromList, null, null);
            }
        }
    }

    /**
     * Used to merge equivalent window definitions.
     *
     * @param wl list of window definitions
     * @return an existing window definition from wl, if 'this' is equivalent
     * to a window in wl.
     */
    public WindowDefinitionNode findEquivalentWindow(WindowList wl) throws StandardException {
        for (int i = 0; i < wl.size(); i++) {
            WindowDefinitionNode old = (WindowDefinitionNode)wl.elementAt(i);

            if (isEquivalent(old)) {
                return old;
            }
        }
        return null;
    }

    public Partition getPartition() {
        return partition;
    }

    public WindowFrameDefinition getFrameExtent() {
        return frameExtent;
    }

    /**
     * @return the order by list of this window definition if any, else null.
     */
    public OrderByList getOrderByList() {
        return orderByList;
    }

    /**
     * @return true if the window specifications are equivalent; no need to create
     * more than one window then.
     */
    private boolean isEquivalent(WindowDefinitionNode other) throws StandardException {
        if (this == other) return true;
        if (other == null) return false;

        if (frameExtent != null ? !frameExtent.isEquivalent(other.frameExtent) : other.frameExtent != null) return false;
        if (orderByList != null ? !isEquivalent(orderByList, other.orderByList) : other.orderByList != null) return false;
        if (partition != null ? !partition.isEquivalent(other.partition) : other.partition != null) return false;

        return true;
    }

    private boolean isEquivalent(OrderByList thisOne, OrderByList thatOne) throws StandardException {
        if (thisOne == thatOne) return true;
        if ((thisOne != null && thatOne == null) || (thisOne == null)) return false;
        if (thisOne.allAscending() != thatOne.allAscending()) return false;
        if (thisOne.size() != thatOne.size()) return false;

        for (int i=0; i<thatOne.size(); i++) {
            ResultColumn thisResultCol = thisOne.getOrderByColumn(i).getResultColumn();
            ResultColumn thatResultCol = thatOne.getOrderByColumn(i).getResultColumn();
            if (thisResultCol != null && thatResultCol != null) {
                if (! thisResultCol.isEquivalent(thatResultCol)) {
                    return false;
                }
            } else if (thisResultCol == null)
                return false;
        }
        return true;
    }

    /**
     * java.lang.Object override.
     * @see QueryTreeNode#toString
     */
    public String toString() {
        return ("name: " + getName() + "\n" +
            "inlined: " + inlined + "\n" +
            "partition: " + partition + "\n" +
            "orderby: " + printOrderByList() + "\n" +
            frameExtent + "\n");
    }

    private String printOrderByList() {
        if (orderByList == null) {
            return "";
        }
        StringBuilder buf = new StringBuilder("\n");
        for (int i=0; i<orderByList.size(); ++i) {
            OrderByColumn col = orderByList.getOrderByColumn(i);
//            buf.append("column_name: ").append(col.getResultColumn().getColumnName()).append("\n");
            buf.append("column_name: ").append(col.getColumnExpression().getColumnName()).append("\n");
            // Lang col indexes are 1-based, storage col indexes are zero-based
            buf.append("columnid: ").append(col.getColumnPosition()).append("\n");
            buf.append("ascending: ").append(col.isAscending()).append("\n");
            buf.append("nullsOrderedLow: ").append(col.isAscending()).append("\n");
        }
//        if (buf.length() > 0) { buf.setLength(buf.length()-1); }
        return buf.toString();
    }

    /**
     * QueryTreeNode override. Prints the sub-nodes of this object.
     * @see QueryTreeNode#printSubNodes
     *
     * @param depth     The depth of this node in the tree
     */

    public void printSubNodes(int depth)
    {
        if (SanityManager.DEBUG)
        {
            super.printSubNodes(depth);

            if (partition != null) {
                printLabel(depth, "partition: "  + depth);
                partition.treePrint(depth + 1);
            }

            if (orderByList != null) {
                printLabel(depth, "orderByList: "  + depth);
                orderByList.treePrint(depth + 1);
            }

            if (frameExtent != null) {
                printLabel(depth, "frame: "  + depth);
                frameExtent.treePrint(depth + 1);
            }
        }
    }
}
