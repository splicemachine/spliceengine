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

package com.splicemachine.db.impl.sql.compile;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

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
     * The over() clause of the window function containing partition,
     * over by and frame definition.
     */
    private OverClause overClause;

    /**
     * Initializer.
     *
     * @param arg1 The window name, null if in-lined definition
     * @param arg2 OverClause containing partition, order by and frame spec
     * @exception StandardException
     */
    public void init(Object arg1,
                     Object arg2)
        throws StandardException
    {
        String name = (String)arg1;

        overClause = (OverClause)arg2;

        if (name != null) {
            super.init(arg1);
            inlined = false;
        } else {
            super.init("IN-LINE_WINDOW");
            inlined = true;
        }
    }

    /**
     * Bind partition and order by columns, since they may not be in select clause.
     * @param target this select node
     * @throws StandardException
     */
    public void bind(SelectNode target) throws StandardException {
        Vector aggregateVector = new Vector();
        // bind partition
        Partition partition = overClause.getPartition();
        if (partition != null) {
            partition.bindGroupByColumns(target, aggregateVector);
        }

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(aggregateVector.size() == 0,
                                 "A window function partition cannot contain an aggregate.");
        }

        // bind order by
        OrderByList orderByList = overClause.getOrderByClause();
        if (orderByList != null) {
            FromList fromList = target.getFromList();
            for (int i=0; i<orderByList.size(); ++i) {
                OrderByColumn obc = orderByList.getOrderByColumn(i);
                obc.getColumnExpression().bindExpression(fromList, null, aggregateVector);
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

    public List<OrderedColumn> getPartition() {
        Partition partition = overClause.getPartition();
        int partitionSize = (partition != null ? partition.size() : 0);
        List<OrderedColumn> partitionList = new ArrayList<OrderedColumn>(partitionSize);
        if (partition != null) {
            for (int i=0; i<partitionSize; i++) {
                partitionList.add((OrderedColumn) partition.elementAt(i));
            }
        }
        return partitionList;
    }

    public WindowFrameDefinition getFrameExtent() {
        return overClause.getFrameDefinition();
    }

    /**
     * @return the order by list of this window definition if any, else null.
     */
    public List<OrderedColumn> getOrderByList() {
        OrderByList orderByList = overClause.getOrderByClause();
        int orderBySize = (orderByList != null ? orderByList.size() : 0);
        List<OrderedColumn> orderedColumns = new ArrayList<OrderedColumn>(orderBySize);
        if (orderByList != null) {
            for (int i=0; i<orderBySize; i++) {
                orderedColumns.add((OrderedColumn) orderByList.elementAt(i));
            }
        }
        return orderedColumns;
    }

    /**
     * See {@link OverClause#getOverColumns()}
     * @return the list of column keys that make up the over() clause, partition and order by.
     */
    public List<OrderedColumn> getOverColumns() {
        return overClause.getOverColumns();
    }

    /**
     * See {@link OverClause#getKeyColumns()}
     * @return the de-duplicated set of column keys
     */
    public List<OrderedColumn> getKeyColumns() {
        return overClause.getKeyColumns();
    }

    /**
     * @return true if the window specifications are equivalent; no need to create
     * more than one window then.
     */
    private boolean isEquivalent(WindowDefinitionNode other) throws StandardException {
        if (this == other) return true;
        if (other == null) return false;

        return !(overClause != null ? !overClause.isEquivalent(other.overClause) : other.overClause != null);

    }

    /**
     * java.lang.Object override.
     * @see QueryTreeNode#toString
     */
    public String toString() {
        return ("name: " + getName() + "\n" +
            "inlined: " + inlined + "\n" +
            overClause);
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

            if (overClause != null) {
                printLabel(depth, "over: "  + depth);
                overClause.treePrint(depth + 1);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WindowDefinitionNode that = (WindowDefinitionNode) o;

        if (inlined != that.inlined) return false;
        if (overClause != null ? !overClause.equals(that.overClause) : that.overClause != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (inlined ? 1 : 0);
        result = 31 * result + (overClause != null ? overClause.hashCode() : 0);
        return result;
    }
}
