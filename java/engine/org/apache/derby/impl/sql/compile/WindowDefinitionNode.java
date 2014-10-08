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
        // bind partition
        Partition partition = overClause.getPartition();
        if (partition != null) {
            Vector partitionAggregateVector = new Vector();
            partition.bindGroupByColumns(target, partitionAggregateVector);
        }

        // bind order by
        OrderByList orderByList = overClause.getOrderByClause();
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
        return overClause.getPartition();
    }

    public WindowFrameDefinition getFrameExtent() {
        return overClause.getFrameDefinition();
    }

    /**
     * @return the order by list of this window definition if any, else null.
     */
    public OrderByList getOrderByList() {
        return overClause.getOrderByClause();
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
