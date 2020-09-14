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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.util.ArrayList;
import java.util.List;

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

    private List<WindowFunctionNode> functionNodes = new ArrayList<>();

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
     * @param parent this select node
     * @throws StandardException
     */
    public void bind(SelectNode parent) throws StandardException {
        List<AggregateNode> aggregates = new ArrayList<>();

        // bind partition
        Partition partition = overClause.getPartition();
        if (partition != null) {
            partition.bindGroupByColumns(parent, aggregates);
        }

        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(aggregates.isEmpty(),
                                 "A window function partition cannot contain an aggregate.");
        }

        // bind order by
        OrderByList orderByList = overClause.getOrderByClause();
        if (orderByList != null) {
            FromList fromList = parent.getFromList();
            for (int i=0; i<orderByList.size(); ++i) {
                OrderByColumn obc = orderByList.getOrderByColumn(i);
                    obc.setColumnExpression(obc.getColumnExpression().bindExpression(fromList, null, aggregates));
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
        List<OrderedColumn> partitionList = new ArrayList<>(partitionSize);
        if (partition != null) {
            for (int i=0; i<partitionSize; i++) {
                partitionList.add(partition.elementAt(i));
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
        List<OrderedColumn> orderedColumns = new ArrayList<>(orderBySize);
        if (orderByList != null) {
            for (int i=0; i<orderBySize; i++) {
                orderedColumns.add(orderByList.elementAt(i));
            }
        }
        return orderedColumns;
    }

    @Override
    public List<WindowFunctionNode> getWindowFunctions() {
        return functionNodes;
    }

    @Override
    public void addWindowFunction(WindowFunctionNode functionNode) {
        functionNodes.add(functionNode);
    }

    /**
     * See {@link OverClause#getOverColumns()}
     * @return the list of column keys that make up the over() clause, partition and order by.
     */
    @Override
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
        return other != null && !(overClause != null ? !overClause.isEquivalent(other.overClause) : other.overClause != null);

    }

    /**
     * @see QueryTreeNode#toString
     */
    @Override
    public String toString() {
        StringBuilder fns = new StringBuilder();
        for (WindowFunctionNode fn : functionNodes) {
            fns.append(fn.getName()).append(", ");
        }
        if (fns.length() > 1) {
            fns.setLength(fns.length()-2);
        }
        return ("Functions: "+fns.toString()+ overClause);
    }

    @Override
    public String toHTMLString() {
        StringBuilder fns = new StringBuilder();
        for (WindowFunctionNode fn : functionNodes) {
            fns.append(fn.getName()).append(", ");
        }
        if (fns.length() > 1) {
            fns.setLength(fns.length()-2);
        }
        return "Functions: "+fns.toString()+ " Name: " + getName() + (inlined ? " Inlined" : " non-Inlined") + "<br/>" +
            overClause.toHTMLString();
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

        return inlined == that.inlined && (overClause != null ? overClause.equals(that.overClause) : that.overClause == null);

    }

    @Override
    public int hashCode() {
        int result = (inlined ? 1 : 0);
        result = 31 * result + (overClause != null ? overClause.hashCode() : 0);
        return result;
    }

    @Override
    public OverClause getOverClause() {
        return overClause;
    }

    @Override
    public void setOverClause(OverClause overClause) {
        this.overClause = overClause;
    }
}
