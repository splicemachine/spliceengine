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

import java.util.List;

/**
 * Superclass of window definition and window reference.
 */
public abstract class WindowNode extends QueryTreeNode
{
    /**
     * The provided name of the window if explicitly defined in a window
     * clause. If the definition is inlined, currently the definition has
     * windowName "IN_LINE".  The standard 2003 sec. 4.14.9 calls for a
     * impl. defined one.
     */
    private String windowName;


    /**
     * Initializer
     *
     * @param arg1 The window name
     *
     * @exception StandardException
     */
    public void init(Object arg1)
        throws StandardException
    {
        windowName = (String)arg1;
    }


    /**
     * @return the name of this window
     */
    public String getName() {
        return windowName;
    }


    public abstract List<OrderedColumn> getPartition();

    public abstract WindowFrameDefinition getFrameExtent();

    public abstract List<OrderedColumn> getOrderByList();

    public abstract List<WindowFunctionNode> getWindowFunctions();

    public abstract void addWindowFunction(WindowFunctionNode functionNode);

    public abstract void bind(SelectNode selectNode) throws StandardException;

    public abstract List<OrderedColumn> getOverColumns();

    public abstract OverClause getOverClause();

    public abstract void setOverClause(OverClause overClause);

}
