/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.sql.compile;

import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;

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
}
