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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;

import java.util.Vector;

/**
 * Collect all nodes of the designated type to be returned in a vector. <p> Can find any type of node -- the class or
 * class name of the target node is passed in as a constructor parameter.
 */
public class CollectNodesVisitor implements Visitor {

    private Vector nodeList;
    private Class nodeClass;
    private Class skipOverClass;

    /**
     * Construct a visitor
     *
     * @param nodeClass the class of the node that we are looking for.
     */
    public CollectNodesVisitor(Class nodeClass) {
        this.nodeClass = nodeClass;
        nodeList = new Vector();
    }

    /**
     * Construct a visitor
     *
     * @param nodeClass     the class of the node that we are looking for.
     * @param skipOverClass do not go below this node when searching for nodeClass.
     */
    public CollectNodesVisitor(Class nodeClass, Class skipOverClass) {
        this(nodeClass);
        this.skipOverClass = skipOverClass;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }
    ////////////////////////////////////////////////
    //
    // VISITOR INTERFACE
    //
    ////////////////////////////////////////////////

    /**
     * If we have found the target node, we are done.
     *
     * @param node the node to process
     * @return me
     */
    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) {
        if (nodeClass.isInstance(node)) {
            nodeList.add(node);
        }
        return node;
    }

    /**
     * Don't visit childen under the skipOverClass node, if it isn't null.
     *
     * @return true/false
     */
    @Override
    public boolean skipChildren(Visitable node) {
        return (skipOverClass != null) && skipOverClass.isInstance(node);
    }

    ////////////////////////////////////////////////
    //
    // CLASS INTERFACE
    //
    ////////////////////////////////////////////////

    /**
     * Return the list of matching nodes.
     */
    public Vector getList() {
        return nodeList;
    }
}
