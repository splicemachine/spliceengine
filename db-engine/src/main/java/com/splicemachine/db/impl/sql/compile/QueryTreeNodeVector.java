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
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Visitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * QueryTreeNodeVector is the root class for all lists of query tree nodes.
 * It provides a wrapper for java.util.Vector. All
 * lists of query tree nodes inherit from QueryTreeNodeVector.
 */

abstract class QueryTreeNodeVector<T extends QueryTreeNode> extends QueryTreeNode implements Iterable<T>{
    private List<T> v=new ArrayList<>();

    public final int size(){
        return v.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public T elementAt(int index){
        return v.get(index);
    }

    public int indexOf(T itemToSearchFor){
        return v.indexOf(itemToSearchFor);
    }

    public final void addElement(T qt){
        v.add(qt);
    }

    public final void removeElementAt(int index){
        v.remove(index);
    }

    public final void removeElement(T qt){
        v.remove(qt);
    }

    final T remove(int index){
        return v.remove(index);
    }

    public final void setElementAt(T qt,int index){
        v.set(index,qt);
    }

    @Override
    public Iterator<T> iterator(){
        return v.iterator();
    }

    void destructiveAppend(QueryTreeNodeVector<T> qtnv){
        nondestructiveAppend(qtnv);
        qtnv.removeAllElements();
    }

    public void nondestructiveAppend(QueryTreeNodeVector<T> qtnv){
        int qtnvSize=qtnv.size();
        for(int index=0;index<qtnvSize;index++){
            v.add(qtnv.elementAt(index));
        }
    }

    final void removeAllElements(){
        v.clear();
    }

    final void insertElementAt(T qt,int index){
        v.add(index, qt);
    }

    /**
     * Return TRUE if this object contains any node that is the specified node or a subclass of the specified node.
     *
     * EXAMPLE fromList.containsNode(UnionNode.class) will return true if the fromList contains any type of UnionNode.
     */
    public boolean containsNode(Class<?> testClass) {
        for(T t: v) {
            if(testClass.isAssignableFrom(t.getClass())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth to indent the sub-nodes
     */
    @Override
    public void printSubNodes(int depth){
        if(SanityManager.DEBUG){
            for(int index=0;index<size();index++){
                debugPrint(formatNodeString("["+index+"]:",depth));
                T elt=elementAt(index);
                elt.treePrint(depth);
            }
        }
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException{
        super.acceptChildren(v);
        int size=size();
        for(int index=0;index<size;index++){
            //noinspection unchecked
            setElementAt((T)elementAt(index).accept(v, this),index);
        }
    }

    public List<T> getNodes(){
        return v;
    }
}
