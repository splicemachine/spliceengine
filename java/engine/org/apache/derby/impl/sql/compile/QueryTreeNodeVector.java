/*

   Derby - Class org.apache.derby.impl.sql.compile.QueryTreeNodeVector

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.Visitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * QueryTreeNodeVector is the root class for all lists of query tree nodes.
 * It provides a wrapper for java.util.Vector. All
 * lists of query tree nodes inherit from QueryTreeNodeVector.
 *
 */

abstract class QueryTreeNodeVector<T extends QueryTreeNode> extends QueryTreeNode implements Iterable<T> {
	private List<T> v = new ArrayList<T>();

	public final int size()
	{
		return v.size();
	}

	T elementAt(int index) {
		return v.get(index);
	}

	final void addElement(T qt) {
		v.add(qt);
	}

	final void removeElementAt(int index) {
		v.remove(index);
	}

	final void removeElement(T qt) {
		v.remove(qt);
	}

	final T remove(int index) {
		return v.remove(index);
	}

	final int indexOf(T qt) {
		return v.indexOf(qt);
	}

	final void setElementAt(T qt, int index) {
		v.set(index, qt);
	}

    public Iterator<T> iterator() {
        return v.iterator();
    }

	void destructiveAppend(QueryTreeNodeVector<T> qtnv) {
		nondestructiveAppend(qtnv);
		qtnv.removeAllElements();
	}

	void nondestructiveAppend(QueryTreeNodeVector<T> qtnv) {
		int qtnvSize = qtnv.size();
		for (int index = 0; index < qtnvSize; index++) {
			v.add(qtnv.elementAt(index));
		}
	}

	final void removeAllElements() {
		v.clear();
	}

	final void insertElementAt(T qt, int index) {
		v.add(index, qt);
	}


	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 * @param depth		The depth to indent the sub-nodes
	 */
	public void printSubNodes(int depth) {
		if (SanityManager.DEBUG) {
			for (int index = 0; index < size(); index++) {
				debugPrint(formatNodeString("[" + index + "]:", depth));
				T elt = elementAt(index);
				elt.treePrint(depth);
			}
		}
	}


	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	public void acceptChildren(Visitor v) throws StandardException {
		super.acceptChildren(v);

		int size = size();
		for (int index = 0; index < size; index++) {
        //noinspection unchecked
        setElementAt((T) elementAt(index).accept(v), index);
		}
	}
	
	public List getNodes(){
		return v;
	}
}
