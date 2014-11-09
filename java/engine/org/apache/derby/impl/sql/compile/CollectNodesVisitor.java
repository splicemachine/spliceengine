/*

   Derby - Class org.apache.derby.impl.sql.compile.CollectNodesVisitor

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

import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Collect all nodes of the designated type to be returned
 * in a vector.
 * <p>
 * Can find any type of node -- the class or class name
 * of the target node is passed in as a constructor
 * parameter.
 *
 */
public class CollectNodesVisitor<T> implements Visitor {
	private List<T> nodeList;
	private Class<? extends T> 	nodeClass;
	private Class	skipOverClass;
	/**
	 * Construct a visitor
	 *
	 * @param nodeClass the class of the node that 
	 * 	we are looking for.
	 */
	public CollectNodesVisitor(Class<? extends T> nodeClass) {
		this.nodeClass = nodeClass;
		nodeList = new ArrayList<T>();
	}

	/**
	 * Construct a visitor
	 *
	 * @param nodeClass the class of the node that 
	 * 	we are looking for.
	 * @param skipOverClass do not go below this
	 * node when searching for nodeClass.
	 */
	public CollectNodesVisitor(Class<T> nodeClass, Class skipOverClass) {
		this(nodeClass);
		this.skipOverClass = skipOverClass;
	}

	public boolean visitChildrenFirst(Visitable node) {
		return false;
	}

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
	 * @param node 	the node to process
	 *
	 * @return me
	 */
	public Visitable visit(Visitable node) {
		if (nodeClass.isInstance(node)) {
        //this is safe because nodeClass.isInstance(node) will return false if the cast won't work
        @SuppressWarnings("unchecked") T n = (T) node;
        nodeList.add(n);
		}
		return node;
	}

	/**
	 * Don't visit childen under the skipOverClass
	 * node, if it isn't null.
	 *
	 * @return true/false
	 */
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
	 *
	 */
	public List<T> getList()
	{
		return nodeList;
	}

    /**
     * Static factory method which makes typing easier
     * @param nodeClass the class to collect
     * @param <T> the type of the class to collect
     * @return a new CollectNodesVisitor for this type
     */
    public static <T> CollectNodesVisitor<T> newVisitor(Class<? extends T> nodeClass){
        return new CollectNodesVisitor<T>(nodeClass);
    }
}
