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

package	com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;

/**
 * A visitor is an object that traverses the querytree
 * and performs some action. 
 *
 */
public interface Visitor
{
	/**
	 * This is the default visit operation on a 
	 * QueryTreeNode.  It just returns the node.  This
	 * will typically suffice as the default visit 
	 * operation for most visitors unless the visitor 
	 * needs to count the number of nodes visited or 
	 * something like that.
	 * <p>
	 * Visitors will overload this method by implementing
	 * a version with a signature that matches a specific
	 * type of node.  For example, if I want to do
	 * something special with aggregate nodes, then
	 * that Visitor will implement a 
	 * 		<I> visit(AggregateNode node)</I>
	 * method which does the aggregate specific processing.
	 *
	 * @param node 	the node to process
     * @param parent the parent of the node being visited, or, more generally, the node that contains a reference to
     *               the node being visited.
	 *
	 * @return a query tree node.  Often times this is
	 * the same node that was passed in, but Visitors that
	 * replace nodes with other nodes will use this to
	 * return the new replacement node.
	 *
	 * @exception StandardException may be throw an error
	 *	as needed by the visitor (i.e. may be a normal error
	 *	if a particular node is found, e.g. if checking 
	 *	a group by, we don't expect to find any ColumnReferences
	 *	that aren't under an AggregateNode -- the easiest
	 *	thing to do is just throw an error when we find the
	 *	questionable node).
	 */
	Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException;

	/**
	 * Method that is called to see if {@code visit()} should be called on
	 * the children of {@code node} before it is called on {@code node} itself.
	 * If this method always returns {@code true}, the visitor will walk the
	 * tree bottom-up. If it always returns {@code false}, the tree is visited
	 * top-down.
	 *
	 * @param node the top node of a sub-tree about to be visited
	 * @return {@code true} if {@code node}'s children should be visited
	 * before {@code node}, {@code false} otherwise
	 */
	public boolean visitChildrenFirst(Visitable node);

	/**
	 * Method that is called to see
	 * if query tree traversal should be
	 * stopped before visiting all nodes.
	 * Useful for short circuiting traversal
	 * if we already know we are done.
	 *
	 * @return true/false
	 */
	public boolean stopTraversal();

	/**
	 * Method that is called to indicate whether
	 * we should skip all nodes below this node
	 * for traversal.  Useful if we want to effectively
	 * ignore/prune all branches under a particular 
	 * node.  
	 * <p>
	 * Differs from stopTraversal() in that it
	 * only affects subtrees, rather than the
	 * entire traversal.
	 *
	 * @param node 	the node to process
	 * 
	 * @return true/false
	 */
	public boolean skipChildren(Visitable node) throws StandardException;
}	
