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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.RoleClosureIterator;
import com.splicemachine.db.iapi.sql.dictionary.RoleGrantDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.*;

/**
 * Allows iterator over the role grant closure defined by the relation
 * <code>GRANT</code> role-a <code>TO</code> role-b, or its inverse.
 * <p>
 * The graph is represented as a <code>HashMap</code> where the key is
 * the node and the value is a List grant descriptors representing
 * outgoing arcs. The set constructed depends on whether <code>inverse</code>
 * was specified in the constructor.
 * @see com.splicemachine.db.iapi.sql.dictionary.RoleClosureIterator
 */
public class RoleClosureIteratorImpl implements RoleClosureIterator
{
    /**
     * true if closure is inverse of GRANT role-a TO role-b.
     */
    private final boolean inverse;

    /**
     * Holds roles seen so far when computing the closure.
     * <ul>
     *   <li>Key: role name. Depending on value of {@code inverse}, the
     *       key represents and is compared against {@code roleName()}
     *       or {@code grantee()} of role descriptors visited.</li>
     *   <li>Value: none</li>
     * </ul>
     */
    private HashMap seenSoFar;

    /**
     * Holds the grant graph.
     * <ul>
     *   <li>key: role name</li>
     *   <li>value: list of {@code RoleGrantDescriptor}, making up outgoing arcs
     *        in graph</li>
     * </ul>
     */
    private Map<String,List<RoleGrantDescriptor>> graph;

    /**
     * Holds discovered, but not yet handed out, roles in the closure.
     */
    private List lifo;

    /**
     * Last node returned by next; a logical pointer into the arcs
     * list of a node we are currently processing.
     */
    private Iterator currNodeIter;

    /**
     * DataDictionaryImpl used to get closure graph
     */
    private DataDictionaryImpl dd;

    /**
     * TransactionController used to get closure graph
     */
    private TransactionController tc;

    /**
     * The role for which we compute the closure.
     */
    private String root;

    /**
     * true before next is called the first time
     */
    private boolean initial;

    /**
     * Constructor (package private).
     * Use {@code createRoleClosureIterator} to obtain an instance.
     * @see com.splicemachine.db.iapi.sql.dictionary.DataDictionary#createRoleClosureIterator
     *
     * @param root The role name for which to compute the closure
     * @param inverse If {@code true}, {@code graph} represents the
     *                grant<sup>-1</sup> relation.
     * @param dd data dictionary
     * @param tc transaction controller
     *
     */
    RoleClosureIteratorImpl(String root, boolean inverse,
                            DataDictionaryImpl dd,
                            TransactionController tc) {
        this.inverse = inverse;
        this.graph = null;
        this.root = root;
        this.dd = dd;
        this.tc = tc;
        seenSoFar = new HashMap();
        lifo      = new ArrayList(); // remaining work stack

        RoleGrantDescriptor dummy = new RoleGrantDescriptor
            (null,
             null,
             inverse ? root : null,
             inverse ? null : root,
             null,
             false,
             false,
             false);
        List dummyList = new ArrayList();
        dummyList.add(dummy);
        currNodeIter = dummyList.iterator();
        initial = true;
    }


    public String next() throws StandardException {
        if (initial) {
            // Optimization so we don't compute the closure for the current
            // role if unnecessary (when next is only called once).
            initial = false;
            seenSoFar.put(root, null);

            return root;

        } else if (graph == null) {
            // We get here the second time next is called.
            graph = dd.getRoleGrantGraph(tc, inverse, true);
            List outArcs = (List)graph.get(root);
            if (outArcs != null) {
                currNodeIter = outArcs.iterator();
            }
        }

        RoleGrantDescriptor result = null;

        while (result == null) {
            while (currNodeIter.hasNext()) {
                RoleGrantDescriptor r =
                    (RoleGrantDescriptor)currNodeIter.next();

                if (seenSoFar.containsKey
                        (inverse ? r.getRoleName() : r.getGrantee())) {
                } else {
                    lifo.add(r);
                    result = r;
                    break;
                }
            }

            if (result == null) {
                // not more candidates located outgoing from the
                // latest found node, pick another and continue
                RoleGrantDescriptor newNode = null;

                currNodeIter = null;

                while (!lifo.isEmpty() && currNodeIter == null) {

                    newNode = (RoleGrantDescriptor)lifo.remove(lifo.size() - 1);

                    // In the example (see interface doc), the
                    // iterator of outgoing arcs for f (grant inverse)
                    // would contain {e,c,d}.
                    List outArcs = (List)graph.get(
                        inverse? newNode.getRoleName(): newNode.getGrantee());

                    if (outArcs != null) {
                        currNodeIter = outArcs.iterator();
                    } // else: leaf node, pop next candidate, if any
                }

                if (currNodeIter == null) {
                    // candidate stack is empty, done
                    currNodeIter = null;
                    break;
                }
            }
        }

        if (result != null) {
            String role = inverse ? result.getRoleName(): result.getGrantee();
            seenSoFar.put(role, null);
            return role;
        } else {
            return null;
        }
    }
}
