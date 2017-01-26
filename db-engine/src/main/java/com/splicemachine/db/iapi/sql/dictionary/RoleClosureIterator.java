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
package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * Allows iterator over the role grant closure defined by the relation
 * GRANT role-a TO role-b, or its inverse.
 * @see DataDictionary#createRoleClosureIterator
 * @see org.apache.derby.impl.sql.catalog.RoleClosureIteratorImpl
 */
public interface RoleClosureIterator
{

    /**
     * Returns the next (as yet unreturned) role in the transitive closure of
     * the grant or grant<sup>-1</sup> relation.
     *
     * The grant relation forms a DAG (directed acyclic graph).
     * <pre>
     * Example:
     *      Assume a set of created roles forming nodes:
     *            {a1, a2, a3, b, c, d, e, f, h, j}
     *
     *      Assume a set of GRANT statements forming arcs:
     *
     *      GRANT a1 TO b;   GRANT b TO e;  GRANT e TO h;
     *      GRANT a1 TO c;                  GRANT e TO f;
     *      GRANT a2 TO c;   GRANT c TO f;  GRANT f TO h;
     *      GRANT a3 TO d;   GRANT d TO f;  GRANT a1 to j;
     *
     *
     *          a1            a2         a3
     *         / | \           |          |
     *        /  b  +--------> c          d
     *       j   |              \        /
     *           e---+           \      /
     *            \   \           \    /
     *             \   \---------+ \  /
     *              \             \_ f
     *               \             /
     *                \           /
     *                 \         /
     *                  \       /
     *                   \     /
     *                    \   /
     *                      h
     * </pre>
     * An iterator on the inverse relation starting at h for the above
     * grant graph will return:
     * <pre>
     *       closure(h, grant-inv) = {h, e, b, a1, f, c, a2, d, a3}
     * </pre>
     * <p>
     * An iterator on normal (not inverse) relation starting at a1 for
     * the above grant graph will return:
     * <pre>
     *       closure(a1, grant)    = {a1, b, j, e, h, f, c}
     * </pre>
     *
     * @return a role name identifying a yet unseen node, or null if the
     *         closure is exhausted.  The order in which the nodes are returned
     *         is not defined, except that the root is always returned first (h
     *         and a1 in the above examples).
     */
    public String next() throws StandardException;
}
