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
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Update all column references in a tree to reference
 * provided result columns in rcl.
 */
class UpdateColumnReferenceVisitor implements Visitor
{
    protected int               numSubstitutions;
    protected ResultColumnList  rcl;
    protected Set<ResultColumn> rcSet;
    protected Map<ColumnReference, ColumnReference> cache;

    UpdateColumnReferenceVisitor(ResultColumnList rcl) {
        this.rcl = rcl;
        rcSet = new HashSet<>();
        for (ResultColumn rc : rcl) {
            rcSet.add(rc);
        }
        cache = new HashMap<>();
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (!(node instanceof ColumnReference)) {
            return node;
        }

        return updateColumnReference((ColumnReference) node);
    }

    public int getNumSubstitutions() {
        return numSubstitutions;
    }

    public boolean stopTraversal() 
    {
        return false;
    }

    public boolean skipChildren(Visitable node) {
        return false;
    }

    public boolean visitChildrenFirst(Visitable node)
    {
        return false;
    }

    private ColumnReference updateColumnReference(ColumnReference cr) throws StandardException {
        if (cache.containsKey(cr)) {
            return cache.get(cr);
        } else {
            cache.put(cr, cr);
        }

        ResultColumn source = cr.getSource();
        if (source == null || rcSet.contains(source)) {
            return cr;
        }

        // we have a source that doesn't come from rcl, update it
        source = source.getNestedQueryBlockResultColumn();
        if (source == null) {
            return cr;
        }

        for (int i = 0; i < rcl.size(); i++) {
            ResultColumn rc = rcl.elementAt(i);
            if (source.equals(rc.getNestedQueryBlockResultColumn())) {
                ColumnReference newCR = rc.getColumnReference(cr);
                cache.put(cr, newCR);
                return newCR;
            }
        }
        return cr;
    }
}
