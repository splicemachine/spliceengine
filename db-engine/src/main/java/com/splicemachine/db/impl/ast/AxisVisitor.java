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

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import splice.com.google.common.base.Predicate;

/**
 * Visitor that applies a visitor along a traversal "axis" defined by a predicate. Only nodes that pass the predicate
 * are considered axes along which to traverse, or, in other words, are considered branches.
 *
 * @author P Trolard Date: 26/02/2014
 */
public class AxisVisitor implements Visitor {

    private final Visitor delegateVisitor;

    /* Only visit children for which this predicate evaluates to true */
    private final Predicate<? super Visitable> onAxisPredicate;

    public AxisVisitor(final Visitor v, final Predicate<? super Visitable> onAxisPredicate) {
        this.delegateVisitor = v;
        this.onAxisPredicate = onAxisPredicate;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        return delegateVisitor.visit(node, parent);
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return delegateVisitor.visitChildrenFirst(node);
    }

    @Override
    public boolean stopTraversal() {
        return delegateVisitor.stopTraversal();
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        boolean shouldVisitChildren = onAxisPredicate.apply(node);
        return !shouldVisitChildren || delegateVisitor.skipChildren(node);
    }
}
