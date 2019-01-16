/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.OrderedColumn;
import com.splicemachine.db.impl.sql.compile.VirtualColumnNode;
import org.spark_project.guava.base.Predicate;

/**
 * Created by yxia on 4/18/18.
 */
public class ColumnCollectingVisitor extends CollectingVisitor {
    public ColumnCollectingVisitor(Predicate<? super Visitable> nodePred) {
        super(nodePred);
    }

    @Override
    public boolean skipChildren(Visitable node) {
        if (node instanceof ColumnReference
                || node instanceof VirtualColumnNode
                || node instanceof OrderedColumn)
            return true;
        else
            return false;
    }
}
