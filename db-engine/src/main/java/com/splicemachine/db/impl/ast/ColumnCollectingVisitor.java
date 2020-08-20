package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.OrderedColumn;
import com.splicemachine.db.impl.sql.compile.VirtualColumnNode;
import splice.com.google.common.base.Predicate;

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
