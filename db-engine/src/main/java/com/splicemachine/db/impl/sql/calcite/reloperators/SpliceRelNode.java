package com.splicemachine.db.impl.sql.calcite.reloperators;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Created by yxia on 9/5/19.
 */
public interface SpliceRelNode extends RelNode {
    void implement(Implementor implementor);

    /**
     * Calling convention for relational operations that occur in Splice.
     */
    Convention CONVENTION = new Convention.Impl("SPLICE", SpliceRelNode.class);

    class Implementor {
        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((SpliceRelNode) input).implement(this);
        }

    }
}

