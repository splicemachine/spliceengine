package com.splicemachine.derby.impl.sql.compile.calcite.reloperators;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.ResultSetNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Created by yxia on 9/5/19.
 */
public interface SpliceRelNode extends RelNode {
    ResultSetNode implement(SpliceImplementor implementor) throws StandardException;

    /**
     * Calling convention for relational operations that occur in Splice.
     */
    Convention CONVENTION = new Convention.Impl("SPLICE", SpliceRelNode.class);

}

