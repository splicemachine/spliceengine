package com.splicemachine.db.impl.sql.calcite.reloperators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/**
 * Created by yxia on 9/5/19.
 */
public class SpliceFilter extends Filter implements SpliceRelNode {

    public SpliceFilter(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        RelNode child,
                        RexNode condition) {
        super(cluster, traitSet, child, condition);
        assert getConvention() == SpliceRelNode.CONVENTION;
        assert getConvention() == child.getConvention();
    }

    public SpliceFilter copy(RelTraitSet traitSet, RelNode input,
                            RexNode condition) {
        return new SpliceFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public void implement(SpliceRelNode.Implementor implementor) {

    }
}
