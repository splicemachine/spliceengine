package com.splicemachine.db.impl.sql.calcite.reloperators;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

/**
 * Created by yxia on 9/5/19.
 */
public class SpliceValues extends Values implements SpliceRelNode {
    public SpliceValues(RelOptCluster cluster,
                        RelDataType rowType,
                        ImmutableList<ImmutableList<RexLiteral>> tuples,
                        RelTraitSet traits) {
        super(cluster, rowType, tuples, traits);
        assert getConvention() == SpliceRelNode.CONVENTION;
    }

    public SpliceValues copy(RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
        return new SpliceValues(getCluster(), rowType, tuples, traitSet);
    }

    @Override
    public void implement(SpliceRelNode.Implementor implementor) {

    }
}
