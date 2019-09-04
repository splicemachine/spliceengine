package com.splicemachine.db.impl.sql.calcite.reloperators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.HashSet;

/**
 * Created by yxia on 9/5/19.
 */
public class SpliceJoin extends Join implements SpliceRelNode {
    public SpliceJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
                      RexNode condition, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, new HashSet<>(0), joinType);
        assert getConvention() == SpliceRelNode.CONVENTION;
    }

    public SpliceJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
                           RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new SpliceJoin(getCluster(), traitSet, left, right, conditionExpr, joinType);
    }

    @Override
    public void implement(SpliceRelNode.Implementor implementor) {

    }
}
