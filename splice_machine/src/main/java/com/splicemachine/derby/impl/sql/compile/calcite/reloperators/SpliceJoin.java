package com.splicemachine.derby.impl.sql.compile.calcite.reloperators;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.ResultSetNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
    public ResultSetNode implement(SpliceImplementor implementor) throws StandardException {
        return null;
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // add dummy cost
        return planner.getCostFactory().makeCost(4, 4, 4);
    }
}
