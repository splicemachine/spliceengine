package com.splicemachine.db.impl.sql.calcite.reloperators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Created by yxia on 9/5/19.
 */
public class SpliceProject extends Project implements SpliceRelNode {
    public SpliceProject(RelOptCluster cluster, RelTraitSet traitSet,
                         RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traitSet, input, projects, rowType);
    }

    @Override public Project copy(RelTraitSet traitSet, RelNode input,
                                  List<RexNode> projects, RelDataType rowType) {
        return new SpliceProject(getCluster(), traitSet, input, projects,
                rowType);
    }

    @Override
    public void implement(SpliceRelNode.Implementor implementor) {

    }
}
