package com.splicemachine.db.impl.sql.calcite.reloperators;

import com.splicemachine.db.impl.sql.calcite.rules.SpliceConverterRule;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.TableScan;

/**
 * Created by yxia on 8/27/19.
 */
public class SpliceTableScan extends TableScan implements SpliceRelNode {
    public SpliceTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public void implement(Implementor implementor) {

    }

    @Override public void register(RelOptPlanner planner) {
        for (RelOptRule rule : SpliceConverterRule.RULES) {
            planner.addRule(rule);
        }
    }
}
