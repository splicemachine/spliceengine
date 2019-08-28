package com.splicemachine.db.impl.sql.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

/**
 * Created by yxia on 8/27/19.
 */
public class SpliceTableScan extends TableScan implements SpliceRel {
    public SpliceTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, table);
    }
}
