package com.splicemachine.derby.impl.sql.compile.calcite.rules;

import com.splicemachine.derby.impl.sql.compile.calcite.reloperators.SpliceFilter;
import com.splicemachine.derby.impl.sql.compile.calcite.reloperators.SpliceProject;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

/**
 * Created by yxia on 9/20/19.
 */
public class SpliceFilterToProjectRule extends RelOptRule {
    public static final SpliceFilterToProjectRule INSTANCE = new SpliceFilterToProjectRule();

    public SpliceFilterToProjectRule() {
        super(operand(SpliceProject.class, operand(SpliceFilter.class, none())), "SpliceFilterToProjectRule");
    }

    public boolean matches(RelOptRuleCall call) {
        return true;
    }

    public void onMatch(RelOptRuleCall call) {
        SpliceProject project = call.rel(0);
        SpliceFilter filter = call.rel(1);
        RelNode converted = convert(project, filter);
        call.transformTo(converted);
    }

    private RelNode convert(SpliceProject project, SpliceFilter filter) {
        return new SpliceProject(project.getCluster(), project.getTraitSet(), filter.getInput(), project.getProjects(), filter.getCondition(), project.getRowType());
    }
}
