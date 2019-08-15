package com.splicemachine.derby.impl.sql.compile.calcite.reloperators;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.sql.compile.ProjectRestrictNode;
import com.splicemachine.db.impl.sql.compile.ResultColumnList;
import com.splicemachine.db.impl.sql.compile.ResultSetNode;
import com.splicemachine.db.impl.sql.compile.ValueNode;
import com.splicemachine.derby.impl.sql.compile.calcite.SpliceContext;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
    public ResultSetNode implement(SpliceImplementor implementor) throws StandardException {
        SpliceContext sc = implementor.sc;
        NodeFactory nodeFactory = implementor.nodeFactory;
        ContextManager contextManager = implementor.contextManager;

        // implement the child first
        ResultSetNode source = ((SpliceRelNode) input).implement(implementor);

        // convert the current node to a ProjectRestrict node
        // construct the result column list, which should be a direct mapping from the source resultset
        ResultColumnList prRCL = source.getResultColumns();
        ResultColumnList rcl = prRCL.copyListAndObjects();
        source.setResultColumns(rcl);
        prRCL.genVirtualColumnNodes(source, rcl);

        // convert the conditions
        ValueNode convertConditions = implementor.convertCondition(condition, source);

        // manufacture a ProjectRestrictNode
        ProjectRestrictNode prn = (ProjectRestrictNode) nodeFactory.getNode(
                C_NodeTypes.PROJECT_RESTRICT_NODE,
                source,		/* Child ResultSet */
                prRCL,	/* Projection */
                convertConditions,	/* Restriction */
                null,			/* Restriction as PredicateList */
                null,			/* Subquerys in Projection */
                null,			/* Subquerys in Restriction */
                null,          /* table properties */
                contextManager);

        // set referenced tableMap for the PRN
        source.getReferencedTableMap();
        prn.setReferencedTableMap((JBitSet)source.getReferencedTableMap().clone());

        // set cost
        CostEstimate costEstimate = sc.newCostEstimate();
        costEstimate.setCost(0.1d, 1.0d, 1.0d);
        prn.fillInCostEstimate(costEstimate);

        return prn;
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // add dummy cost
        return planner.getCostFactory().makeCost(2, 2, 2);
    }
}
