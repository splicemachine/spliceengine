package com.splicemachine.derby.impl.sql.compile.calcite.reloperators;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.derby.impl.sql.compile.calcite.SpliceContext;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

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
    public ResultSetNode implement(SpliceImplementor implementor) throws StandardException {
        SpliceContext sc = implementor.sc;
        NodeFactory nodeFactory = implementor.nodeFactory;
        ContextManager contextManager = implementor.contextManager;

        // implement the source first
        ResultSetNode source = ((SpliceRelNode) input).implement(implementor);

        // construct the result column list
        ResultColumnList rcl = (ResultColumnList) nodeFactory.getNode(C_NodeTypes.RESULT_COLUMN_LIST, contextManager);
        for (Pair<RexNode, RelDataTypeField> pair : Pair.zip(getProjects(), rowType.getFieldList())) {
            ResultColumn rc = (ResultColumn)nodeFactory.getNode(C_NodeTypes.RESULT_COLUMN,
                    null,
                    null,
                    contextManager);

            implementor.setNameAndType(pair.right, rc);
            RexNode sourceExp = pair.left;
            if (sourceExp instanceof RexInputRef) {
                int index = ((RexInputRef) sourceExp).getIndex();
                // this points to the source
                // note, index in RexInputRef is 0-based
                VirtualColumnNode vc = (VirtualColumnNode)nodeFactory.getNode(C_NodeTypes.VIRTUAL_COLUMN_NODE,
                        source,
                        source.getResultColumns().elementAt(index),
                        ReuseFactory.getInteger(index+1),
                        contextManager);
                rc.setExpression(vc);
                rc.setReferenced();
            } else if (sourceExp instanceof RexLiteral) {
                rc.setExpression(implementor.convertExpression(sourceExp, null));
                rc.setReferenced();
            } else {
                // TODO
                assert false: "TODO: handle more complex scenarios";
            }

            rcl.addResultColumn(rc);
        }

        // manufacture a ProjectRestrictNode
        ProjectRestrictNode prn = (ProjectRestrictNode) nodeFactory.getNode(
                C_NodeTypes.PROJECT_RESTRICT_NODE,
                source,		/* Child ResultSet */
                rcl,	/* Projection */
                null,			/* Restriction */
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
}
