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
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Created by yxia on 9/5/19.
 */
public class SpliceProject extends Project implements SpliceRelNode {
    RexNode condition;

    public SpliceProject(RelOptCluster cluster, RelTraitSet traitSet,
                         RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traitSet, input, projects, rowType);
        condition = null;
    }

    public SpliceProject(RelOptCluster cluster, RelTraitSet traitSet,
                         RelNode input, List<? extends RexNode> projects, RexNode condition, RelDataType rowType) {
        super(cluster, traitSet, input, projects, rowType);
        this.condition = condition;
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input,
                                  List<RexNode> projects, RelDataType rowType) {
        return new SpliceProject(getCluster(), traitSet, input, projects,
                rowType);
    }

    public Project copy(RelTraitSet traitSet, RelNode input,
                        List<RexNode> projects, RexNode condition, RelDataType rowType) {
        return new SpliceProject(getCluster(), traitSet, input, projects, condition,
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

        ValueNode convertConditions = null;
        if (condition != null) {
            convertConditions = implementor.convertCondition(condition, source);
        }
        // manufacture a ProjectRestrictNode
        ProjectRestrictNode prn = (ProjectRestrictNode) nodeFactory.getNode(
                C_NodeTypes.PROJECT_RESTRICT_NODE,
                source,		/* Child ResultSet */
                rcl,	/* Projection */
                convertConditions,			/* Restriction */
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

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // add dummy clost
        return planner.getCostFactory().makeCost(2, 2, 2);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);

        for (Ord<RelDataTypeField> field : Ord.zip(rowType.getFieldList())) {
            String fieldName = field.e.getName();
            if (fieldName == null) {
                fieldName = "field#" + field.i;
            }
            pw.item(fieldName, exps.get(field.i));
        }

        if (condition != null)
            pw.item("condition", condition);

        // If we're generating a digest, include the rowtype. If two projects
        // differ in return type, we don't want to regard them as equivalent,
        // otherwise we will try to put rels of different types into the same
        // planner equivalence set.
        //CHECKSTYLE: IGNORE 2
        if ((pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
                && false) {
            pw.item("type", rowType);
        }

        return pw;
    }
}
