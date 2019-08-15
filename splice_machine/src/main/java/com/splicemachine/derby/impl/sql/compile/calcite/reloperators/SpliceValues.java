package com.splicemachine.derby.impl.sql.compile.calcite.reloperators;

import com.google.common.collect.ImmutableList;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.derby.impl.sql.compile.calcite.SpliceContext;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

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

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // add dummy cost
        return planner.getCostFactory().makeCost(1, 1, 1);
    }

    @Override
    public ResultSetNode implement(SpliceImplementor implementor) throws StandardException {
        SpliceContext sc = implementor.sc;
        NodeFactory nodeFactory = implementor.nodeFactory;
        ContextManager contextManager = implementor.contextManager;

        // get the constant value row
        List<RexLiteral> tuple = null;
        if (!tuples.isEmpty())
            tuple = tuples.get(0);

        // create a result column list
        ResultColumnList rcl = (ResultColumnList)nodeFactory.getNode( C_NodeTypes.RESULT_COLUMN_LIST, contextManager);
        for (RelDataTypeField field: rowType.getFieldList()) {
            ResultColumn rc = (ResultColumn)nodeFactory.getNode(C_NodeTypes.RESULT_COLUMN,
                    null,
                    null,
                    contextManager);

            implementor.setNameAndType(field, rc);
            ValueNode sourceValue;
            if (tuple != null) {
                sourceValue = implementor.convertExpression((RexNode)tuple.get(field.getIndex()), null);
            } else {
                sourceValue = rc.getNullNode(rc.getTypeServices());
            }
            rc.setExpression(sourceValue);
            rcl.addResultColumn(rc);
        }

        // create a RowResultSetNode
        RowResultSetNode resultSetNode = (RowResultSetNode)nodeFactory.getNode(C_NodeTypes.ROW_RESULT_SET_NODE, rcl, null, contextManager);
        resultSetNode.setLevel(0);
        resultSetNode.setTableNumber(0);
        /* Allocate a dummy referenced table map */
        int numTables = sc.getCompilerContext().getMaximalPossibleTableCount();
        JBitSet tableMap = new JBitSet(numTables);
        tableMap.set(0);
        resultSetNode.setReferencedTableMap(tableMap);

        CostEstimate costEstimate = sc.newCostEstimate();
        costEstimate.setCost(0.1d, 1.0d, 1.0d);
        resultSetNode.fillInCostEstimate(costEstimate);
        //resultSetNode.estimateCost(null, null, null, sc.getDeryOptimizer(), null);

        // we may need to add an unsat condition to it
        if (tuple == null) {
            Predicate unsatPredicate = Predicate.generateUnsatPredicate(numTables, nodeFactory, contextManager);
            PredicateList predList = (PredicateList)nodeFactory.getNode( C_NodeTypes.PREDICATE_LIST, contextManager);
            predList.addPredicate(unsatPredicate);

            ResultColumnList prRCL = rcl;
            rcl = rcl.copyListAndObjects();
            resultSetNode.setResultColumns(rcl);
            prRCL.genVirtualColumnNodes(resultSetNode, rcl);

            // Add ProjectRestrictNode ontop with unsat condition
            ProjectRestrictNode newPRN = (ProjectRestrictNode) nodeFactory.getNode(
                    C_NodeTypes.PROJECT_RESTRICT_NODE,
                    resultSetNode,		/* Child ResultSet */
                    prRCL,	/* Projection */
                    null,			/* Restriction */
                    predList,			/* Restriction as PredicateList */
                    null,			/* Subquerys in Projection */
                    null,			/* Subquerys in Restriction */
                    null,          /* table properties */
                    contextManager);

            newPRN.setLevel(resultSetNode.getLevel());
            // set referenced tableMap for the PRN
            newPRN.setReferencedTableMap((JBitSet)tableMap.clone());


            // TODO: compute cost
            return newPRN;
        }

        return resultSetNode;
    }
}
