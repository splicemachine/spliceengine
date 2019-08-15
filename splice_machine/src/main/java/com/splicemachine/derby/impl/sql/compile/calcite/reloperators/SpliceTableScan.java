package com.splicemachine.derby.impl.sql.compile.calcite.reloperators;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.derby.impl.sql.compile.NestedLoopJoinStrategy;
import com.splicemachine.derby.impl.sql.compile.calcite.SpliceContext;
import com.splicemachine.derby.impl.sql.compile.calcite.SpliceTable;
import com.splicemachine.derby.impl.sql.compile.calcite.rules.SpliceConverterRule;
import com.splicemachine.utils.Pair;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * Created by yxia on 8/27/19.
 */
public class SpliceTableScan extends TableScan implements SpliceRelNode {
    // define access path
    ConglomerateDescriptor cd;
    public SpliceTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public ResultSetNode implement(SpliceImplementor implementor) throws StandardException {
        SpliceContext sc = implementor.sc;
        NodeFactory nodeFactory = implementor.nodeFactory;
        ContextManager contextManager = implementor.contextManager;

        // get tablenumber
        SpliceTable spliceTable = table.unwrap(SpliceTable.class);
        int tableNumber = spliceTable.getTableNumber();
        FromBaseTable fromBaseTable = spliceTable.getFromBaseTableNode();

        // create a result column list according to the RowType
        ResultColumnList rcl = (ResultColumnList) nodeFactory.getNode(C_NodeTypes.RESULT_COLUMN_LIST, contextManager);
        for (RelDataTypeField field : rowType.getFieldList()) {
            ResultColumn rc = sc.getBaseColumn(Pair.newPair(tableNumber, field.getIndex()+1));
            rc.setReferenced();
            /*
            ResultColumn rc = (ResultColumn) nodeFactory.getNode(C_NodeTypes.RESULT_COLUMN,
                    null,
                    null,
                    contextManager);

            implementor.setNameAndType(field, rc);

            // construct the source expression
            BaseColumnNode baseColumnNode = (BaseColumnNode) nodeFactory.getNode(C_NodeTypes.BASE_COLUMN_NODE, )
            ValueNode sourceValue;
            if (tuple != null) {
                sourceValue = implementor.literalToValueNode(tuple.get(field.getIndex()));
            } else {
                sourceValue = rc.getNullNode(rc.getTypeServices());
            }
            rc.setExpression(sourceValue);
            */
            rcl.addResultColumn(rc);
        }

        // set referencedColumn
        fromBaseTable.setResultColumns(rcl);
        fromBaseTable.setReferencedCols();
        //populate the referencedTableMap
        int numTables = sc.getCompilerContext().getMaximalPossibleTableCount();
        JBitSet tableMap = new JBitSet(numTables);
        tableMap.set(tableNumber);
        fromBaseTable.setReferencedTableMap(tableMap);

        // set trulyTheBestAccessPath
        // for now, do not consider index, so always use base table
        AccessPathImpl accessPath = new AccessPathImpl();
        accessPath.setConglomerateDescriptor(fromBaseTable.getBaseConglomerateDescriptor());
        CostEstimate costEstimate = sc.newCostEstimate();
        costEstimate.setCost(0.1d, 1.0d, 1.0d);
        accessPath.setCostEstimate(costEstimate);
        accessPath.setCoveringIndexScan(false);
        accessPath.setNonMatchingIndexScan(false);
        accessPath.setLockMode(0);
        accessPath.setMissingHashKeyOK(false);
        accessPath.setJoinStrategy(new NestedLoopJoinStrategy());
        fromBaseTable.setTrulyTheBestAccessPath(accessPath);

        // no condition right now
        PredicateList storeRestrictionList=(PredicateList)nodeFactory.getNode(C_NodeTypes.PREDICATE_LIST, contextManager);
        PredicateList nonStoreRestrictionList=(PredicateList)nodeFactory.getNode(C_NodeTypes.PREDICATE_LIST, contextManager);
        fromBaseTable.setConditionList(storeRestrictionList, nonStoreRestrictionList);
/*
        setConglomerateDescriptor(copyFrom.getConglomerateDescriptor());
        setCostEstimate(copyFrom.getCostEstimate());
        setCoveringIndexScan(copyFrom.getCoveringIndexScan());
        setNonMatchingIndexScan(copyFrom.getNonMatchingIndexScan());
        setJoinStrategy(copyFrom.getJoinStrategy());
        setHintedJoinStrategy(copyFrom.isHintedJoinStrategy());
        setLockMode(copyFrom.getLockMode());
        setMissingHashKeyOK(copyFrom.isMissingHashKeyOK());
 */
        // for now, only nested loop join
        /*
        JoinStrategy joinStrategy = new NestedLoopJoinStrategy();
        fromBaseTable.initAccessPaths();
        */

        return fromBaseTable;
    }

    @Override public void register(RelOptPlanner planner) {
        for (RelOptRule rule : SpliceConverterRule.RULES) {
            planner.addRule(rule);
        }
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // add dummy cost
        return planner.getCostFactory().makeCost(2, 2, 2);
    }
}
