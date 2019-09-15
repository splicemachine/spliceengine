package com.splicemachine.db.impl.sql.calcite.reloperators;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.impl.sql.calcite.SpliceContext;
import com.splicemachine.db.impl.sql.calcite.SpliceTable;
import com.splicemachine.db.impl.sql.calcite.rules.SpliceConverterRule;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import com.splicemachine.db.impl.sql.compile.ResultColumn;
import com.splicemachine.db.impl.sql.compile.ResultColumnList;
import com.splicemachine.db.impl.sql.compile.ResultSetNode;
import com.splicemachine.utils.Pair;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.TableScan;
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

        // set trulyTheBestAccessPath
        // for now, do not consider index, so always use base table

        // for now, only nested loop join
        /*
        JoinStrategy joinStrategy =
        fromBaseTable.initAccessPaths();
        */

        return fromBaseTable;
    }

    @Override public void register(RelOptPlanner planner) {
        for (RelOptRule rule : SpliceConverterRule.RULES) {
            planner.addRule(rule);
        }
    }
}
