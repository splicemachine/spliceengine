/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.impl.sql.compile.aggregatepush;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import com.splicemachine.db.impl.ast.AbstractSpliceVisitor;
import com.splicemachine.db.impl.ast.CollectingVisitorBuilder;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;

/**
 * Created by yxia on 4/3/17.
 */
public class AggregatePushingVisitor extends AbstractSpliceVisitor implements Visitor {
    public static final String AGGREGATE_PUSH_ALIAS_PREFIX = "AggregatePush-";
    private int pushCount = 0;
    private int aggCount = 0;

    public AggregatePushingVisitor () {
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return true;
    }

    @Override
    public Visitable visit(Visitable node, QueryTreeNode parent) throws StandardException {
        if (!(node instanceof SelectNode) || ((SelectNode) node).getSelectAggregates().isEmpty())
            return node;

        SelectNode topSelectNode = (SelectNode)node;

        // do not trigger aggregate pushing if whereAggregates and havingAggregates are not empty
        // we need to either do together or not to do
        if (topSelectNode.getWhereAggregates() != null && !topSelectNode.getWhereAggregates().isEmpty()
                || topSelectNode.getHavingAggregates() != null && !topSelectNode.getHavingAggregates().isEmpty())
            return node;

        //TODO
        // if there are aggregates other than Min, Max, Count Sum, do not trigger this optimization

        List<AggregateNode> pushableAggregates = new ArrayList<>();
        for (AggregateNode aggregateNode: topSelectNode.getSelectAggregates()) {
            if (!aggregateNode.isWindowFunction() && !aggregateNode.isDistinct()) {
                // only consider single field on the aggregation
                if (aggregateNode.getOperand() instanceof ColumnReference)
                    pushableAggregates.add(aggregateNode);
                else
                    return node;
            } else
                return node;
        }

        for (AggregateNode aggregateNode: pushableAggregates) {
            push(topSelectNode, aggregateNode);
        }

        return node;
    }

    private boolean push(SelectNode topSelectNode, AggregateNode aggregateNode) throws StandardException {
        //get the base table from the FromTable list
        ColumnReference cr = (ColumnReference)aggregateNode.getOperand();

        int tableNumber = cr.getTableNumber();
        FromTable ft = null;
        FromList fromList = topSelectNode.getFromList();

        int tabPosInFL = -1;

        for (tabPosInFL=0; tabPosInFL<fromList.size(); tabPosInFL++) {
            ft = (FromTable)fromList.elementAt(tabPosInFL);
            if (ft.getTableNumber() == tableNumber)
                break;
        }

        assert ft != null;

        //create a FromSubquery from it with groupby and aggregate + count in the select list
        if (ft instanceof FromBaseTable) {
            FromSubquery fromSubquery = replaceTableWithFromSubquery(
                    aggregateNode, (FromBaseTable)ft, tableNumber, topSelectNode, tabPosInFL);
        } else
            return false;

        return true;

    }

    private FromSubquery replaceTableWithFromSubquery(AggregateNode aggregateNode,
                                                      FromBaseTable ft,
                                                      int baseTableNumber,
                                                      SelectNode outerSelect,
                                                      int tabPosInFL) throws StandardException {
        ContextManager cm = outerSelect.getContextManager();
        NodeFactory nf = outerSelect.getNodeFactory();

        //create a SelectNode
        // 1: create a FromList to hold the base table
        FromList fromList = (FromList)nf.getNode(C_NodeTypes.FROM_LIST, cm);
        fromList.addFromTable(ft);

        // 2: result column list contains all the columns referenced in the join predicates
        // and group by columns, the pushed down aggregation, and the count(*) column

        ResultColumnList rcl = (ResultColumnList) nf.getNode(C_NodeTypes.RESULT_COLUMN_LIST, cm);

        SelectNode selectNode = (SelectNode)nf.getNode(
                        C_NodeTypes.SELECT_NODE,
                        rcl,      // ResultColumns
                        null,     // AGGREGATE list
                        fromList, // FROM list
                        null,     // WHERE clause
                        null,     // GROUP BY list
                        null,     // having clause
                        null, /* window list */
                        cm);

        // add group by columns
        List<ColumnReference> cfl = CollectingVisitorBuilder.<ColumnReference>forClass(ColumnReference.class)
                .collect(outerSelect.getWhereClause());
        for (ValueNode cf:cfl) {
            if (cf.getTableNumber() != baseTableNumber)
                continue;

            AggregateUtil.addGroupByNode(selectNode, cf);
        }

        //add join columns
        cfl = CollectingVisitorBuilder.<ColumnReference>forClass(ColumnReference.class)
                .collect(outerSelect.getGroupByList());
        for (ValueNode cf:cfl) {
            if (cf.getTableNumber() != baseTableNumber)
                continue;
            AggregateUtil.addGroupByNode(selectNode, cf);
        }

        // according to bindExpressions() in SelectNode, the following fields are always set even
        // if they are empty
        selectNode.setupnInitFields();

        //Push the original aggregate to the base table
        int aggrColId = AggregateUtil.addAggregateNode(selectNode, aggregateNode, ++aggCount);

        //Generate Count aggregation on the base table
        AggregateUtil.addCountAggregation(selectNode, ++aggCount);

        //genreate RCL for FromSubquery
        ResultColumnList newRcl = selectNode.getResultColumns().copyListAndObjects();
        newRcl.genVirtualColumnNodes(selectNode, selectNode.getResultColumns());

        // Insert the new FromSubquery into to origSelectNode's From list.
        FromSubquery fromSubquery = (FromSubquery)nf.getNode(C_NodeTypes.FROM_SUBQUERY,
                selectNode,
                null,                  // order by
                null,                  // offset
                null,                  // fetchFirst
                false,                 // hasJDBClimitClause
                getSubqueryAlias(),
                newRcl,
                null,
                cm);
        fromSubquery.setTableNumber(outerSelect.getCompilerContext().getNextTableNumber());
        FromTable baseTable = (FromTable)outerSelect.getFromList().elementAt(tabPosInFL);
        int origNestingLevel = baseTable.getLevel();
        fromSubquery.setLevel(origNestingLevel);
        //increase the original table's nestinglevel
        selectNode.getFromList().setLevel(origNestingLevel+1);
        baseTable.setLevel(origNestingLevel + 1);

        //TODO: should we push in single table predicate?

        // update all column references' nestinglevel in the fromSubquery
        ColumnNestingLevelAdjustor cnlAdjustor = new ColumnNestingLevelAdjustor(baseTableNumber);
        fromSubquery.accept(cnlAdjustor);

        // replace the original FromBaseTable with the new FromSubquery
        outerSelect.getFromList().setElementAt(fromSubquery, tabPosInFL);

        //Generate the final aggregate based on the original one
        AggregateUtil.rewriteFinalAggregateNode(outerSelect, aggregateNode, fromSubquery, aggrColId);


        // update column references in the outer query block for all the column references
        // originally pointing to this base table
        // construct a mapping between the original fieldref and the fieldref to FromSubquery
        HashMap<ColumnReference, ResultColumn> fieldMap = new HashMap<>();
        for (int i=0; i<rcl.size(); i++) {
            ResultColumn resultColumn = rcl.elementAt(i);
            if (resultColumn.isGroupingColumn() && resultColumn.getExpression() instanceof ColumnReference)
                fieldMap.put((ColumnReference)resultColumn.getExpression(), newRcl.elementAt(i));
        }

        ColumnMapVisitor columnMapVisitor =
                new ColumnMapVisitor(fieldMap, baseTableNumber, fromSubquery.getTableNumber());

        outerSelect.getResultColumns().accept(columnMapVisitor);
        outerSelect.getWhereClause().accept(columnMapVisitor);
        GroupByList gbList = outerSelect.getGroupByList();
        if (gbList != null)
            gbList.accept(columnMapVisitor);
        OrderByList obList = outerSelect.getOrderByList();
        if (obList != null)
            obList.accept(columnMapVisitor);
        //TODO: what about other fields, like having, and original Where ...
        return fromSubquery;
    }

    private String getSubqueryAlias() {
        return String.format(AGGREGATE_PUSH_ALIAS_PREFIX + "%s", ++pushCount);
    }

}
