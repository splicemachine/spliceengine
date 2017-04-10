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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yxia on 4/4/17.
 */
public class AggregateUtil {

    private static ResultColumn newResultColumn(NodeFactory nodeFactory, ContextManager contextManager, ValueNode groupByCol) throws StandardException {
        ResultColumn rc = (ResultColumn) nodeFactory.getNode(
                C_NodeTypes.RESULT_COLUMN,
                groupByCol.getColumnName(),
                groupByCol,
                contextManager);
        rc.setColumnDescriptor(null);
        return rc;
    }

    public static int addAggregateNode(SelectNode selectNode, AggregateNode aggregateNode, int aggCount) throws StandardException {

        ResultColumn rc = newResultColumn(selectNode.getNodeFactory(), selectNode.getContextManager(), aggregateNode);

        rc.setReferenced();
        rc.setResultSetNumber(selectNode.getResultSetNumber());
        selectNode.getResultColumns().addResultColumn(rc);

        rc.setName(String.format("%s-%s",aggregateNode.getAggregateName(), aggCount));
        rc.setNameGenerated(true);

        List<AggregateNode> aggList = selectNode.getSelectAggregates();

        aggList.add(aggregateNode);
        return rc.getVirtualColumnId();
    }

    public static AggregateNode rewriteFinalAggregateNode(SelectNode outerSelectNode,
                                                           AggregateNode origAggregateNode,
                                                           FromSubquery fromSubquery,
                                                           int aggrColId) throws StandardException {

        String origAggName = origAggregateNode.getAggregateName();
        Class  aggregateClass = null;
        String aggregateName = origAggName;

        switch (origAggName)
        {
            case "MAX":
            case "MIN":
                aggregateClass = MaxMinAggregateDefinition.class;
                break;
            case "SUM":
            case "COUNT":
                aggregateClass = SumAvgAggregateDefinition.class;
                aggregateName = "SUM";
                break;
            default:
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT("Unsupported aggregate: "
                            + origAggName);
                }
                break;
        }

        //prepare ValueNode
        ResultColumn aggrColumn = fromSubquery.getResultColumns().elementAt(aggrColId-1);

        ColumnReference expressionNode = (ColumnReference)outerSelectNode.
                getNodeFactory().getNode(
                C_NodeTypes.COLUMN_REFERENCE,
                aggrColumn.getName(),
                null,
                outerSelectNode.getContextManager());


        AggregateNode finalAggr = (AggregateNode)outerSelectNode.
                getNodeFactory().getNode(C_NodeTypes.AGGREGATE_NODE,
                expressionNode,
                aggregateClass,
                origAggregateNode.isDistinct(),
                aggregateName,
                outerSelectNode.getContextManager());

        SubqueryList selectSubquerys=(SubqueryList)outerSelectNode.getNodeFactory().getNode(
                C_NodeTypes.SUBQUERY_LIST,
                outerSelectNode.getContextManager());
        List<AggregateNode> aggregateVector = new ArrayList<>();
        finalAggr.bindExpression(outerSelectNode.getFromList(), selectSubquerys, aggregateVector);

        //replace original aggregate node in selectAggregate with the new one
        int i = -1;
        for (AggregateNode tempNode: outerSelectNode.getSelectAggregates()) {
            i++;
            if (tempNode.equals(origAggregateNode))
                break;
        }
        assert (i==-1);

        outerSelectNode.getSelectAggregates().set(i, finalAggr);

        //locate the original aggregate node position in RCL
        ResultColumnList rcl = outerSelectNode.getResultColumns();

        for (i=0; i<rcl.size(); i++) {
            ResultColumn rc = rcl.elementAt(i);
            if (rc.getExpression().equals(origAggregateNode)) {
                rc.setExpression(finalAggr);
                break;
            }
        }

        assert (i>=rcl.size());

        return finalAggr;

    }

    public static int addCountAggregation(SelectNode selectNode,
                                           int aggCount) throws StandardException {
        ValueNode valueNode = null;

        AggregateNode countAggr = (AggregateNode)selectNode.
                getNodeFactory().getNode(C_NodeTypes.AGGREGATE_NODE,
                valueNode,
                CountAggregateDefinition.class,
                false,
                "COUNT",
                selectNode.getContextManager());


        SubqueryList selectSubquerys=(SubqueryList)selectNode.getNodeFactory().getNode(
                C_NodeTypes.SUBQUERY_LIST,
                selectNode.getContextManager());
        List<AggregateNode> aggregateVector = new ArrayList<>();
        countAggr.bindExpression(selectNode.getFromList(), selectSubquerys, aggregateVector);

        return addAggregateNode(selectNode, countAggr, aggCount);
    }

    public static void replaceAggregateNode(SelectNode selectNode,
                                            AggregateNode finalAggregateNode,
                                            AggregateNode origAggregateNode) throws StandardException {
    }

    public static int locateColumn(ResultColumnList targetRCL, ValueNode colRef) throws StandardException {
        int rclSize = targetRCL.size();
        int retVal = -1;
        for (int i=0; i<rclSize; i++) {
            ResultColumn selectListRC=targetRCL.elementAt(i);
            if(!(selectListRC.getExpression() instanceof ColumnReference)){
                continue;
            }
            ColumnReference selectListCR=(ColumnReference)selectListRC.getExpression();

            if(selectListCR.isEquivalent(colRef))
                break;
        }
        return retVal;
    }

    public static void addGroupByNode(SelectNode selectNode, ValueNode groupByCol) throws StandardException {
        // check if the node has already been added
        int columnPosition = locateColumn(selectNode.getResultColumns(), groupByCol);

        //column already added, return
        if (columnPosition >=0)
            return;


        //
        // PART 1: Add column ref to result columns
        //
        assert (groupByCol instanceof ColumnReference);
        ColumnReference tmpColumnRef= (ColumnReference)selectNode.
                getNodeFactory().getNode(C_NodeTypes.COLUMN_REFERENCE,
                groupByCol.getColumnName(),
                null,
                selectNode.getContextManager());
        tmpColumnRef.setSource(((ColumnReference) groupByCol).getSource());
        tmpColumnRef.setNestingLevel(((ColumnReference) groupByCol).getNestingLevel());
        tmpColumnRef.setSourceLevel(tmpColumnRef.getNestingLevel());
        tmpColumnRef.setTableNumber(groupByCol.getTableNumber());

        ResultColumn rc = newResultColumn(selectNode.getNodeFactory(), selectNode.getContextManager(), tmpColumnRef);


        ColumnReference colRef = (ColumnReference) groupByCol;
        if (colRef.getTableNameNode() != null) {
            rc.setSourceSchemaName(colRef.getTableNameNode().getSchemaName());
            rc.setSourceTableName(colRef.getTableNameNode().getTableName());
        }

        rc.setReferenced();
        rc.markAsGroupingColumn();
        rc.setResultSetNumber(selectNode.getResultSetNumber());
        selectNode.getResultColumns().addResultColumn(rc);

        //
        // PART 2: Add the GroupByList and GroupByColumn
        //

        // Create GroupByList if there isn't already one in the subquery.
        if (selectNode.getGroupByList() == null) {
            GroupByList groupByList = (GroupByList) selectNode.
                    getNodeFactory().getNode(C_NodeTypes.GROUP_BY_LIST, selectNode.getContextManager());
            selectNode.setGroupByList(groupByList);
        }

        GroupByColumn groupByColumn = (GroupByColumn) selectNode.
                    getNodeFactory().getNode(
                        C_NodeTypes.GROUP_BY_COLUMN,
                        tmpColumnRef,
                        selectNode.getContextManager());

        groupByColumn.setColumnPosition(rc.getVirtualColumnId());

        selectNode.getGroupByList().addGroupByColumn(groupByColumn);
        return;
    }
}
