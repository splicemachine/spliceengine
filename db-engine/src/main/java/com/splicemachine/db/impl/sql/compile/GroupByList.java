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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;

import java.util.List;

/**
 * A GroupByList represents the list of expressions in a GROUP BY clause in
 * a SELECT statement.
 */

public class GroupByList extends OrderedColumnList{
    int numGroupingColsAdded=0;
    boolean rollup=false;

    /**
     * Add a column to the list
     *
     * @param column The column to add to the list
     */
    public void addGroupByColumn(GroupByColumn column){
        addElement(column);
    }

    /**
     * Get a column from the list
     *
     * @param position The column to get from the list
     */
    public GroupByColumn getGroupByColumn(int position){
        assert position>=0: "position <0";
        assert position < size() : "position>=size()";
        return (GroupByColumn)elementAt(position);
    }


    public void setRollup(){
        rollup=true;
    }

    public boolean isRollup(){
        return rollup;
    }


    /**
     * Bind the group by list.  Verify:
     * o  Number of grouping columns matches number of non-aggregates in
     * SELECT's RCL.
     * o  Names in the group by list are unique
     * o  Names of grouping columns match names of non-aggregate
     * expressions in SELECT's RCL.
     *
     * @param select          The SelectNode
     * @param aggregateVector The aggregate vector being built as we find AggregateNodes
     * @throws StandardException Thrown on error
     */
    public void bindGroupByColumns(SelectNode select,List<AggregateNode> aggregateVector) throws StandardException{
        FromList fromList=select.getFromList();

        SubqueryList dummySubqueryList=
                (SubqueryList)getNodeFactory().getNode(
                        C_NodeTypes.SUBQUERY_LIST,
                        getContextManager());
        int size=size();

		/* Only 32677 columns allowed in GROUP BY clause */
        if(size>Limits.DB2_MAX_ELEMENTS_IN_GROUP_BY){
            throw StandardException.newException(SQLState.LANG_TOO_MANY_ELEMENTS);
        }

		/* Bind the grouping column */
        for(int index=0;index<size;index++){
            GroupByColumn groupByCol=(GroupByColumn)elementAt(index);
            groupByCol.bindExpression(fromList,
                    dummySubqueryList,aggregateVector);
        }

		/* Verify that no subqueries got added to the dummy list */
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(dummySubqueryList.size()==0,
                    "dummySubqueryList.size() is expected to be 0");
        }
    }

    /**
     * Bind the group by list.  Verify:
     * o  Number of grouping columns matches number of non-aggregates in
     * SELECT's RCL.
     * o  Names in the group by list are unique
     * o  Names of grouping columns match names of non-aggregate
     * expressions in SELECT's RCL.
     *
     * @param select          The SelectNode
     * @param aggregateVector The aggregate vector being built as we find AggregateNodes
     * @throws StandardException Thrown on error
     */
    public void bindAndPullGroupByColumns(SelectNode select,List<AggregateNode> aggregateVector) throws StandardException{
        bindGroupByColumns(select, aggregateVector);
        boolean hasDistinct = select.hasDistinct();

        numGroupingColsAdded += pullUpColumns(select, hasDistinct);
    }

    public int pullUpColumns(ResultSetNode target, boolean hasDistinct) throws StandardException {
        ResultColumnList targetRCL = target.getResultColumns();
        int size = size();
        int numColsAddedHere = 0;
        for(int index=0;index<size;index++){
            GroupByColumn groupingCol=(GroupByColumn)elementAt(index);

            /*
            ** Track the number of columns that we have added
            ** in this routine.  We track this separately
            ** than the total number of columns added by this
            ** object (numGroupingColsAdded) because we
            ** might be bound (though not gagged) more than
            ** once (in which case numGroupingColsAdded will
            ** already be set).
            */
            numColsAddedHere += pullUpGroupByColumn(groupingCol, targetRCL, hasDistinct);
        }
        return numColsAddedHere;
    }

    public int pullUpGroupByColumn(GroupByColumn groupingCol, ResultColumnList targetRCL, boolean hasDistinct) throws StandardException {
        boolean matchFound=false;
        int rclSize = targetRCL.size();
        /* Verify that this entry in the GROUP BY list matches a
         * grouping column in the select list.
         */
        for(int inner=0;inner<rclSize;inner++){
            ResultColumn selectListRC=targetRCL.elementAt(inner);
            if(!(selectListRC.getExpression() instanceof ColumnReference)){
                continue;
            }

            ColumnReference selectListCR=(ColumnReference)selectListRC.getExpression();

            if(selectListCR.isEquivalent(groupingCol.getColumnExpression())){
                /* Column positions for grouping columns are 0-based */
                groupingCol.setColumnPosition(inner+1);

                /* Mark the RC in the SELECT list as a grouping column */
                selectListRC.markAsGroupingColumn();
                matchFound=true;
                break;
            }
        }
        /* If no match found in the SELECT list, then add a matching
         * ResultColumn/ColumnReference pair to the SelectNode's RCL.
         * However, don't add additional result columns if the query
         * specified DISTINCT, because distinct processing considers
         * the entire RCL and including extra columns could change the
         * results: e.g. select distinct a,b from t group by a,b,c
         * should not consider column c in distinct processing (DERBY-3613)
         */
        if(!matchFound && !hasDistinct &&
                groupingCol.getColumnExpression() instanceof ColumnReference){
            // only add matching columns for column references not
            // expressions yet. See DERBY-883 for details.
            ResultColumn newRC;

            /* Get a new ResultColumn */
            newRC=(ResultColumn)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    groupingCol.getColumnName(),
                    groupingCol.getColumnExpression().getClone(),
                    getContextManager());
            newRC.setVirtualColumnId(targetRCL.size()+1);
            newRC.markGenerated();
            newRC.markAsGroupingColumn();

            /* Add the new RC/CR to the RCL */
            targetRCL.addElement(newRC);

            /* Set the columnPosition in the GroupByColumn, now that it
            * has a matching entry in the SELECT list.
            */
            groupingCol.setColumnPosition(targetRCL.size());

            // a new hidden or generated column is added to this RCL
            // i.e. that the size() of the RCL != visibleSize().
            // Error checking done later should be aware of this
            // special case.
            targetRCL.setCountMismatchAllowed(true);
        }
        if(groupingCol.getColumnExpression() instanceof JavaToSQLValueNode){
            // disallow any expression which involves native java computation.
            // Not possible to consider java expressions for equivalence.
            throw StandardException.newException(SQLState.LANG_INVALID_GROUPED_SELECT_LIST);
        }
        return 1;
    }


    /**
     * Find the matching grouping column if any for the given expression
     *
     * @param node an expression for which we are trying to find a match
     *             in the group by list.
     * @return the matching GroupByColumn if one exists, null otherwise.
     * @throws StandardException
     */
    public GroupByColumn findGroupingColumn(ValueNode node)
            throws StandardException{
        int sz=size();
        for(int i=0;i<sz;i++){
            GroupByColumn gbc=(GroupByColumn)elementAt(i);
            if(gbc.getColumnExpression().isEquivalent(node)){
                return gbc;
            }
        }
        return null;
    }

    /**
     * Remap all ColumnReferences in this tree to be clones of the
     * underlying expression.
     *
     * @throws StandardException Thrown on error
     */
    public void remapColumnReferencesToExpressions() throws StandardException{
        GroupByColumn gbc;
        int size=size();

		/* This method is called when flattening a FromTable. */
        // gd this is pulling in a patch from Derby 10.11 see JIRA issue DERBY-5313
        for(int index=0;index<size;index++){
            gbc=(GroupByColumn)elementAt(index);

            gbc.setColumnExpression(
                    gbc.getColumnExpression().remapColumnReferencesToExpressions());

        }
    }


    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */
    @Override
    public String toString(){
        if(SanityManager.DEBUG){
            return "numGroupingColsAdded: "+numGroupingColsAdded+"\n"+
                    super.toString();
        }else{
            return "";
        }
    }


    public void preprocess(int numTables,
                           FromList fromList,
                           SubqueryList whereSubquerys,
                           PredicateList wherePredicates) throws StandardException{
        for(int index=0;index<size();index++){
            GroupByColumn groupingCol=(GroupByColumn)elementAt(index);
            groupingCol.setColumnExpression(
                    groupingCol.getColumnExpression().preprocess(numTables,fromList,whereSubquerys,wherePredicates));
        }
    }
}
