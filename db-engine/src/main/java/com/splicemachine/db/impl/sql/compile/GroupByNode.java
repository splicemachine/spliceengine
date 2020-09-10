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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.LanguageFactory;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.AggregateCostController;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.sql.execute.AggregatorInfo;
import com.splicemachine.db.impl.sql.execute.AggregatorInfoList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Types;
import java.util.*;


/**
 * A GroupByNode represents a result set for a grouping operation
 * on a select.  Note that this includes a SELECT with aggregates
 * and no grouping columns (in which case the select list is null)
 * It has the same description as its input result set.
 * <p/>
 * For the most part, it simply delegates operations to its bottomPRSet,
 * which is currently expected to be a ProjectRestrictResultSet generated
 * for a SelectNode.
 * <p/>
 * NOTE: A GroupByNode extends FromTable since it can exist in a FromList.
 * <p/>
 * There is a lot of room for optimizations here: <UL>
 * <LI> agg(distinct x) group by x => agg(x) group by x (for min and max) </LI>
 * <LI> min()/max() use index scans if possible, no sort may
 * be needed. </LI>
 * </UL>
 */
public class GroupByNode extends SingleChildResultSetNode{
    /**
     * The GROUP BY list
     */
    GroupByList groupingList;

    /**
     * The list of all aggregates in the query block
     * that contains this group by.
     */
    List<AggregateNode> aggregateVector;
    /**
     * The parent to the GroupByNode.  If we need to
     * generate a ProjectRestrict over the group by
     * then this is set to that node.  Otherwise it
     * is null.
     */
    FromTable parent;
    /**
     * Information that is used at execution time to
     * process aggregates.
     */
    private AggregatorInfoList aggInfo;
    private boolean addDistinctAggregate;
    private boolean singleInputRowOptimization;
    private int addDistinctAggregateColumnNum;

    // Is the source in sorted order
    private boolean isInSortedOrder;

    private ValueNode havingClause;

    private SubqueryList havingSubquerys;

    private GroupByColumn groupingIdColumn;

    private HashMap<Integer, VirtualColumnNode>groupingIdColumns = new HashMap<>();

    @Override
    public boolean isParallelizable(){
        return true; //is a grouped aggregate
    }

    /**
     * Intializer for a GroupByNode.
     *
     * @param bottomPR        The child FromTable
     * @param groupingList    The groupingList
     * @param aggregateVector The vector of aggregates from
     *                        the query block.  Since aggregation is done
     *                        at the same time as grouping, we need them
     *                        here.
     * @param havingClause    The having clause.
     * @param havingSubquerys subqueries in the having clause.
     * @param tableProperties Properties list associated with the table
     * @param nestingLevel    nestingLevel of this group by node. This is used for
     *                        error checking of group by queries with having clause.
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(
            Object bottomPR,
            Object groupingList,
            Object aggregateVector,
            Object havingClause,
            Object havingSubquerys,
            Object tableProperties,
            Object nestingLevel)
            throws StandardException{
        super.init(bottomPR,tableProperties);
        setLevel((Integer) nestingLevel);
        this.havingClause=(ValueNode)havingClause;
        this.havingSubquerys=(SubqueryList)havingSubquerys;
        /* Group by without aggregates gets xformed into distinct */
        if(SanityManager.DEBUG){
//            Aggregage vector can be null if we have a having clause.
//          select c1 from t1 group by c1 having c1 > 1;
//            SanityManager.ASSERT(((Vector) aggregateVector).size() > 0,
//            "aggregateVector expected to be non-empty");
            if(!(childResult instanceof Optimizable)){
                SanityManager.THROWASSERT("childResult, "+childResult.getClass().getName()+
                        ", expected to be instanceof Optimizable");
            }
            if(!(childResult instanceof FromTable)){
                SanityManager.THROWASSERT("childResult, "+childResult.getClass().getName()+
                        ", expected to be instanceof FromTable");
            }
        }

        ResultColumnList newBottomRCL;
        this.groupingList=(GroupByList)groupingList;
        //noinspection unchecked
        this.aggregateVector=(List<AggregateNode>)aggregateVector;
        this.parent=this;

        /*
        ** The first thing we do is put ourselves on
        ** top of the SELECT.  The select becomes the
        ** childResult.  So our RCL becomes its RCL (so
        ** nodes above it now point to us).  Map our
        ** RCL to its columns.
        */
        newBottomRCL=childResult.getResultColumns().copyListAndObjects();
        resultColumns=childResult.getResultColumns();
        childResult.setResultColumns(newBottomRCL);

        /*
        ** We have aggregates, so we need to add
        ** an extra PRNode and we also have to muck around
        ** with our trees a might.
        */
        addAggregates();

        if(this.groupingList!=null && this.groupingList.isRollup()){
            resultColumns.setNullability(true);
            parent.getResultColumns().setNullability(true);
        }
        /* We say that the source is never in sorted order if there is a distinct aggregate.
         * (Not sure what happens if it is, so just skip it for now.)
         * Otherwise, we check to see if the source is in sorted order on any permutation
         * of the grouping columns.)
         */
        if(!addDistinctAggregate && groupingList!=null){
            //this is safe because this.groupingList = groupingList, it's just properly typed
            @SuppressWarnings("ConstantConditions") ColumnReference[] crs=new ColumnReference[this.groupingList.size()];

            // Now populate the CR array and see if ordered
            int glSize=this.groupingList.size();
            int index;
            for(index=0;index<glSize;index++){
                GroupByColumn gc=(GroupByColumn)this.groupingList.elementAt(index);
                if(gc.getColumnExpression() instanceof ColumnReference){
                    crs[index]=(ColumnReference)gc.getColumnExpression();
                }else{
                    isInSortedOrder=false;
                    break;
                }
            }
            if(index==glSize){
                isInSortedOrder=childResult.isOrderedOn(crs,true,null);
            }
        }
    }

    /**
     * Return the parent node to this one, if there is
     * one.  It will return 'this' if there is no generated
     * node above this one.
     *
     * @return the parent node
     */
    public FromTable getParent(){
        return parent;
    }

    /**
     * @throws StandardException Thrown on error
     * @see Optimizable#optimizeIt
     */
    @Override
    public CostEstimate optimizeIt(Optimizer optimizer,
                                   OptimizablePredicateList predList,
                                   CostEstimate outerCost,
                                   RowOrdering rowOrdering) throws StandardException{
        // RESOLVE: NEED TO FACTOR IN THE COST OF GROUPING (SORTING) HERE
        ((Optimizable)childResult).optimizeIt(
                optimizer,
                predList,
                outerCost,
                rowOrdering);

        return super.optimizeIt(
                optimizer,
                predList,
                outerCost,
                rowOrdering);
    }

    @Override
    public CostEstimate estimateCost(OptimizablePredicateList predList,
                                     ConglomerateDescriptor cd,
                                     CostEstimate outerCost,
                                     Optimizer optimizer,
                                     RowOrdering rowOrdering) throws StandardException{
        AggregateCostController acc=optimizer.newAggregateCostController(groupingList,aggregateVector);
        costEstimate=acc.estimateAggregateCost(outerCost);
        return costEstimate;
    }

    @Override
    public CostEstimate getFinalCostEstimate(boolean useSelf) throws StandardException{
        if(costEstimate==null){
            throw new RuntimeException("Should not be null");
        }
        return costEstimate;
    }

    /**
     * @throws StandardException Thrown on error
     * @see com.splicemachine.db.iapi.sql.compile.Optimizable#pushOptPredicate
     */
    @Override
    public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate) throws StandardException{
        return ((Optimizable)childResult).pushOptPredicate(optimizablePredicate);
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
            return "singleInputRowOptimization: "+singleInputRowOptimization+"\n"+
                    super.toString();
        }else{
            return "";
        }
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */
    @Override
    public void printSubNodes(int depth){
        if(SanityManager.DEBUG){
            super.printSubNodes(depth);

            printLabel(depth,"aggregateVector:\n");

            for(int i=0;i<aggregateVector.size();i++){
                AggregateNode agg=aggregateVector.get(i);
                debugPrint(formatNodeString("["+i+"]:",depth+1));
                agg.treePrint(depth+1);
            }

            if(groupingList!=null){
                printLabel(depth,"groupingList: ");
                groupingList.treePrint(depth+1);
            }

            if (groupingIdColumn != null) {
                printLabel(depth, "groupingId: ");
                groupingIdColumn.treePrint(depth+1);
            }

            if(havingClause!=null){
                printLabel(depth,"havingClause: ");
                havingClause.treePrint(depth+1);
            }

            if(havingSubquerys!=null){
                printLabel(depth,"havingSubqueries: ");
                havingSubquerys.treePrint(depth+1);
            }
        }
    }

    /**
     * Evaluate whether or not the subquery in a FromSubquery is flattenable.
     * Currently, a FSqry is flattenable if all of the following are true:
     * o  Subquery is a SelectNode.
     * o  It contains no top level subqueries.  (RESOLVE - we can relax this)
     * o  It does not contain a group by or having clause
     * o  It does not contain aggregates.
     *
     * @param fromList The outer from list
     * @return boolean    Whether or not the FromSubquery is flattenable.
     */
    @Override
    public boolean flattenableInFromSubquery(FromList fromList){
        /* Can't flatten a GroupByNode */
        return false;
    }

    /**
     * Optimize this GroupByNode.
     *
     * @param dataDictionary The DataDictionary to use for optimization
     * @param predicates     The PredicateList to optimize.  This should
     *                       be a join predicate.
     * @param outerRows      The number of outer joining rows
     * @param forSpark
     * @return ResultSetNode    The top of the optimized subtree
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultSetNode optimize(DataDictionary dataDictionary,
                                  PredicateList predicates,
                                  double outerRows,
                                  boolean forSpark) throws StandardException{
        /* We need to implement this method since a PRN can appear above a
         * SelectNode in a query tree.
         */
        childResult=childResult.optimize(dataDictionary, predicates, outerRows, forSpark);
        Optimizer optimizer=getOptimizer(
                (FromList)getNodeFactory().getNode(
                        C_NodeTypes.FROM_LIST,
                        getNodeFactory().doJoinOrderOptimization(),
                        getContextManager()),
                predicates,
                dataDictionary,
                null);
        optimizer.setForSpark(forSpark);

        // RESOLVE: NEED TO FACTOR IN COST OF SORTING AND FIGURE OUT HOW
        // MANY ROWS HAVE BEEN ELIMINATED.
        costEstimate=optimizer.newCostEstimate();

        costEstimate.setCost(childResult.getCostEstimate().getEstimatedCost(),
                childResult.getCostEstimate().rowCount(),
                childResult.getCostEstimate().singleScanRowCount());

        return this;
    }

    @Override
    public ResultColumnDescriptor[] makeResultDescriptors(){
        return childResult.makeResultDescriptors();
    }

    /**
     * Return whether or not the underlying ResultSet tree will return
     * a single row, at most.
     * This is important for join nodes where we can save the extra next
     * on the right side if we know that it will return at most 1 row.
     *
     * @return Whether or not the underlying ResultSet tree will return a single row.
     * @throws StandardException Thrown on error
     */
    @Override
    public boolean isOneRowResultSet() throws StandardException{
        // Only consider scalar aggregates for now
        return ((groupingList==null) || (groupingList.isEmpty()));
    }

    /**
     * generate the sort result set operating over the source
     * resultset.  Adds distinct aggregates to the sort if
     * necessary.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException{
        int orderingItem;
        int aggInfoItem;
        FormatableArrayHolder orderingHolder;

        /* Get the next ResultSet#, so we can number this ResultSetNode, its
         * ResultColumnList and ResultSet.
         */
        assignResultSetNumber();

        // Get the final cost estimate from the child.
        childResult.getFinalCostEstimate(true);

        /*
        ** Get the column ordering for the sort.  Note that
        ** for a scalar aggegate we may not have any ordering
        ** columns (if there are no distinct aggregates).
        ** WARNING: if a distinct aggregate is passed to
        ** SortResultSet it assumes that the last column
        ** is the distinct one.  If this assumption changes
        ** then SortResultSet will have to change.
        */
        orderingHolder=acb.getColumnOrdering(groupingList);
        if(addDistinctAggregate){
            orderingHolder=acb.addColumnToOrdering(
                    orderingHolder,
                    addDistinctAggregateColumnNum);
        }

        if(SanityManager.DEBUG){
            if(SanityManager.DEBUG_ON("AggregateTrace")){
                StringBuilder s=new StringBuilder();

                s.append("Group by column ordering is (");
                ColumnOrdering[] ordering=(ColumnOrdering[])orderingHolder.getArray(ColumnOrdering.class);

                for(ColumnOrdering colOrder : ordering){
                    s.append(colOrder.getColumnId());
                    s.append(" ");
                }
                s.append(")");
                SanityManager.DEBUG("AggregateTrace",s.toString());
            }
        }

        orderingItem=acb.addItem(orderingHolder);

        /*
        ** We have aggregates, so save the aggInfo
        ** struct in the activation and store the number
        */
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(aggInfo!=null,
                    "aggInfo not set up as expected");
        }
        aggInfoItem=acb.addItem(aggInfo);

        acb.pushGetResultSetFactoryExpression(mb);

        // Generate the child ResultSet
        childResult.generate(acb,mb);
        mb.push(isInSortedOrder);
        mb.push(aggInfoItem);
        mb.push(orderingItem);

        resultColumns.generateHolder(acb,mb);

        mb.push(resultColumns.getTotalColumnSize());
        mb.push(resultSetNumber);

        /* Generate a (Distinct)ScalarAggregateResultSet if scalar aggregates */
        if((groupingList==null) || (groupingList.isEmpty())){
            genScalarAggregateResultSet(mb);
        }
        /* Generate a (Distinct)GroupedAggregateResultSet if grouped aggregates */
        else{
            genGroupedAggregateResultSet(acb, mb);
        }
    }

    /**
     * Get whether or not the source is in sorted order.
     *
     * @return Whether or not the source is in sorted order.
     */
    boolean getIsInSortedOrder(){
        return isInSortedOrder;
    }

    /**
     * Consider any optimizations after the optimizer has chosen a plan.
     * Optimizations include:
     * o  min optimization for scalar aggregates
     * o  max optimization for scalar aggregates
     *
     * @param selectHasPredicates true if SELECT containing this
     *                            vector/scalar aggregate has a restriction
     * @throws StandardException on error
     */
    void considerPostOptimizeOptimizations(boolean selectHasPredicates) throws StandardException{
        /* Consider the optimization for min with asc index on that column or
         * max with desc index on that column:
         *    o  No group by
         *  o  One of:
         *        o  min/max(ColumnReference) is only aggregate && source is
         *           ordered on the ColumnReference
         *        o  min/max(ConstantNode)
         * The optimization of the other way around (min with desc index or
         * max with asc index) has the same restrictions with the additional
         * temporary restriction of no qualifications at all (because
         * we don't have true backward scans).
         */
        if(groupingList==null){
            if(aggregateVector.size()==1){
                AggregateNode an=aggregateVector.get(0);
                AggregateDefinition ad=an.getAggregateDefinition();
                if(ad instanceof MaxMinAggregateDefinition){
                    if(an.getOperand() instanceof ColumnReference){
                        /* See if the underlying ResultSet tree
                         * is ordered on the ColumnReference.
                         */
                        ColumnReference[] crs=new ColumnReference[1];
                        crs[0]=(ColumnReference)an.getOperand();

                        Vector tableVector=new Vector();
                        boolean minMaxOptimizationPossible=isOrderedOn(crs,false,tableVector);
                        if(SanityManager.DEBUG){
                            SanityManager.ASSERT(tableVector.size()<=1,"bad number of FromBaseTables returned by isOrderedOn() -- "+tableVector.size());
                        }

                        if(minMaxOptimizationPossible){
                            boolean ascIndex=true;
                            int colNum=crs[0].getColumnNumber();

                            /* Check if we have an access path, this will be
                             * null in a join case (See Beetle 4423,DERBY-3904)
                             */
                            AccessPath accessPath=getTrulyTheBestAccessPath();
                            if(accessPath==null || accessPath.getConglomerateDescriptor()==null ||
                                    accessPath.getConglomerateDescriptor().getIndexDescriptor()==null)
                                return;
                            IndexDescriptor id=accessPath.getConglomerateDescriptor().getIndexDescriptor();
                            int[] keyColumns=id.baseColumnPositions();
                            boolean[] isAscending=id.isAscending();
                            for(int i=0;i<keyColumns.length;i++){
                                /* in such a query: select min(c3) from
                                 * tab1 where c1 = 2 and c2 = 5, if prefix keys
                                 * have equality operator, then we can still use
                                 * the index.  The checking of equality operator
                                 * has been done in isStrictlyOrderedOn.
                                 */
                                if(colNum==keyColumns[i]){
                                    if(!isAscending[i])
                                        ascIndex=false;
                                    break;
                                }
                            }
                            FromBaseTable fbt=(FromBaseTable)tableVector.firstElement();
                            MaxMinAggregateDefinition temp=(MaxMinAggregateDefinition)ad;

                            /*  MAX   ASC      NULLABLE
                             *  ----  ----------
                             *  TRUE  TRUE      TRUE/FALSE  =  Special Last Key Scan (ASC Index Last key with null skips)
                             *  TRUE  FALSE     TRUE/FALSE  =  JustDisableBulk(DESC index 1st key with null skips)
                             *  FALSE TRUE      TRUE/FALSE  = JustDisableBulk(ASC index 1st key)
                             *  FALSE FALSE     TRUE/FALSE  = Special Last Key Scan(Desc Index Last Key)
                             */

                            if(((!temp.isMax()) && ascIndex) ||
                                    ((temp.isMax()) && !ascIndex)){
                                fbt.disableBulkFetch();
                                singleInputRowOptimization=true;
                            }
                            /*
                            ** Max optimization with asc index or min with
                            ** desc index is currently more
                            ** restrictive than otherwise.
                            ** We are getting the store to return the last
                            ** row from an index (for the time being, the
                            ** store cannot do real backward scans).  SO
                            ** we cannot do this optimization if we have
                            ** any predicates at all.
                            */
                            else if(!selectHasPredicates){
                                // we have make the choice during costing whether to pick SpecialMaxScan
                                if (accessPath.getSpecialMaxScan()) {
                                    fbt.disableBulkFetch();
                                    fbt.doSpecialMaxScan();
                                    singleInputRowOptimization = true;
                                }
                            }
                        }
                    }else if(an.getOperand() instanceof ConstantNode){
                        singleInputRowOptimization=true;
                    }
                }
            }
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    /**
     * Add the extra result columns required by the aggregates
     * to the result list.
     *
     * @throws StandardException
     */
    private void addAggregates() throws StandardException{
        addNewPRNode();
        addNewColumnsForAggregation();
        addDistinctAggregatesToOrderBy();
    }

    /**
     * Add any distinct aggregates to the order by list.
     * Asserts that there are 0 or more distincts.
     */
    private void addDistinctAggregatesToOrderBy(){
        int numDistinct=numDistinctAggregates(aggregateVector);
        if(numDistinct!=0){
            AggregatorInfo agg=null;
            for(AggregatorInfo info : aggInfo){
                agg=info;
                if(agg.isDistinct()){
                    break;
                }
            }

            assert agg!=null && agg.isDistinct();

            addDistinctAggregate = true;
            addDistinctAggregateColumnNum = agg.getInputColNum();
        }
    }

    /**
     * Add a new PR node for aggregation.  Put the
     * new PR under the sort.
     *
     * @throws StandardException
     */
    private void addNewPRNode() throws StandardException{
        /*
        ** Get the new PR, put above the GroupBy.
        */
        ResultColumnList rclNew=(ResultColumnList)getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN_LIST,
                getContextManager());
        int sz=resultColumns.size();
        for(int i=0;i<sz;i++){
            ResultColumn rc=resultColumns.elementAt(i);
            if(!rc.isGenerated()){
                rclNew.addElement(rc);
            }
        }

        // if any columns in the source RCL were generated for an order by
        // remember it in the new RCL as well. After the sort is done it will
        // have to be projected out upstream.
        rclNew.copyOrderBySelect(resultColumns);

        parent=(FromTable)getNodeFactory().getNode(
                C_NodeTypes.PROJECT_RESTRICT_NODE,
                this,    // child
                rclNew,
                null, //havingClause,
                null,                // restriction list
                null,                // project subqueries
                havingSubquerys,
                tableProperties,
                getContextManager());


        /*
        ** Reset the bottom RCL to be empty.
        */
        childResult.setResultColumns((ResultColumnList)
                getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN_LIST,getContextManager()));

        /*
        ** Set the group by RCL to be empty
        */
        resultColumns=(ResultColumnList)getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN_LIST,getContextManager());

    }

    /**
     * In the query rewrite for group by, add the columns on which we are doing
     * the group by.
     *
     * @return havingRefsToSubstitute visitors array. Return any
     * havingRefsToSubstitute visitors since it is too early to apply
     * them yet; we need the AggregateNodes unmodified until after
     * we add the new columns for aggregation (DERBY-4071).
     * @see #addNewColumnsForAggregation
     */
    @SuppressWarnings("Convert2Diamond")
    private List<SubstituteExpressionVisitor> addUnAggColumns(List<SubstituteExpressionVisitor> referencesToSubstitute) throws StandardException{
        ResultColumnList bottomRCL=childResult.getResultColumns();
        ResultColumnList groupByRCL=resultColumns;

        List<SubstituteExpressionVisitor> havingRefsToSubstitute=null;
        if(havingClause!=null)
            havingRefsToSubstitute=new ArrayList<SubstituteExpressionVisitor>();
        int sz=groupingList.size();
        for(int i=0;i<sz;i++){
            GroupByColumn gbc=(GroupByColumn)groupingList.elementAt(i);
            ResultColumn newRC=(ResultColumn)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    "##UnaggColumn"+gbc.getColumnName(),
                    gbc.getColumnExpression(),
                    getContextManager());

            // add this result column to the bottom rcl
            bottomRCL.addElement(newRC);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            newRC.setVirtualColumnId(bottomRCL.size());

            // now add this column to the groupbylist
            ResultColumn gbRC=(ResultColumn)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    "##UnaggColumn"+gbc.getColumnName(),
                    gbc.getColumnExpression(),
                    getContextManager());
            groupByRCL.addElement(gbRC);
            gbRC.markGenerated();
            gbRC.bindResultColumnToExpression();
            gbRC.setVirtualColumnId(groupByRCL.size());

            /*
             ** Reset the original node to point to the
             ** Group By result set.
             */
            //noinspection UnnecessaryBoxing
            VirtualColumnNode vc=(VirtualColumnNode)getNodeFactory().getNode(
                    C_NodeTypes.VIRTUAL_COLUMN_NODE,
                    this, // source result set.
                    gbRC,
                    Integer.valueOf(groupByRCL.size()), //sometimes the boxing is needed to make it compile
                    getContextManager());

            // we replace each group by expression
            // in the projection list with a virtual column node
            // that effectively points to a result column
            // in the result set doing the group by
            //
            // Note that we don't perform the replacements
            // immediately, but instead we accumulate them
            // until the end of the loop. This allows us to
            // sort the expressions and process them in
            // descending order of complexity, necessary
            // because a compound expression may contain a
            // reference to a simple grouped column, but in
            // such a case we want to process the expression
            // as an expression, not as individual column
            // references. E.g., if the statement was:
            //   SELECT ... GROUP BY C1, C1 * (C2 / 100), C3
            // then we don't want the replacement of the
            // simple column reference C1 to affect the
            // compound expression C1 * (C2 / 100). DERBY-3094.
            //
            ValueNode vn=gbc.getColumnExpression();
            SubstituteExpressionVisitor vis=new SubstituteExpressionVisitor(vn,vc,AggregateNode.class);
            referencesToSubstitute.add(vis);

            // Since we always need a PR node on top of the GB
            // node to perform projection we can use it to perform
            // the having clause restriction as well.
            // To evaluate the having clause correctly, we need to
            // convert each aggregate and expression to point
            // to the appropriate result column in the group by node.
            // This is no different from the transformations we do to
            // correctly evaluate aggregates and expressions in the
            // projection list.
            //
            //
            // For this query:
            // SELECT c1, SUM(c2), MAX(c3)
            //    FROM t1
            //    HAVING c1+max(c3) > 0;

            // PRSN RCL -> (ptr(gbn:rcl[0]), ptr(gbn:rcl[1]), ptr(gbn:rcl[4]))
            // Restriction: (> (+ ptr(gbn:rcl[0]) ptr(gbn:rcl[4])) 0)
            //              |
            // GBN (RCL) -> (C1, SUM(C2), <input>, <aggregator>, MAX(C3), <input>, <aggregator>
            //              |
            //       FBT (C1, C2)
            if(havingClause!=null){
                SubstituteExpressionVisitor havingSE=new SubstituteExpressionVisitor(vn,vc,null);
                //this is safe because we construct the list under the same if clause
                //noinspection ConstantConditions
                havingRefsToSubstitute.add(havingSE);
            }
            gbc.setColumnPosition(bottomRCL.size());
        }
        Comparator<SubstituteExpressionVisitor> sorter=new ExpressionSorter();
        Collections.sort(referencesToSubstitute,sorter);

        if(havingRefsToSubstitute!=null){
            Collections.sort(havingRefsToSubstitute,sorter);
            // DERBY-4071 Don't substitute quite yet; we need the AggrateNodes
            // undisturbed until after we have had the chance to build the
            // other columns.  (The AggrateNodes are shared via an alias from
            // aggregateVector and from the expression tree under
            // havingClause).
        }
        return havingRefsToSubstitute;
    }
    /**
     * Get the null aggregate result expression
     * column.
     *
     * @return the value node
     *
     * @exception StandardException on error
     */
    public ValueNode    getNewNullResultExpressionForGroupingID()
            throws StandardException
    {

        DataTypeDescriptor type = null;
        try {
                type =  new DataTypeDescriptor( TypeId.getBuiltInTypeId( Types.TINYINT), true);
                return getNullNode(type);
        }
        catch (StandardException e) {
            if (e.getSqlState().compareTo(SQLState.LANG_NONULL_DATATYPE) == 0) {
               throw StandardException.newException(SQLState.LANG_INVALID_AGGREGATION_DATATYPE,
                                                      type.getTypeId().getSQLTypeName());
             }
             else throw e;
        }
    }

    private void addGroupingIdColumnsForNativeSpark() throws StandardException {
        ResultColumnList bottomRCL = childResult.getResultColumns();
        ResultColumnList groupByRCL = resultColumns;

        int numElements = groupByRCL.size()-1;
        for (int i = 0; i < numElements; i++) {
            String colName = String.format("##UnaggColumn-GroupingId-Col%d", i);
            GroupByColumn gbn = (GroupByColumn)getNodeFactory().getNode(
                C_NodeTypes.GROUP_BY_COLUMN,
                getNewNullResultExpressionForGroupingID(),
                getContextManager());

            ResultColumn newRC=(ResultColumn)getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                colName,
                gbn.getColumnExpression(),
                getContextManager());

            bottomRCL.addElement(newRC);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            newRC.setVirtualColumnId(bottomRCL.size());

            // add result column to the groupbylist
            ResultColumn gbRC=(ResultColumn)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    colName,
                    gbn.getColumnExpression(),
                    getContextManager());
            groupByRCL.addElement(gbRC);
            gbRC.markGenerated();
            gbRC.bindResultColumnToExpression();
            gbRC.setVirtualColumnId(groupByRCL.size());

            gbn.setColumnPosition(bottomRCL.size());

            VirtualColumnNode vc=(VirtualColumnNode)getNodeFactory().getNode(
                C_NodeTypes.VIRTUAL_COLUMN_NODE,
                this,
                gbRC,
                groupByRCL.size(),
                getContextManager());

            groupingIdColumns.put(numElements - i - 1, vc);
        }
    }
    private void addGroupingIdColumnAndUpdateGroupingFunction() throws StandardException {
        ResultColumnList bottomRCL=childResult.getResultColumns();
        ResultColumnList groupByRCL=resultColumns;

        // step 1: add result column to the bottom rcl

        // define groupingId as SQLBit type, where each bit represent one grouping column
        // in the order the grouping columns appear in the groupByList
        DataTypeDescriptor bitType = new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.BINARY), true);
          //      DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BINARY, true);

        groupingIdColumn = (GroupByColumn)getNodeFactory().getNode(
                C_NodeTypes.GROUP_BY_COLUMN,
                getNullNode(bitType),
                getContextManager());

        ResultColumn newRC=(ResultColumn)getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##UnaggColumn-GroupingId",
                groupingIdColumn.getColumnExpression(),
                getContextManager());

        bottomRCL.addElement(newRC);
        newRC.markGenerated();
        newRC.bindResultColumnToExpression();
        newRC.setVirtualColumnId(bottomRCL.size());

        // add result column to the groupbylist
        ResultColumn gbRC=(ResultColumn)getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##UnaggColumn-GroupingId",
                groupingIdColumn.getColumnExpression(),
                getContextManager());
        groupByRCL.addElement(gbRC);
        gbRC.markGenerated();
        gbRC.bindResultColumnToExpression();
        gbRC.setVirtualColumnId(groupByRCL.size());

        groupingIdColumn.setColumnPosition(bottomRCL.size());

        VirtualColumnNode vc=(VirtualColumnNode)getNodeFactory().getNode(
                C_NodeTypes.VIRTUAL_COLUMN_NODE,
                this, // source result set.
                gbRC,
                groupByRCL.size(), //sometimes the boxing is needed to make it compile
                getContextManager());

        addGroupingIdColumnsForNativeSpark();
        updateGroupingFunctions(vc);

        return;
    }

    private void updateGroupingFunctions(VirtualColumnNode vc) throws StandardException {
            //update the GROUPING function in the parent result column if any
            UpdateGroupingFunctionVisitor updateGroupingFunctionVisitor = new UpdateGroupingFunctionVisitor(vc, groupingList,
                                                                                       groupingIdColumns, null);
            parent.getResultColumns().accept(updateGroupingFunctionVisitor);
            if (havingClause != null)
                havingClause.accept(updateGroupingFunctionVisitor);
    }

    /**
     * Add a whole slew of columns needed for
     * aggregation. Basically, for each aggregate we add
     * 3 columns: the aggregate input expression
     * and the aggregator column and a column where the aggregate
     * result is stored.  The input expression is
     * taken directly from the aggregator node.  The aggregator
     * is the run time aggregator.  We add it to the RC list
     * as a new object coming into the sort node.
     * <p/>
     * At this point this is invoked, we have the following
     * tree: <UL>
     * PR - (PARENT): RCL is the original select list
     * |
     * PR - GROUP BY:  RCL is empty
     * |
     * PR - FROM TABLE: RCL is empty </UL> <P>
     * <p/>
     * For each ColumnReference in PR RCL <UL>
     * <LI> clone the ref </LI>
     * <LI> create a new RC in the bottom RCL and set it
     * to the col ref </LI>
     * <LI> create a new RC in the GROUPBY RCL and set it to
     * point to the bottom RC </LI>
     * <LI> reset the top PR ref to point to the new GROUPBY
     * RC</LI></UL>
     * <p/>
     * For each aggregate in aggregateVector <UL>
     * <LI> create RC in FROM TABLE.  Fill it with
     * aggs Operator.
     * <LI> create RC in FROM TABLE for agg result</LI>
     * <LI> create RC in FROM TABLE for aggregator</LI>
     * <LI> create RC in GROUPBY for agg input, set it
     * to point to FROM TABLE RC </LI>
     * <LI> create RC in GROUPBY for agg result</LI>
     * <LI> create RC in GROUPBY for aggregator</LI>
     * <LI> replace Agg with reference to RC for agg result </LI></UL>.
     * <p/>
     * For a query like,
     * <pre>
     * select c1, sum(c2), max(c3)
     * from t1
     * group by c1;
     * </pre>
     * the query tree ends up looking like this:
     * <pre>
     * ProjectRestrictNode RCL -> (ptr to GBN(column[0]), ptr to GBN(column[1]), ptr to GBN(column[4]))
     * |
     * GroupByNode RCL->(C1, SUM(C2), <agg-input>, <aggregator>, MAX(C3), <agg-input>, <aggregator>)
     * |
     * ProjectRestrict RCL->(C1, C2, C3)
     * |
     * FromBaseTable
     * </pre>
     * <p/>
     * The RCL of the GroupByNode contains all the unagg (or grouping columns)
     * followed by 3 RC's for each aggregate in this order: the final computed
     * aggregate value, the aggregate input and the aggregator function.
     * <p/>
     * The Aggregator function puts the results in the first of the 3 RC's
     * and the PR resultset in turn picks up the value from there.
     * <p/>
     * The notation (ptr to GBN(column[0])) basically means that it is
     * a pointer to the 0th RC in the RCL of the GroupByNode.
     * <p/>
     * The addition of these unagg and agg columns to the GroupByNode and
     * to the PRN is performed in addUnAggColumns and addAggregateColumns.
     * <p/>
     * Note that that addition of the GroupByNode is done after the
     * query is optimized (in SelectNode#modifyAccessPaths) which means a
     * fair amount of patching up is needed to account for generated group by columns.
     *
     * @throws StandardException
     */
    private void addNewColumnsForAggregation() throws StandardException{
        aggInfo=new AggregatorInfoList();
        List<SubstituteExpressionVisitor> havingRefsToSubstitute=null;

        if(groupingList!=null){
            List<SubstituteExpressionVisitor> referencesToSubstitute=new ArrayList<SubstituteExpressionVisitor>();
            havingRefsToSubstitute=addUnAggColumns(referencesToSubstitute);

            /* add a grouping_id field as an extra grouping column for rollup.
               this field is useful for two purpose:
               1. It can differentiate the rows where the group by column has null value
                  vs. a rollup row where we set a group by column's value to null
               2. It provide essential information to support GROUPING() function
            */
            if (groupingList.isRollup()) {
                addGroupingIdColumnAndUpdateGroupingFunction();
            }

            // let the column references in the parent's result columns point to the aggregate result's result columns
            for(SubstituteExpressionVisitor refVisistor : referencesToSubstitute)
                parent.getResultColumns().accept(refVisistor);
        }

        addAggregateColumns();

        if(havingClause!=null){

            // Now do the substitution of the group by expressions in the
            // having clause.
            if(havingRefsToSubstitute!=null){
                for(SubstituteExpressionVisitor refVisitor : havingRefsToSubstitute){
                    havingClause.accept(refVisitor);
                }
            }

            // we have replaced group by expressions in the having clause.
            // there should be no column references in the having clause
            // referencing this table. Skip over aggregate nodes.
            //   select a, sum(b) from t group by a having a+c > 1
            //  is not valid because of column c.
            //
            // it is allright to have columns from parent or child subqueries;
            //   select * from p where p.p1 in
            //      (select c.c1 from c group by c.c1 having count(*) = p.p2
            CollectNodesVisitor collectNodesVisitor=new CollectNodesVisitor(ColumnReference.class,AggregateNode.class);
            havingClause.accept(collectNodesVisitor);

            for(Object o : collectNodesVisitor.getList()){
                ColumnReference cr=(ColumnReference)o;

                if(!(cr.getGeneratedToReplaceAggregate() ||
                        cr.getGeneratedToReplaceWindowFunctionCall()) &&
                        cr.getSourceLevel()==level){
                    throw StandardException.newException(
                            SQLState.LANG_INVALID_COL_HAVING_CLAUSE,
                            cr.getSQLColumnName());
                }
            }
        }

    }

    /**
     * In the query rewrite involving aggregates, add the columns for
     * aggregation.
     *
     * @see #addNewColumnsForAggregation
     */
    private void addAggregateColumns() throws StandardException{
        DataDictionary dd=getDataDictionary();
        AggregateNode aggregate;
        ColumnReference newColumnRef;
        ResultColumn newRC;
        ResultColumn tmpRC;
        ResultColumn aggResultRC;
        ResultColumnList bottomRCL=childResult.getResultColumns();
        ResultColumnList groupByRCL=resultColumns;
        ResultColumnList aggRCL;
        int aggregatorVColId;
        int aggInputVColId;
        int aggResultVColId;

        /*
         ** Now process all of the aggregates.  Replace
         ** every aggregate with an RC.  We toss out
         ** the list of RCs, we need to get each RC
         ** as we process its corresponding aggregate.
         */
        LanguageFactory lf=getLanguageConnectionContext().getLanguageFactory();

        ReplaceAggregatesWithCRVisitor replaceAggsVisitor=
                new ReplaceAggregatesWithCRVisitor((ResultColumnList)getNodeFactory().getNode(
                        C_NodeTypes.RESULT_COLUMN_LIST,
                        getContextManager()),
                        ((FromTable)childResult).getTableNumber(),
                        ResultSetNode.class);
        parent.getResultColumns().accept(replaceAggsVisitor);


        if(havingClause!=null){
            // replace aggregates in the having clause with column references.
            replaceAggsVisitor=new ReplaceAggregatesWithCRVisitor((ResultColumnList)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN_LIST,
                    getContextManager()),
                    ((FromTable)childResult).getTableNumber());
            havingClause.accept(replaceAggsVisitor);
            // make having clause a restriction list in the parent
            // project restrict node.
            ProjectRestrictNode parentPRSN=(ProjectRestrictNode)parent;
            parentPRSN.setRestriction(havingClause);
        }

        /*
        ** For each aggregate
        */
        //noinspection Convert2Diamond
        for(AggregateNode aggNode : aggregateVector){
            aggregate=aggNode;

            /*
            ** AGG RESULT: Set the aggregate result to null in the
            ** bottom project restrict.
            */
            newRC=(ResultColumn)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    "##"+aggregate.getAggregateName()+"Result",
                    aggregate.getNewNullResultExpression(),
                    getContextManager());
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            aggResultVColId=newRC.getVirtualColumnId();

            /*
            ** Set the GB aggregrate result column to
            ** point to this.  The GB aggregate result
            ** was created when we called
            ** ReplaceAggregatesWithCRVisitor()
            */
            newColumnRef=(ColumnReference)getNodeFactory().getNode(
                    C_NodeTypes.COLUMN_REFERENCE,
                    newRC.getName(),
                    null,
                    getContextManager());
            newColumnRef.setSource(newRC);
            newColumnRef.setNestingLevel(this.getLevel());
            newColumnRef.setSourceLevel(this.getLevel());
            tmpRC=(ResultColumn)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    newColumnRef.getColumnName(),
                    newColumnRef,
                    getContextManager());
            tmpRC.markGenerated();
            tmpRC.bindResultColumnToExpression();
            groupByRCL.addElement(tmpRC);
            tmpRC.setVirtualColumnId(groupByRCL.size());

            /*
            ** Set the column reference to point to
            ** this.
            */
            newColumnRef=aggregate.getGeneratedRef();
            newColumnRef.setSource(tmpRC);

            /*
            ** AGG INPUT: Create a ResultColumn in the bottom
            ** project restrict that has the expression that is
            ** to be aggregated
            */
            newRC=aggregate.getNewExpressionResultColumn(dd);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            newRC.setName("##"+aggregate.getAggregateName()+"Input");
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            aggInputVColId=newRC.getVirtualColumnId();
            aggResultRC=(ResultColumn)getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN,
                    "##aggregate expression",
                    aggregate.getNewNullResultExpression(),
                    getContextManager());


            /*
            ** Add a reference to this column into the
            ** group by columns.
            */
            tmpRC=getColumnReference(newRC);
            groupByRCL.addElement(tmpRC);
            tmpRC.setVirtualColumnId(groupByRCL.size());

            /*
            ** AGGREGATOR: Add a getAggregator method call
            ** to the bottom result column list.
            */
            newRC=aggregate.getNewAggregatorResultColumn(dd);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            newRC.setName("##"+aggregate.getAggregateName()+"Function");
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            aggregatorVColId=newRC.getVirtualColumnId();

            /*
            ** Add a reference to this column in the Group By result
            ** set.
            */
            tmpRC=getColumnReference(newRC);
            groupByRCL.addElement(tmpRC);
            tmpRC.setVirtualColumnId(groupByRCL.size());

            /*
            ** Piece together a fake one column rcl that we will use
            ** to generate a proper result description for input
            ** to this agg if it is a user agg.
            */
            aggRCL=(ResultColumnList)getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN_LIST,getContextManager());
            aggRCL.addElement(aggResultRC);

            DataValueDescriptor parameter = aggregate instanceof StringAggregateNode ?
                    ((StringAggregateNode)aggregate).getParameter() : null;
            /*
            ** Note that the column ids in the row are 0 based
            ** so we have to subtract 1.
            */
            aggInfo.add(new AggregatorInfo(
                    aggregate.getAggregateName(),
                    aggregate.getAggregatorClassName(),
                    aggInputVColId-1,            // aggregate input column
                    aggResultVColId-1,            // the aggregate result column
                    aggregatorVColId-1,        // the aggregator column
                    aggregate.isDistinct(),
                    lf.getResultDescription(aggRCL.makeResultDescriptors(),"SELECT"),
                    parameter
            ));
        }
    }

    /**
     * Generate the code to evaluate scalar aggregates.
     */
    private void genScalarAggregateResultSet(MethodBuilder mb) throws StandardException {
        /* Generate the (Distinct)ScalarAggregateResultSet:
         *    arg1: childExpress - Expression for childResult
         *  arg2: isInSortedOrder - true if source result set in sorted order
         *  arg3: aggregateItem - entry in saved objects for the aggregates,
         *  arg4: orderItem - entry in saved objects for the ordering
         *  arg5: Activation
         *  arg6: rowAllocator - method to construct rows for fetching
         *            from the sort
         *  arg7: row size
         *  arg8: resultSetNumber
         *  arg9: Whether or not to perform min optimization.
         */
        String resultSet=(addDistinctAggregate)?"getDistinctScalarAggregateResultSet":"getScalarAggregateResultSet";

        mb.push(singleInputRowOptimization);
        mb.push(costEstimate.rowCount());
        mb.push(costEstimate.getEstimatedCost());
        mb.push(this.printExplainInformationForActivation());
        CompilerContext.NativeSparkModeType nativeSparkMode;
        nativeSparkMode = getCompilerContext().getNativeSparkAggregationMode();
        mb.push(nativeSparkMode.ordinal());
        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,resultSet,
                ClassName.NoPutResultSet,12);
    }

    ///////////////////////////////////////////////////////////////
    //
    // UTILITIES
    //
    ///////////////////////////////////////////////////////////////

    /**
     * Generate the code to evaluate grouped aggregates.
     */
    private void genGroupedAggregateResultSet(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException{
        /* Generate the (Distinct)GroupedAggregateResultSet:
         *    arg1: childExpress - Expression for childResult
         *  arg2: isInSortedOrder - true if source result set in sorted order
         *  arg3: aggregateItem - entry in saved objects for the aggregates,
         *  arg4: orderItem - entry in saved objects for the ordering
         *  arg5: Activation
         *  arg6: rowAllocator - method to construct rows for fetching
         *            from the sort
         *  arg7: row size
         *  arg8: resultSetNumber
         *  arg9: isRollup
         */
        String resultSet=(addDistinctAggregate)?"getDistinctGroupedAggregateResultSet":"getGroupedAggregateResultSet";

        mb.push(costEstimate.rowCount());
        mb.push(costEstimate.getEstimatedCost());
        mb.push(groupingList.isRollup());

        // pass down groupingId column position and the groupingId value for rollup rows
        if (groupingList.isRollup()) {
            mb.push(groupingIdColumn.getColumnPosition()-1);
            FormatableBitSet[] groupingIds = new FormatableBitSet[groupingList.size()];
            FormatableBitSet base = new FormatableBitSet(groupingList.size());
            for (int i=0; i< groupingList.size(); i++) {
                base.set(i);
                groupingIds[i] = (FormatableBitSet)base.clone();
            }
            FormatableArrayHolder groupingIdArrayHolder = new FormatableArrayHolder(groupingIds);
            int groupIdArrayItem = acb.addItem(groupingIdArrayHolder);
            mb.push(groupIdArrayItem);
        } else {
            mb.push(-1);
            mb.push(-1);
        }

        mb.push(printExplainInformationForActivation());

        CompilerContext.NativeSparkModeType nativeSparkMode;
        nativeSparkMode = getCompilerContext().getNativeSparkAggregationMode();
        mb.push(nativeSparkMode.ordinal());
        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,resultSet,
                ClassName.NoPutResultSet, 14);
    }

    /**
     * Method for creating a new result column referencing
     * the one passed in.
     *
     * @param targetRC the source
     * @return the new result column
     * @throws StandardException on error
     */
    private ResultColumn getColumnReference(ResultColumn targetRC) throws StandardException{
        ColumnReference tmpColumnRef;
        ResultColumn newRC;

        tmpColumnRef=(ColumnReference)getNodeFactory().getNode(
                C_NodeTypes.COLUMN_REFERENCE,
                targetRC.getName(),
                null,
                getContextManager());
        tmpColumnRef.setSource(targetRC);
        tmpColumnRef.setNestingLevel(this.getLevel());
        tmpColumnRef.setSourceLevel(this.getLevel());
        newRC=(ResultColumn)getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                tmpColumnRef.getColumnName(),
                tmpColumnRef,
                getContextManager());
        newRC.markGenerated();
        newRC.bindResultColumnToExpression();
        return newRC;
    }

    /**
     * Comparator class for GROUP BY expression substitution.
     * <p/>
     * This class enables the sorting of a collection of
     * SubstituteExpressionVisitor instances. We sort the visitors
     * during the tree manipulation processing in order to process
     * expressions of higher complexity prior to expressions of
     * lower complexity. Processing the expressions in this order ensures
     * that we choose the best match for an expression, and thus avoids
     * problems where we substitute a sub-expression instead of the
     * full expression. For example, if the statement is:
     * ... GROUP BY a+b, a, a*(a+b), a+b+c
     * we'll process those expressions in the order: a*(a+b),
     * a+b+c, a+b, then a.
     */
    @SuppressFBWarnings(value="SE_COMPARATOR_SHOULD_BE_SERIALIZABLE", justification="DB-9367")
    private static final class ExpressionSorter implements Comparator<SubstituteExpressionVisitor>{
        @Override
        public int compare(SubstituteExpressionVisitor o1,SubstituteExpressionVisitor o2){
            try{
                ValueNode v1=o1.getSource();
                ValueNode v2=o2.getSource();
                int refCount1, refCount2;
                CollectNodesVisitor vis=new CollectNodesVisitor(
                        ColumnReference.class);
                v1.accept(vis);
                refCount1=vis.getList().size();
                vis=new CollectNodesVisitor(ColumnReference.class);
                v2.accept(vis);
                refCount2=vis.getList().size();
                // The ValueNode with the larger number of refs
                // should compare lower. That way we are sorting
                // the expressions in descending order of complexity.
                return refCount2-refCount1;
            }catch(StandardException e){
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String printExplainInformation(String attrDelim) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb = sb.append(spaceToLevel())
                .append("GroupBy").append("(")
                .append("n=").append(getResultSetNumber());
        sb.append(attrDelim).append(getFinalCostEstimate(false).prettyProcessingString(attrDelim));
        sb = sb.append(")");
        return sb.toString();
    }

    public HashSet<String> getNoStatsColumns() throws StandardException {
        assert costEstimate != null : "Trying to get columns missing statistic but there is no cost estimation.";

        HashSet<String> noStatsColumns = new HashSet<>();
        CollectNodesVisitor cnv = new CollectNodesVisitor(ColumnReference.class);
        for (OrderedColumn oc : groupingList) {
            oc.accept(cnv);
        }
        // we do not need stats on columns inside aggregate functions, no need to visit them
        List<ColumnReference> columnRefNodes = cnv.getList();
        for (ColumnReference cr : columnRefNodes) {
            if (!cr.useRealColumnStatistics())
                noStatsColumns.add(cr.getSource().getSchemaName() + "." + cr.getSource().getFullName());
        }
        return noStatsColumns;
    }

}
