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

import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicate;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.RequiredRowOrdering;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;

import com.splicemachine.db.iapi.reference.ClassName;

import com.splicemachine.db.iapi.services.classfile.VMOpcode;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.compiler.MethodBuilder;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.util.*;

/**
 * A DistinctNode represents a result set for a disinct operation
 * on a select.  It has the same description as its input result set.
 *
 * For the most part, it simply delegates operations to its childResultSet,
 * which is currently expected to be a ProjectRestrictResultSet generated
 * for a SelectNode.
 *
 * NOTE: A DistinctNode extends FromTable since it can exist in a FromList.
 *
 */
public class DistinctNode extends SingleChildResultSetNode
{
    boolean inSortedOrder;

    @Override
    public boolean isParallelizable() {
        return true; //is a distinct scan and/or aggregate
    }

    /**
     * Initializer for a DistinctNode.
     *
     * @param childResult    The child ResultSetNode
     * @param inSortedOrder    Whether or not the child ResultSetNode returns its
     *                        output in sorted order.
     * @param tableProperties    Properties list associated with the table
     *
     * @exception StandardException        Thrown on error
     */
    public void init(
                        Object childResult,
                        Object inSortedOrder,
                        Object tableProperties) throws StandardException
    {
        super.init(childResult, tableProperties);

        if (SanityManager.DEBUG)
        {
            if (!(childResult instanceof Optimizable))
            {
                SanityManager.THROWASSERT("childResult, " + childResult.getClass().getName() +
                    ", expected to be instanceof Optimizable");
            }
            if (!(childResult instanceof FromTable))
            {
                SanityManager.THROWASSERT("childResult, " + childResult.getClass().getName() +
                    ", expected to be instanceof FromTable");
            }
        }

        ResultColumnList prRCList;

        /*
            We want our own resultColumns, which are virtual columns
            pointing to the child result's columns.

            We have to have the original object in the distinct node,
            and give the underlying project the copy.
         */

        /* We get a shallow copy of the ResultColumnList and its
         * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
         */
        prRCList = this.childResult.getResultColumns().copyListAndObjects();
        resultColumns = this.childResult.getResultColumns();
        this.childResult.setResultColumns(prRCList);

        /* Replace ResultColumn.expression with new VirtualColumnNodes
         * in the DistinctNode's RCL.  (VirtualColumnNodes include
         * pointers to source ResultSetNode, this, and source ResultColumn.)
         */
        resultColumns.genVirtualColumnNodes(this, prRCList);

        /* Verify that we can perform a DISTINCT on the
         * underlying tree.
         */
        resultColumns.verifyAllOrderable();

        this.inSortedOrder = (Boolean) inSortedOrder;
    }

    /*
     *  Optimizable interface
     */

    /**
     * @see Optimizable#optimizeIt
     *
     * @exception StandardException        Thrown on error
     */
    public CostEstimate optimizeIt(Optimizer optimizer,
                                   OptimizablePredicateList predList,
                                   CostEstimate outerCost,
                                   RowOrdering rowOrdering)
            throws StandardException
    {
        // RESOLVE: NEED TO FACTOR IN THE COST OF GROUPING (SORTING) HERE
        ((Optimizable) childResult).optimizeIt(
                optimizer, predList, outerCost, rowOrdering);

        return super.optimizeIt(optimizer, predList, outerCost, rowOrdering);
    }

    /**
     * @see Optimizable#estimateCost
     *
     * @exception StandardException        Thrown on error
     */
    public CostEstimate estimateCost(OptimizablePredicateList predList,
                                     ConglomerateDescriptor cd,
                                     CostEstimate outerCost,
                                     Optimizer optimizer,
                                     RowOrdering rowOrdering)
            throws StandardException
    {
        costEstimate = getNewCostEstimate();
        CostEstimate baseCost = childResult.getFinalCostEstimate(true);
        double rc = baseCost.rowCount() > 1? baseCost.rowCount():1;
        double outputHeapSizePerRow = baseCost.getEstimatedHeapSize()/rc;
        double remoteCostPerRow=baseCost.remoteCost()/rc;

        double sortCost = 0;
        double outputHeapSize = 0d;

        ResultColumnList rcl = childResult.getResultColumns();
        double selectivity = 0d;
        for (int i = 1; i <= rcl.size(); ++i) {
            ResultColumn resultColumn = rcl.getResultColumn(i);
            selectivity += resultColumn.nonZeroCardinality((long) rc) / rc;
        }

        double tableCardinality = Math.min(1, selectivity) * rc;

        sortCost = tableCardinality * remoteCostPerRow;
        outputHeapSize = tableCardinality * outputHeapSizePerRow;

        double totalLocalCost = baseCost.localCost()/baseCost.partitionCount();
        double totalRemoteCost = sortCost + remoteCostPerRow * rc;
        costEstimate.setLocalCost(totalLocalCost);
        costEstimate.setRemoteCost(totalRemoteCost);
        costEstimate.setNumPartitions(1);
        costEstimate.setLocalCostPerPartition(totalLocalCost);
        costEstimate.setRemoteCostPerPartition(totalRemoteCost);
        costEstimate.setRowCount(tableCardinality);
        costEstimate.setEstimatedHeapSize((long)outputHeapSize);

        return costEstimate;
    }

    /**
     * @see com.splicemachine.db.iapi.sql.compile.Optimizable#pushOptPredicate
     *
     * @exception StandardException        Thrown on error
     */

    public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate)
            throws StandardException
    {
        return false;
        // return ((Optimizable) childResult).pushOptPredicate(optimizablePredicate);
    }


    /**
     * Optimize this DistinctNode.
     *
     * @param dataDictionary    The DataDictionary to use for optimization
     * @param predicates        The PredicateList to optimize.  This should
     *                            be a join predicate.
     * @param outerRows            The number of outer joining rows
     *
     * @param forSpark
     * @return    ResultSetNode    The top of the optimized subtree
     *
     * @exception StandardException        Thrown on error
     */

    public ResultSetNode optimize(DataDictionary dataDictionary,
                                  PredicateList predicates,
                                  double outerRows,
                                  boolean forSpark)
                    throws StandardException
    {
        /* We need to implement this method since a PRN can appear above a
         * SelectNode in a query tree.
         */
        childResult = (ResultSetNode) childResult.optimize(
                dataDictionary,
                predicates,
                outerRows,
                forSpark);
        Optimizer optimizer = getOptimizer(
                        (FromList) getNodeFactory().getNode(
                            C_NodeTypes.FROM_LIST,
                            getNodeFactory().doJoinOrderOptimization(),
                            this,
                            getContextManager()),
                        predicates,
                        dataDictionary,
                        (RequiredRowOrdering) null);
        optimizer.setForSpark(forSpark);

        // RESOLVE: NEED TO FACTOR IN COST OF SORTING AND FIGURE OUT HOW
        // MANY ROWS HAVE BEEN ELIMINATED.
        costEstimate = optimizer.newCostEstimate();

        costEstimate.setCost(childResult.getCostEstimate().getEstimatedCost(),
                             childResult.getCostEstimate().rowCount(),
                             childResult.getCostEstimate().singleScanRowCount());

        return this;
    }

    /**
     * Return whether or not the underlying ResultSet tree
     * is ordered on the specified columns.
     * RESOLVE - This method currently only considers the outermost table
     * of the query block.
     *
     * @param    crs                    The specified ColumnReference[]
     * @param    permuteOrdering        Whether or not the order of the CRs in the array can be permuted
     * @param    fbtVector            Vector that is to be filled with the FromBaseTable
     *
     * @return    Whether the underlying ResultSet tree
     * is ordered on the specified column.
     */
    boolean isOrderedOn(ColumnReference[] crs, boolean permuteOrdering, Vector fbtVector)
    {
        /* RESOLVE - DistinctNodes are ordered on their RCLs.
         * Walk RCL to see if cr is 1st non-constant column in the
         * ordered result.
         */
        return false;
    }

    /**
     * generate the distinct result set operating over the source
     * resultset.
     *
     * @exception StandardException        Thrown on error
     */
    public void generate(ActivationClassBuilder acb,
                                MethodBuilder mb)
                            throws StandardException
    {
        /* Get the next ResultSet#, so we can number this ResultSetNode, its
         * ResultColumnList and ResultSet.
         */
        assignResultSetNumber();

        // Get the final cost estimate based on the child's cost.
        costEstimate = getFinalCostEstimate(false);

        /*
            create the orderItem and stuff it in.
         */
        int orderItem = acb.addItem(acb.getColumnOrdering(resultColumns));

        /* Generate the SortResultSet:
         *    arg1: childExpress - Expression for childResultSet
         *  arg2: distinct - true, of course
         *  arg3: isInSortedOrder - is the source result set in sorted order
         *  arg4: orderItem - entry in saved objects for the ordering
         *  arg5: rowAllocator - method to construct rows for fetching
         *            from the sort
         *  arg6: row size
         *  arg7: resultSetNumber
         *  arg8: explain plan
         */

        acb.pushGetResultSetFactoryExpression(mb);

        childResult.generate(acb, mb);
        mb.push(true);
        mb.push(inSortedOrder);
        mb.push(orderItem);
        resultColumns.generateHolder(acb, mb);
        mb.push(resultColumns.getTotalColumnSize());
        mb.push(resultSetNumber);
        mb.push(costEstimate.rowCount());
        mb.push(costEstimate.getEstimatedCost());
        mb.push(printExplainInformationForActivation());

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getSortResultSet",
                ClassName.NoPutResultSet, 10);
    }


    @Override
    public CostEstimate getFinalCostEstimate(boolean useSelf) throws StandardException {

        if (costEstimate != null) {
            finalCostEstimate = costEstimate;
        }
        else {
            finalCostEstimate = childResult.getFinalCostEstimate(true);
        }
        return finalCostEstimate;
    }

    @Override
    public String printExplainInformation(String attrDelim) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb = sb.append(spaceToLevel())
                .append("Distinct").append("(")
                .append("n=").append(getResultSetNumber());
        sb.append(attrDelim).append(getFinalCostEstimate(false).prettyProcessingString(attrDelim));
        sb = sb.append(")");
        return sb.toString();
    }
}
