/*
   Derby - Class org.apache.derby.impl.sql.compile.WindowResultSetNode

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.compile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.LanguageFactory;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.impl.sql.execute.AggregatorInfo;
import org.apache.derby.impl.sql.execute.AggregatorInfoList;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;


/**
 * A WindowResultSetNode represents a result set for a window partitioning operation
 * on a select.  Note that this includes a SELECT with aggregates
 * and no grouping columns (in which case the select list is null)
 * It has the same description as its input result set.
 * <p/>
 * For the most part, it simply delegates operations to its bottomPRSet,
 * which is currently expected to be a ProjectRestrictResultSet generated
 * for a SelectNode.
 * <p/>
 * NOTE: A WindowResultSetNode extends FromTable since it can exist in a FromList.
 * <p/>
 * Modelled on the code in GroupByNode.
 */
public class WindowResultSetNode extends SingleChildResultSetNode {

    WindowDefinitionNode wdn;
    /**
     * The Partition clause
     */
    Partition partition;

    /**
     * The list of all window functions in the query block
     * that contains this partition.
     */
    Vector windowFunctions;

    /**
     * The list of aggregate nodes we've processed as
     * window functions
     */
    Vector<AggregateNode> processedAggregates = new Vector<AggregateNode>();

    /**
     * Information that is used at execution time to
     * process aggregates.
     */
    private AggregatorInfoList aggInfo;

    /**
     * The parent to the WindowResultSetNode.  We generate a ProjectRestrict
     * over the windowing node and parent is set to that node.
     */
    FromTable parent;

    private boolean	addDistinctAggregate;
    private int		addDistinctAggregateColumnNum;

    // Is the source in sorted order
    private boolean isInSortedOrder;

    /**
     * Intializer for a WindowResultSetNode.
     *
     * @param bottomPR        The child FromTable
     * @param windowDef       The window definition
     * @param windowFunctions The vector of aggregates from
     *                        the query block.  Since aggregation is done
     *                        at the same time as grouping, we need them
     *                        here.
     * @param tableProperties Properties list associated with the table
     * @param nestingLevel    nestingLevel of this group by node. This is used for
     *                        error checking of group by queries with having clause.
     * @throws StandardException Thrown on error
     */
    public void init(
        Object bottomPR,
        Object windowDef,
        Object windowFunctions,
        Object tableProperties,
        Object nestingLevel)
        throws StandardException {
        super.init(bottomPR, tableProperties);
        setLevel(((Integer) nestingLevel).intValue());
        /* Group by without aggregates gets xformed into distinct */
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(((Vector) windowFunctions).size() > 0,
                                 "windowFunctions expected to be non-empty");
            if (!(childResult instanceof Optimizable)) {
                SanityManager.THROWASSERT("childResult, " + childResult.getClass().getName() +
                                              ", expected to be instanceof Optimizable");
            }
            if (!(childResult instanceof FromTable)) {
                SanityManager.THROWASSERT("childResult, " + childResult.getClass().getName() +
                                              ", expected to be instanceof FromTable");
            }
        }

        this.wdn = (WindowDefinitionNode) windowDef;
        this.partition = wdn.getPartition();
        this.windowFunctions = (Vector) windowFunctions;
        this.parent = this;

		/*
		** The first thing we do is put ourselves on
		** top of the SELECT.  The select becomes the
		** childResult.  So our RCL becomes its RCL (so
		** nodes above it now point to us).  Map our
		** RCL to its columns.
		*/
        ResultColumnList newBottomRCL;
        newBottomRCL = childResult.getResultColumns().copyListAndObjects();
        resultColumns = childResult.getResultColumns();
        childResult.setResultColumns(newBottomRCL);

		/*
		** We have aggregates, so we need to add
		** an extra PRNode and we also have to muck around
		** with our trees a might.
		*/
        addAggregates();

		/* We say that the source is never in sorted order if there is a distinct aggregate.
		 * (Not sure what happens if it is, so just skip it for now.)
		 * Otherwise, we check to see if the source is in sorted order on any permutation
		 * of the grouping columns.)
		 */
        if (! addDistinctAggregate && partition != null)
        {
            ColumnReference[] crs =
                new ColumnReference[this.partition.size()];

            // Now populate the CR array and see if ordered
            int glSize = this.partition.size();
            int index;
            for (index = 0; index < glSize; index++)
            {
                GroupByColumn gc =
                    (GroupByColumn) this.partition.elementAt(index);
                if (gc.getColumnExpression() instanceof ColumnReference)
                {
                    crs[index] = (ColumnReference)gc.getColumnExpression();
                }
                else
                {
                    isInSortedOrder = false;
                    break;
                }

            }
            if (index == glSize) {
                isInSortedOrder = childResult.isOrderedOn(crs, true, (Vector)null);
            }
        }
    }

    /**
     * Get the aggregates that were processed as window function
     *
     * @return list of aggregates processed as window functions
     */
    Vector<AggregateNode> getProcessedAggregates() {
        return this.processedAggregates;
    }

    /**
     * Get whether or not the source is in sorted order.
     *
     * @return Whether or not the source is in sorted order.
     */
    boolean getIsInSortedOrder() {
        return isInSortedOrder;
    }

    /**
     * Add the extra result columns required by the aggregates
     * to the result list.
     *
     * @throws StandardException
     */
    private void addAggregates() throws StandardException {
        addNewPRNode();
        addNewColumnsForAggregation();
        addDistinctAggregatesToOrderBy();
    }

    /**
     * Add any distinct aggregates to the order by list.
     * Asserts that there are 0 or more distincts.
     */
    private void addDistinctAggregatesToOrderBy()
    {
        int numDistinct = numDistinctAggregates(windowFunctions);
        if (numDistinct != 0)
        {
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(partition != null || numDistinct == 1,
                                     "Should not have more than 1 distinct aggregate per Partition");
            }

            AggregatorInfo agg = null;
            int count = aggInfo.size();
            for (int i = 0; i < count; i++)
            {
                agg = (AggregatorInfo) aggInfo.elementAt(i);
                if (agg.isDistinct())
                {
                    break;
                }
            }

            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(agg != null && agg.isDistinct());
            }

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
    private void addNewPRNode() throws StandardException {
		/*
		** Get the new PR, put above the GroupBy.
		*/
        ResultColumnList rclNew = (ResultColumnList) getNodeFactory().getNode(
            C_NodeTypes.RESULT_COLUMN_LIST,
            getContextManager());
        int sz = resultColumns.size();
        for (int i = 0; i < sz; i++) {
            ResultColumn rc = (ResultColumn) resultColumns.elementAt(i);
            if (!rc.isGenerated()) {
                rclNew.addElement(rc);
            }
        }

        // if any columns in the source RCL were generated for an order by
        // remember it in the new RCL as well. After the sort is done it will
        // have to be projected out upstream.
        rclNew.copyOrderBySelect(resultColumns);

        parent = (FromTable) getNodeFactory().getNode(
            C_NodeTypes.PROJECT_RESTRICT_NODE,
            this,    // child
            rclNew,
            null, //havingClause,
            null,                // restriction list
            null,                // project subqueries
            null,               // having subqueries
            tableProperties,
            getContextManager());


		/*
		** Reset the bottom RCL to be empty.
		*/
        childResult.setResultColumns((ResultColumnList)
                                         getNodeFactory().getNode(
                                             C_NodeTypes.RESULT_COLUMN_LIST,
                                             getContextManager()));

        /*
         * Set the Windowing RCL to be empty
         */
        resultColumns = (ResultColumnList) getNodeFactory().getNode(
            C_NodeTypes.RESULT_COLUMN_LIST,
            getContextManager());

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
     * For each aggregate in windowFunctions <UL>
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
    private void addNewColumnsForAggregation()
        throws StandardException {
        aggInfo = new AggregatorInfoList();

        if (partition != null) {
            addUnAggColumns();
        }
        addAggregateColumns();
    }

    /**
     * In the query rewrite for partition, add the columns on which we are doing
     * the partition.
     *
     * @see #addNewColumnsForAggregation
     */
    private void addUnAggColumns() throws StandardException {
        ResultColumnList bottomRCL = childResult.getResultColumns();
        ResultColumnList groupByRCL = resultColumns;

        List<OrderedColumn> overClauseColumns = collectOverClauseColumns();
        ArrayList referencesToSubstitute = new ArrayList();
        for (OrderedColumn oc : overClauseColumns) {
            ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##PartitionColumn_"+oc.getColumnExpression().getColumnName(),
                oc.getColumnExpression(),
                getContextManager());

            // add this result column to the bottom rcl
            bottomRCL.addElement(newRC);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            newRC.setVirtualColumnId(bottomRCL.size());

            // now add this column to the groupbylist
            ResultColumn gbRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##PartitionColumn_"+oc.getColumnExpression().getColumnName(),
                oc.getColumnExpression(),
                getContextManager());
            groupByRCL.addElement(gbRC);
            gbRC.markGenerated();
            gbRC.bindResultColumnToExpression();
            gbRC.setVirtualColumnId(groupByRCL.size());

            /*
             ** Reset the original node to point to the
             ** Group By result set.
             */
            VirtualColumnNode vc = (VirtualColumnNode) getNodeFactory().getNode(
                C_NodeTypes.VIRTUAL_COLUMN_NODE,
                this, // source result set.
                gbRC,
                new Integer(groupByRCL.size()),
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
            ValueNode vn = oc.getColumnExpression();
            SubstituteExpressionVisitor vis =
                new SubstituteExpressionVisitor(vn, vc,
                                                AggregateNode.class);
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
            oc.setColumnPosition(bottomRCL.size());
        }
        Comparator sorter = new ExpressionSorter();
        Collections.sort(referencesToSubstitute, sorter);
        for (int r = 0; r < referencesToSubstitute.size(); r++)
            parent.getResultColumns().accept(
                (SubstituteExpressionVisitor) referencesToSubstitute.get(r));
    }

    private List<OrderedColumn> collectOverClauseColumns() {
        int partitionSize = (partition != null ? partition.size() : 0);
        OrderByList orderByList = wdn.getOrderByList();
        int oderbySize = (orderByList != null? orderByList.size():0);
        List<OrderedColumn> cols = new ArrayList<OrderedColumn>(partitionSize + oderbySize);
        if (partition != null) {
            for (int i=0; i<partitionSize; ++i) {
                cols.add((OrderedColumn) partition.elementAt(i));
            }
        }
        if (orderByList != null) {
            for (int i=0; i<orderByList.size(); ++i) {
                cols.add((OrderedColumn) orderByList.elementAt(i));
            }
        }
        return cols;
    }

    /**
     * In the query rewrite involving aggregates, add the columns for
     * aggregation.
     *
     * @see #addNewColumnsForAggregation
     */
    private void addAggregateColumns() throws StandardException {
        DataDictionary dd = getDataDictionary();
        WindowFunctionNode windowFunctionNode = null;
        ColumnReference newColumnRef;
        ResultColumn newRC;
        ResultColumn tmpRC;
        ResultColumn aggResultRC;
        ResultColumnList bottomRCL = childResult.getResultColumns();
        ResultColumnList groupByRCL = resultColumns;
        ResultColumnList aggRCL;
        int aggregatorVColId;
        int aggInputVColId;
        int aggResultVColId;

		/*
		 ** Now process all of the aggregates.  Replace
		 ** every windowFunctionNode with an RC.  We toss out
		 ** the list of RCs, we need to get each RC
		 ** as we process its corresponding windowFunctionNode.
		 */
        LanguageFactory lf = getLanguageConnectionContext().getLanguageFactory();

        ReplaceAggregatesWithCRVisitor replaceAggsVisitor =
            new ReplaceAggregatesWithCRVisitor(
                (ResultColumnList) getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN_LIST,
                    getContextManager()),
                ((FromTable) childResult).getTableNumber(),
                ResultSetNode.class);
        parent.getResultColumns().accept(replaceAggsVisitor);

		/*
		** For each windowFunctionNode
		*/
        int alSize = windowFunctions.size();
        for (int index = 0; index < alSize; index++) {
            windowFunctionNode = (WindowFunctionNode) windowFunctions.get(index);

			/*
			** AGG RESULT: Set the windowFunctionNode result to null in the
			** bottom project restrict.
			*/
            newRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##WindowResult",
                windowFunctionNode.getNewNullResultExpression(),
                getContextManager());
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            aggResultVColId = newRC.getVirtualColumnId();

			/*
			** Set the GB windowFunctionNode result column to
			** point to this.  The GB windowFunctionNode result
			** was created when we called
			** ReplaceAggregatesWithCRVisitor()
			*/
            newColumnRef = (ColumnReference) getNodeFactory().getNode(
                C_NodeTypes.COLUMN_REFERENCE,
                newRC.getName(),
                null,
                getContextManager());
            newColumnRef.setSource(newRC);
            newColumnRef.setNestingLevel(this.getLevel());
            newColumnRef.setSourceLevel(this.getLevel());
            tmpRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                newRC.getColumnName(),
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
            newColumnRef = windowFunctionNode.getGeneratedRef();
            newColumnRef.setSource(tmpRC);

			/*
			** AGG INPUT: Create a ResultColumn in the bottom
			** project restrict that has the expression that is
			** to be aggregated
			*/
            newRC = windowFunctionNode.getNewExpressionResultColumn(dd);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            aggInputVColId = newRC.getVirtualColumnId();
            aggResultRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##WindowExpression",
                windowFunctionNode.getNewNullResultExpression(),
                getContextManager());


			/*
			** Add a reference to this column into the
			** group by columns.
			*/
            tmpRC = getColumnReference(newRC, dd);
            groupByRCL.addElement(tmpRC);
            tmpRC.setVirtualColumnId(groupByRCL.size());

			/*
			** AGGREGATOR: Add a getAggregator method call
			** to the bottom result column list.
			*/
            newRC = windowFunctionNode.getNewAggregatorResultColumn(dd);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            aggregatorVColId = newRC.getVirtualColumnId();

			/*
			** Add a reference to this column in the Group By result
			** set.
			*/
            tmpRC = getColumnReference(newRC, dd);
            groupByRCL.addElement(tmpRC);
            tmpRC.setVirtualColumnId(groupByRCL.size());

			/*
			** Piece together a fake one column rcl that we will use
			** to generate a proper result description for input
			** to this agg if it is a user agg.
			*/
            aggRCL = (ResultColumnList) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN_LIST,
                getContextManager());
            aggRCL.addElement(aggResultRC);

			/*
			** Note that the column ids in the row are 0 based
			** so we have to subtract 1.
			*/
            aggInfo.addElement(new AggregatorInfo(
                windowFunctionNode.getAggregateName(),
                windowFunctionNode.getAggregatorClassName(),
                aggInputVColId - 1,            // windowFunctionNode input column
                aggResultVColId - 1,            // the windowFunctionNode result column
                aggregatorVColId - 1,        // the aggregator column
                windowFunctionNode.isDistinct(),
                lf.getResultDescription(aggRCL.makeResultDescriptors(), "SELECT")
            ));
            this.processedAggregates.add(windowFunctionNode.getWrappedAggregate());
        }
    }

    /**
     * Return the parent node to this one, if there is
     * one.  It will return 'this' if there is no generated
     * node above this one.
     *
     * @return the parent node
     */
    public FromTable getParent() {
        return parent;
    }


	/*
	 *  Optimizable interface
	 */

    /**
     * @throws StandardException Thrown on error
     * @see Optimizable#optimizeIt
     */
    public CostEstimate optimizeIt(
        Optimizer optimizer,
        OptimizablePredicateList predList,
        CostEstimate outerCost,
        RowOrdering rowOrdering)
        throws StandardException {
        // RESOLVE: NEED TO FACTOR IN THE COST OF GROUPING (SORTING) HERE
        CostEstimate childCost = ((Optimizable) childResult).optimizeIt(
            optimizer,
            predList,
            outerCost,
            rowOrdering);

        CostEstimate retval = super.optimizeIt(
            optimizer,
            predList,
            outerCost,
            rowOrdering
        );

        return retval;
    }

    /**
     * @throws StandardException Thrown on error
     * @see Optimizable#estimateCost
     */
    public CostEstimate estimateCost(OptimizablePredicateList predList,
                                     ConglomerateDescriptor cd,
                                     CostEstimate outerCost,
                                     Optimizer optimizer,
                                     RowOrdering rowOrdering
    )
        throws StandardException {
        // RESOLVE: NEED TO FACTOR IN THE COST OF GROUPING (SORTING) HERE
        //
        CostEstimate childCost = ((Optimizable) childResult).estimateCost(
            predList,
            cd,
            outerCost,
            optimizer,
            rowOrdering);

        CostEstimate costEstimate = getCostEstimate(optimizer);
        costEstimate.setCost(childCost.getEstimatedCost(),
                             childCost.rowCount(),
                             childCost.singleScanRowCount());

        return costEstimate;
    }

    /**
     * @throws StandardException Thrown on error
     * @see org.apache.derby.iapi.sql.compile.Optimizable#pushOptPredicate
     */

    public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate)
        throws StandardException {
        return ((Optimizable) childResult).pushOptPredicate(optimizablePredicate);
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        if (SanityManager.DEBUG) {
            return wdn.toString()+ "\n" + super.toString();
        } else {
            return "";
        }
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */
    public void printSubNodes(int depth) {
        if (SanityManager.DEBUG) {
            super.printSubNodes(depth);

            printLabel(depth, "windowFunctions:\n");

            for (int i = 0; i < windowFunctions.size(); i++) {
                AggregateNode agg =
                    (AggregateNode) windowFunctions.get(i);
                debugPrint(formatNodeString("[" + i + "]:", depth + 1));
                agg.treePrint(depth + 1);
            }

            printLabel(depth, "windowDefintionNode: ");
            wdn.treePrint(depth + 1);
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
    public boolean flattenableInFromSubquery(FromList fromList) {
		/* Can't flatten a WindowResultSetNode */
        return false;
    }

    /**
     * Optimize this WindowResultSetNode.
     *
     * @param dataDictionary The DataDictionary to use for optimization
     * @param predicates     The PredicateList to optimize.  This should
     *                       be a join predicate.
     * @param outerRows      The number of outer joining rows
     * @throws StandardException Thrown on error
     * @return ResultSetNode    The top of the optimized subtree
     */

    public ResultSetNode optimize(DataDictionary dataDictionary,
                                  PredicateList predicates,
                                  double outerRows)
        throws StandardException {
		/* We need to implement this method since a PRN can appear above a
		 * SelectNode in a query tree.
		 */
        childResult = (ResultSetNode) childResult.optimize(
            dataDictionary,
            predicates,
            outerRows);
        Optimizer optimizer = getOptimizer(
            (FromList) getNodeFactory().getNode(
                C_NodeTypes.FROM_LIST,
                getNodeFactory().doJoinOrderOptimization(),
                getContextManager()),
            predicates,
            dataDictionary,
            (RequiredRowOrdering) null);

        // RESOLVE: NEED TO FACTOR IN COST OF SORTING AND FIGURE OUT HOW
        // MANY ROWS HAVE BEEN ELIMINATED.
        costEstimate = optimizer.newCostEstimate();

        costEstimate.setCost(childResult.getCostEstimate().getEstimatedCost(),
                             childResult.getCostEstimate().rowCount(),
                             childResult.getCostEstimate().singleScanRowCount());

        return this;
    }

    ResultColumnDescriptor[] makeResultDescriptors() {
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
    public boolean isOneRowResultSet() throws StandardException {
        // Only consider scalar aggregates for now
        return ((partition == null) || (partition.size() == 0));
    }

    /**
     * generate the sort result set operating over the source
     * resultset.  Adds distinct aggregates to the sort if
     * necessary.
     *
     * @throws StandardException Thrown on error
     */
    public void generate(ActivationClassBuilder acb,  MethodBuilder mb) throws StandardException {

		/* Get the next ResultSet#, so we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
        assignResultSetNumber();

        // Get the final cost estimate from the child.
        costEstimate = childResult.getFinalCostEstimate();

		/*
		** Get the column ordering for the sort.  Note that
		** for a scalar aggregate we may not have any ordering
		** columns (if there are no distinct aggregates).
		** WARNING: if a distinct aggregate is passed to
		** SortResultSet it assumes that the last column
		** is the distinct one.  If this assumption changes
		** then SortResultSet will have to change.
		*/
        FormatableArrayHolder partitionHolder = createColumnOrdering(partition);
        if (addDistinctAggregate) {
            partitionHolder = acb.addColumnToOrdering(partitionHolder, addDistinctAggregateColumnNum);
        }

        // add column ordering from order by to end of partition following what's stated above
        FormatableArrayHolder orderByHolder = createColumnOrdering(wdn.getOrderByList());

        if (SanityManager.DEBUG) {
            if (SanityManager.DEBUG_ON("WindowTrace")) {
                StringBuilder s = new StringBuilder();

                s.append("Partition column ordering is (");
                org.apache.derby.iapi.store.access.ColumnOrdering[] ordering =
                    (org.apache.derby.iapi.store.access.ColumnOrdering[]) partitionHolder.getArray(org.apache.derby
                                                                                                      .iapi.store
                                                                                                      .access
                                                                                                      .ColumnOrdering
                                                                                                      .class);

                for (int i = 0; i < ordering.length; i++) {
                    s.append(ordering[i].getColumnId());
                    s.append(" ");
                }
                s.append(")");
                SanityManager.DEBUG("WindowTrace", s.toString());
            }
        }

        int partitionItemIndex = acb.addItem(partitionHolder);
        int orderByItemIndex = acb.addItem(orderByHolder);

		/*
		** We have aggregates, so save the aggInfo
		** struct in the activation and store the number
		*/
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(aggInfo != null, "aggInfo not set up as expected");
        }
        int aggInfoItemIndex = acb.addItem(aggInfo);

        // add the window frame definition
        int frameDefnIndex = acb.addItem(wdn.getFrameExtent().toMap());

        acb.pushGetResultSetFactoryExpression(mb);


		/* Generate the WindowResultSet:
		 *	arg1: childExpress - Expression for childResult
		 *  arg2: isInSortedOrder - true if source result set in sorted order
		 *  arg3: aggInfoItem - entry in saved objects for the aggregates,
		 *  arg4: partitionItemIndex - index of entry in saved objects for the partition
		 *  arg5: orderByItemIndex - index of entry in saved objects for the ordering
		 *  arg6: frameDefnIndex - index of entry in saved objects for the frame definition
		 *  arg7: Activation
		 *  arg8: number of columns in the result
		 *  arg9: resultSetNumber
		 *  arg10: row count estimate for cost
		 *  arg11: estimated cost
		 */
        // Generate the child ResultSet
        childResult.generate(acb, mb);
        mb.push(isInSortedOrder);
        mb.push(aggInfoItemIndex);
        mb.push(partitionItemIndex);
        mb.push(orderByItemIndex);
        mb.push(frameDefnIndex);

        resultColumns.generateHolder(acb, mb);

        mb.push(resultColumns.getTotalColumnSize());
        mb.push(resultSetNumber);
        mb.push(costEstimate.rowCount());
        mb.push(costEstimate.getEstimatedCost());

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getWindowResultSet",
                      ClassName.NoPutResultSet, 11);

    }

    ///////////////////////////////////////////////////////////////
    //
    // UTILITIES
    //
    ///////////////////////////////////////////////////////////////

    /**
     * Create the zero-based column ordering for one or more ordered column lists
     * @param orderedColumnLists using varargs ordered column list here so that we can
     *                           add both partition and orderby lists at once.
     * @return The holder for the column ordering array.
     */
    private FormatableArrayHolder createColumnOrdering(OrderedColumnList... orderedColumnLists) {
        FormatableArrayHolder colOrdering = null;
        if (orderedColumnLists != null) {
            Map<Integer, OrderedColumn> map = new TreeMap<Integer, OrderedColumn>();
            for (int i=0; i<orderedColumnLists.length && orderedColumnLists[i] != null; i++) {
                for (OrderedColumn orderedColumn : orderedColumnLists[i]) {
                    map.put(i, orderedColumn);
                }
            }
            IndexColumnOrder[] ordering = new IndexColumnOrder[map.size()];
            int j = 0;
            for (OrderedColumn oc : map.values()) {
                ordering[j++] = new IndexColumnOrder(oc.getColumnPosition()-1, oc.isAscending(), oc.isNullsOrderedLow());
            }
            colOrdering = new FormatableArrayHolder(ordering);
        }

        if (colOrdering == null) {
            colOrdering = new FormatableArrayHolder(new IndexColumnOrder[0]);
        }
        return colOrdering;
    }

    /**
     * Method for creating a new result column referencing
     * the one passed in.
     *
     * @return the new result column
     * @throws StandardException on error
     * @param    targetRC    the source
     * @param    dd
     */
    private ResultColumn getColumnReference(ResultColumn targetRC,
                                            DataDictionary dd)
        throws StandardException {
        ColumnReference tmpColumnRef;
        ResultColumn newRC;

        tmpColumnRef = (ColumnReference) getNodeFactory().getNode(
            C_NodeTypes.COLUMN_REFERENCE,
            targetRC.getName(),
            null,
            getContextManager());
        tmpColumnRef.setSource(targetRC);
        tmpColumnRef.setNestingLevel(this.getLevel());
        tmpColumnRef.setSourceLevel(this.getLevel());
        newRC = (ResultColumn) getNodeFactory().getNode(
            C_NodeTypes.RESULT_COLUMN,
            targetRC.getColumnName(),
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
    private static final class ExpressionSorter implements Comparator {
        public int compare(Object o1, Object o2) {
            try {
                ValueNode v1 = ((SubstituteExpressionVisitor) o1).getSource();
                ValueNode v2 = ((SubstituteExpressionVisitor) o2).getSource();
                int refCount1, refCount2;
                CollectNodesVisitor vis = new CollectNodesVisitor(
                    ColumnReference.class);
                v1.accept(vis);
                refCount1 = vis.getList().size();
                vis = new CollectNodesVisitor(ColumnReference.class);
                v2.accept(vis);
                refCount2 = vis.getList().size();
                // The ValueNode with the larger number of refs
                // should compare lower. That way we are sorting
                // the expressions in descending order of complexity.
                return refCount2 - refCount1;
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
