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
import java.util.Vector;

import org.apache.derby.catalog.IndexDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.LanguageFactory;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.compile.AccessPath;
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
 * <p/>
 * <p/>
 * <p/>
 * A WindowResultSetNode represents a result set for a window partitioning on a
 * select. Modelled on the code in GroupByNode.
 */
public class WindowResultSetNode extends SingleChildResultSetNode {

    WindowDefinitionNode wdn;
    /**
     * The GROUP BY list
     */
    Partition partition;

    /**
     * The list of all aggregates in the query block
     * that contains this group by.
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

    // TODO: still need dist agg?
    private boolean addDistinctAggregate;
    private boolean singleInputRowOptimization;
    private int addDistinctAggregateColumnNum;

    // Is the source in sorted order
    private boolean isInSortedOrder;

    /**
     * Intializer for a GroupByNode.
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
//			Aggregage vector can be null if we have a having clause.
//          select c1 from t1 group by c1 having c1 > 1;
//			SanityManager.ASSERT(((Vector) windowFunctions).size() > 0,
//			"windowFunctions expected to be non-empty");
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

        if (this.partition != null && this.partition.isRollup()) {
            resultColumns.setNullability(true);
            parent.getResultColumns().setNullability(true);
        }
		/* We say that the source is never in sorted order if there is a distinct aggregate.
		 * (Not sure what happens if it is, so just skip it for now.)
		 * Otherwise, we check to see if the source is in sorted order on any permutation
		 * of the grouping columns.)
		 */
        if (!addDistinctAggregate && partition != null) {
            ColumnReference[] crs =
                new ColumnReference[this.partition.size()];

            // Now populate the CR array and see if ordered
            int glSize = this.partition.size();
            int index;
            for (index = 0; index < glSize; index++) {
                GroupByColumn gc =
                    (GroupByColumn) this.partition.elementAt(index);
                if (gc.getColumnExpression() instanceof ColumnReference) {
                    crs[index] = (ColumnReference) gc.getColumnExpression();
                } else {
                    isInSortedOrder = false;
                    break;
                }

            }
            if (index == glSize) {
                isInSortedOrder = childResult.isOrderedOn(crs, true, (Vector) null);
            }
        }

        // TODO: OrderBy?
        addOrderby();
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

    private void addOrderby() throws StandardException {
        /* Generate the OrderByNode if a sort is still required for
		 * the order by.
		 */
        OrderByList windowOrderBy = wdn.getOrderByList();

        if (windowOrderBy != null && windowOrderBy.getSortNeeded()) {
            ResultSetNode prnRSN = parent;
            prnRSN = (ResultSetNode) getNodeFactory().getNode(
                C_NodeTypes.ORDER_BY_NODE,
                prnRSN,
                windowOrderBy,
                null,
                getContextManager());

            // There may be columns added to the select projection list
            // a query like:
            // select a, b from t group by a,b order by a+b
            // the expr a+b is added to the select list.
            // TODO: probably no orderbyselect in this context
            int orderBySelect = this.getResultColumns().getOrderBySelect();
            if (orderBySelect > 0)  {
                // Keep the same RCL on top, since there may be references to
                // its result columns above us, i.e. in this query:
                //
                // select sum(j),i from t group by i having i
                //             in (select i from t order by j)
                //
                ResultColumnList topList = prnRSN.getResultColumns();
                ResultColumnList newSelectList = topList.copyListAndObjects();
                prnRSN.setResultColumns(newSelectList);

                topList.removeOrderByColumns();
                topList.genVirtualColumnNodes(prnRSN, newSelectList);
                prnRSN = (ResultSetNode) getNodeFactory().getNode(
                    C_NodeTypes.PROJECT_RESTRICT_NODE,
                    prnRSN,
                    topList,
                    null,
                    null,
                    null,
                    null,
                    null,
                    getContextManager());
            }
        }
    }

    /**
     * Add any distinct aggregates to the order by list.
     * Asserts that there are 0 or more distincts.
     */
    private void addDistinctAggregatesToOrderBy() {
        int numDistinct = numDistinctAggregates(windowFunctions);
        if (numDistinct != 0) {
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(partition != null || numDistinct == 1,
                                     "Should not have more than 1 distinct aggregate per Group By node");
            }

            AggregatorInfo agg = null;
            int count = aggInfo.size();
            for (int i = 0; i < count; i++) {
                agg = (AggregatorInfo) aggInfo.elementAt(i);
                if (agg.isDistinct()) {
                    break;
                }
            }

            if (SanityManager.DEBUG) {
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
     * In the query rewrite for group by, add the columns on which we are doing
     * the group by.
     *
     * @see #addNewColumnsForAggregation
     */
    private void addUnAggColumns() throws StandardException {
        ResultColumnList bottomRCL = childResult.getResultColumns();
        ResultColumnList groupByRCL = resultColumns;

        ArrayList referencesToSubstitute = new ArrayList();
        int sz = partition.size();
        for (int i = 0; i < sz; i++) {
            GroupByColumn gbc = (GroupByColumn) partition.elementAt(i);
            ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##UnaggColumn",
                gbc.getColumnExpression(),
                getContextManager());

            // add this result column to the bottom rcl
            bottomRCL.addElement(newRC);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            newRC.setVirtualColumnId(bottomRCL.size());

            // now add this column to the groupbylist
            ResultColumn gbRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##UnaggColumn",
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
            ValueNode vn = gbc.getColumnExpression();
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
            gbc.setColumnPosition(bottomRCL.size());
        }
        Comparator sorter = new ExpressionSorter();
        Collections.sort(referencesToSubstitute, sorter);
        for (int r = 0; r < referencesToSubstitute.size(); r++)
            parent.getResultColumns().accept(
                (SubstituteExpressionVisitor) referencesToSubstitute.get(r));
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
        ArrayList havingRefsToSubstitute = null;

        addUnAggColumns();

        addAggregateColumns();

    }

    /**
     * In the query rewrite involving aggregates, add the columns for
     * aggregation.
     *
     * @see #addNewColumnsForAggregation
     */
    private void addAggregateColumns() throws StandardException {
        DataDictionary dd = getDataDictionary();
        WindowFunctionNode aggregate = null;
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
		 ** every aggregate with an RC.  We toss out
		 ** the list of RCs, we need to get each RC
		 ** as we process its corresponding aggregate.
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
		** For each aggregate
		*/
        int alSize = windowFunctions.size();
        for (int index = 0; index < alSize; index++) {
            aggregate = (WindowFunctionNode) windowFunctions.get(index);

			/*
			** AGG RESULT: Set the aggregate result to null in the
			** bottom project restrict.
			*/
            newRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##window result",
                aggregate.getNewNullResultExpression(),
                getContextManager());
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            aggResultVColId = newRC.getVirtualColumnId();

			/*
			** Set the GB aggregrate result column to
			** point to this.  The GB aggregate result
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
            newColumnRef = aggregate.getGeneratedRef();
            newColumnRef.setSource(tmpRC);

			/*
			** AGG INPUT: Create a ResultColumn in the bottom
			** project restrict that has the expression that is
			** to be aggregated
			*/
            newRC = aggregate.getNewExpressionResultColumn(dd);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            aggInputVColId = newRC.getVirtualColumnId();
            aggResultRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##window expression",
                aggregate.getNewNullResultExpression(),
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
            newRC = aggregate.getNewAggregatorResultColumn(dd);
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
                aggregate.getAggregateName(),
                aggregate.getAggregatorClassName(),
                aggInputVColId - 1,            // aggregate input column
                aggResultVColId - 1,            // the aggregate result column
                aggregatorVColId - 1,        // the aggregator column
                aggregate.isDistinct(),
                lf.getResultDescription(aggRCL.makeResultDescriptors(), "SELECT")
            ));
            this.processedAggregates.add(aggregate.getWrappedAggregate());
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
            return "singleInputRowOptimization: " + singleInputRowOptimization + "\n" +
                super.toString();
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
    public void generate(ActivationClassBuilder acb,
                         MethodBuilder mb)
        throws StandardException {
        int orderingItem = 0;
        int aggInfoItem = 0;
        FormatableArrayHolder orderingHolder;

		/* Get the next ResultSet#, so we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
        assignResultSetNumber();

        // Get the final cost estimate from the child.
        costEstimate = childResult.getFinalCostEstimate();

		/*
		** Get the column ordering for the sort.  Note that
		** for a scalar aggegate we may not have any ordering
		** columns (if there are no distinct aggregates).
		** WARNING: if a distinct aggregate is passed to
		** SortResultSet it assumes that the last column
		** is the distinct one.  If this assumption changes
		** then SortResultSet will have to change.
		*/
        orderingHolder = acb.getColumnOrdering(wdn.getOrderByList());
        if (addDistinctAggregate) {
            orderingHolder = acb.addColumnToOrdering(
                orderingHolder,
                addDistinctAggregateColumnNum);
        }

        if (SanityManager.DEBUG) {
            if (SanityManager.DEBUG_ON("WindowTrace")) {
                StringBuilder s = new StringBuilder();

                s.append("Partition column ordering is (");
                org.apache.derby.iapi.store.access.ColumnOrdering[] ordering =
                    (org.apache.derby.iapi.store.access.ColumnOrdering[]) orderingHolder.getArray(org.apache.derby
                                                                                                      .iapi.store
                                                                                                      .access
                                                                                                      .ColumnOrdering
                                                                                                      .class);

                for (int i = 0; i < ordering.length; i++) {
                    s.append(ordering[i].getColumnId());
                    s.append(" ");
                }
                s.append(")");
                SanityManager.DEBUG("AggregateTrace", s.toString());
            }
        }

        orderingItem = acb.addItem(orderingHolder);

		/*
		** We have aggregates, so save the aggInfo
		** struct in the activation and store the number
		*/
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(aggInfo != null,
                                 "aggInfo not set up as expected");
        }
        aggInfoItem = acb.addItem(aggInfo);

        acb.pushGetResultSetFactoryExpression(mb);


		/* Generate the WindowResultSet:
		 *	arg1: childExpress - Expression for childResult
		 *  arg2: isInSortedOrder - true if source result set in sorted order
		 *  arg3: aggregateItem - entry in saved objects for the aggregates,
		 *  arg4: orderItem - entry in saved objects for the ordering
		 *  arg5: Activation
		 *  arg6: rowAllocator - method to construct rows for fetching
		 *			from the sort
		 *  arg7: row size
		 *  arg8: resultSetNumber
		 *  arg9: isRollup
		 */
        // Generate the child ResultSet
        childResult.generate(acb, mb);
        mb.push(isInSortedOrder);
        mb.push(aggInfoItem);
        mb.push(orderingItem);

        resultColumns.generateHolder(acb, mb);

        mb.push(resultColumns.getTotalColumnSize());
        mb.push(resultSetNumber);
        mb.push(costEstimate.rowCount());
        mb.push(costEstimate.getEstimatedCost());

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getWindowResultSet",
                      ClassName.NoPutResultSet, 9);

    }

    ///////////////////////////////////////////////////////////////
    //
    // UTILITIES
    //
    ///////////////////////////////////////////////////////////////

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
     * Consider any optimizations after the optimizer has chosen a plan.
     * Optimizations include:
     * o  min optimization for scalar aggregates
     * o  max optimization for scalar aggregates
     *
     * @param selectHasPredicates true if SELECT containing this
     *                            vector/scalar aggregate has a restriction
     * @throws StandardException on error
     */
    void considerPostOptimizeOptimizations(boolean selectHasPredicates)
        throws StandardException {
		/* Consider the optimization for min with asc index on that column or
		 * max with desc index on that column:
		 *	o  No group by
		 *  o  One of:
		 *		o  min/max(ColumnReference) is only aggregate && source is
		 *		   ordered on the ColumnReference
		 *		o  min/max(ConstantNode)
		 * The optimization of the other way around (min with desc index or
		 * max with asc index) has the same restrictions with the additional
		 * temporary restriction of no qualifications at all (because
		 * we don't have true backward scans).
		 */
        if (partition == null && windowFunctions.size() == 1) {
            // TODO: Again enforcing window functions limited to 1, why?
            AggregateNode an = (AggregateNode) windowFunctions.get(0);
            AggregateDefinition ad = an.getAggregateDefinition();
            if (ad instanceof MaxMinAggregateDefinition) {
                if (an.getOperand() instanceof ColumnReference) {
                    /* See if the underlying ResultSet tree
                     * is ordered on the ColumnReference.
                     */
                    ColumnReference[] crs = new ColumnReference[1];
                    crs[0] = (ColumnReference) an.getOperand();

                    Vector tableVector = new Vector();
                    boolean minMaxOptimizationPossible = isOrderedOn(crs, false, tableVector);
                    if (SanityManager.DEBUG) {
                        SanityManager.ASSERT(tableVector.size() <= 1, "bad number of FromBaseTables returned by " +
                            "isOrderedOn() -- " + tableVector.size());
                    }

                    if (minMaxOptimizationPossible) {
                        boolean ascIndex = true;
                        int colNum = crs[0].getColumnNumber();

                        /* Check if we have an access path, this will be
                         * null in a join case (See Beetle 4423,DERBY-3904)
                         */
                        AccessPath accessPath = getTrulyTheBestAccessPath();
                        if (accessPath == null ||
                            accessPath.getConglomerateDescriptor() == null ||
                            accessPath.getConglomerateDescriptor().
                                getIndexDescriptor() == null)
                            return;
                        IndexDescriptor id = accessPath.
                                                           getConglomerateDescriptor().
                                                           getIndexDescriptor();
                        int[] keyColumns = id.baseColumnPositions();
                        boolean[] isAscending = id.isAscending();
                        for (int i = 0; i < keyColumns.length; i++) {
                            /* in such a query: select min(c3) from
                             * tab1 where c1 = 2 and c2 = 5, if prefix keys
                             * have equality operator, then we can still use
                             * the index.  The checking of equality operator
                             * has been done in isStrictlyOrderedOn.
                             */
                            if (colNum == keyColumns[i]) {
                                if (!isAscending[i])
                                    ascIndex = false;
                                break;
                            }
                        }
                        FromBaseTable fbt = (FromBaseTable) tableVector.firstElement();
                        MaxMinAggregateDefinition temp = (MaxMinAggregateDefinition) ad;

                        /*  MAX   ASC      NULLABLE
                         *  ----  ----------
                         *  TRUE  TRUE      TRUE/FALSE  =  Special Last Key Scan (ASC Index Last key with null
                         *  skips)
                         *  TRUE  FALSE     TRUE/FALSE  =  JustDisableBulk(DESC index 1st key with null skips)
                         *  FALSE TRUE      TRUE/FALSE  = JustDisableBulk(ASC index 1st key)
                         *  FALSE FALSE     TRUE/FALSE  = Special Last Key Scan(Desc Index Last Key)
                         */

                        if (((!temp.isMax()) && ascIndex) ||
                            ((temp.isMax()) && !ascIndex)) {
                            fbt.disableBulkFetch();
                            singleInputRowOptimization = true;
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
                        else if (!selectHasPredicates &&
                            ((temp.isMax() && ascIndex) ||
                                (!temp.isMax() && !ascIndex))) {
                            fbt.disableBulkFetch();
                            fbt.doSpecialMaxScan();
                            singleInputRowOptimization = true;
                        }
                    }
                } else if (an.getOperand() instanceof ConstantNode) {
                    singleInputRowOptimization = true;
                }
            }
        }
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
