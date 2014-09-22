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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;
import org.apache.derby.impl.sql.execute.WindowFunctionInfo;
import org.apache.derby.impl.sql.execute.WindowFunctionInfoList;


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

    private WindowDefinitionNode wdn;

    /**
     * The list of all window functions in the query block
     * that contains this partition.
     */
    private Collection<WindowFunctionNode> windowFunctions;

    /**
     * The list of aggregate nodes we've processed as
     * window functions
     */
    private Vector<AggregateNode> processedAggregates = new Vector<AggregateNode>();

    /**
     * Information that is used at execution time to
     * process a window function.
     */
    private WindowFunctionInfoList windowInfoList;

    /**
     * The parent to the WindowResultSetNode.  We generate a ProjectRestrict
     * over the windowing node and parent is set to that node.
     */
    private FromTable parent;

    // Is the source in sorted order
    private boolean isInSortedOrder;

    private Vector aggregateResultColumns;

    private GroupByList	groupByList;

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
        Object groupByList,
        Object aggregateResultColumns,
        Object tableProperties,
        Object nestingLevel)
        throws StandardException {
        super.init(bottomPR, tableProperties);
        setLevel(((Integer) nestingLevel).intValue());
        /* Group by without aggregates gets xformed into distinct */
        this.windowFunctions = (Collection<WindowFunctionNode>) windowFunctions;
        this.groupByList = (GroupByList)groupByList;
        this.aggregateResultColumns = (Vector)aggregateResultColumns;
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(! this.windowFunctions.isEmpty(),
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

		/*
		** The first thing we do is put ourselves on
		** top of the SELECT.  The select becomes the
		** childResult.  So our RCL becomes its RCL (so
		** nodes above it now point to us).  Map our
		** RCL to its columns.
		*/
        addNewPRNode();

		/*
		** We have aggregates, so we need to add
		** an extra PRNode and we also have to muck around
		** with our trees a might.
         * Add the extra result columns required by the aggregates
         * to the result list.
		*/
        addNewColumnsForAggregation();

		/* We say that the source is never in sorted order if there is a distinct aggregate.
		 * (Not sure what happens if it is, so just skip it for now.)
		 * Otherwise, we check to see if the source is in sorted order on any permutation
		 * of the grouping columns.)
		 */
        Partition partition = this.wdn.getPartition();
        if (partition != null)
        {
            ColumnReference[] crs =
                new ColumnReference[partition.size()];

            // Now populate the CR array and see if ordered
            int glSize = partition.size();
            int index;
            for (index = 0; index < glSize; index++)
            {
                GroupByColumn gc =
                    (GroupByColumn) partition.elementAt(index);
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
                isInSortedOrder = childResult.isOrderedOn(crs, true, null);
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
     * Add a new PR node for aggregation.  Put the
     * new PR under the sort.
     *
     * @throws StandardException
     */
    private void addNewPRNode() throws StandardException {
		/*
		** The first thing we do is put ourselves on
		** top of the SELECT.  The select becomes the
		** childResult.  So our RCL becomes its RCL (so
		** nodes above it now point to us).  Map our
		** RCL to its columns.
		* This is done so that we can map parent PR columns to reference
		* our function rusult columns and to  any function result columns below us.
		*/
        this.parent = this;
        ResultColumnList newBottomRCL;
        newBottomRCL = childResult.getResultColumns().copyListAndObjects();
        resultColumns = childResult.getResultColumns();
        childResult.setResultColumns(newBottomRCL);

		/*
		** Get the new PR, put above this window result.
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
            this,       // child
            rclNew,     // new result columns
            null,       // havingClause,
            null,       // restriction list
            null,       // project subqueries
            null,       // having subqueries
            tableProperties,
            getContextManager());


		/*
        ** Reset the bottom RCL to be empty. We'll rebuild them when adding function columns.
		*/
        childResult.setResultColumns((ResultColumnList) getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN_LIST,
                                                                                 getContextManager()));

        /*
         * Set the Windowing RCL to be empty. We'll rebuild them when adding function columns.
         */
        resultColumns = (ResultColumnList) getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN_LIST,
                                                                    getContextManager());
    }

    /**
     * Add a whole slew of columns needed for
     * aggregation. Basically, for each aggregate we add at least
     * 3 columns: the aggregate input expression(s) (ranking functions accept many)
     * and the aggregator column and a column where the aggregate
     * result is stored.  The input expressions are
     * taken directly from the aggregator node.  The aggregator
     * is the run time aggregator.  We add it to the RC list
     * as a new object coming into the sort node.
     * <p/>
     * At this point this is invoked, we have the following
     * tree:
     * <pre>
     * PR - (PARENT): RCL is the original select list
     * |
     * PR - PARTITION:  RCL is empty
     * |
     * PR - FROM TABLE: RCL is empty
     * </pre>
     * For each ColumnReference in PR RCL <UL>
     * <LI> clone the ref </LI>
     * <LI> create a new RC in the bottom RCL and set it
     * to the col ref </LI>
     * <LI> create a new RC in the partition RCL and set it to
     * point to the bottom RC </LI>
     * <LI> reset the top PR ref to point to the new partition
     * RCL</LI></UL>
     * <p/>
     * For each aggregate in windowFunctions <UL>
     * <LI> create RC in FROM TABLE.  Fill it with
     * aggs Operator.
     * <LI> create RC in FROM TABLE for agg result</LI>
     * <LI> create RC in FROM TABLE for aggregator</LI>
     * <LI> create RCs in PARTITION for agg input, set them
     * to point to FROM TABLE RC </LI>
     * <LI> create RC in PARTITION for agg result</LI>
     * <LI> create RC in PARTITION for aggregator</LI>
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
     * followed by at least 3 RC's for each aggregate in this order: the final computed
     * aggregate value (aggregate result), the aggregate input columns and a column for
     * the aggregator function itself.
     * <p/>
     * The Aggregator function puts the results in the first column of the RC's
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
     * fair amount of patching up is needed to account for generated partition columns.
     *
     * @throws StandardException
     */
    private void addNewColumnsForAggregation()
        throws StandardException {
        windowInfoList = new WindowFunctionInfoList();

        addUnAggColumns();
        addAggregateColumns();
    }

    /**
     * In the query rewrite for partition, add the columns on which we are doing
     * the partition.
     *
     * @see #addNewColumnsForAggregation
     */
    private void addUnAggColumns() throws StandardException {
        // note bottomRCL is a reference to childResult resultColumns
        // setting columns on bottomRCL will also set them on childResult resultColumns
        ResultColumnList bottomRCL = childResult.getResultColumns();
        // note windowingRCL is a reference to resultColumns
        // setting columns on windowingRCL will also set them on resultColumns
        ResultColumnList windowingRCL = resultColumns;
        List<SubstituteExpressionVisitor> referencesToSubstitute = new ArrayList<SubstituteExpressionVisitor>();

        // Add all columns from groupByList
        if (groupByList != null) {
            for (int i = 0; i < groupByList.size(); ++i) {
                {
                    GroupByColumn gbc = (GroupByColumn) groupByList.elementAt(i);
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
                    windowingRCL.addElement(gbRC);
                    gbRC.markGenerated();
                    gbRC.bindResultColumnToExpression();
                    gbRC.setVirtualColumnId(windowingRCL.size());

                    /*
                     ** Reset the original node to point to the
                     ** Group By result set.
                     */
                    VirtualColumnNode vc = (VirtualColumnNode) getNodeFactory().getNode(
                            C_NodeTypes.VIRTUAL_COLUMN_NODE,
                            this, // source result set.
                            gbRC,
                            new Integer(windowingRCL.size()),
                            getContextManager());

                    ValueNode vn = gbc.getColumnExpression();
                    SubstituteExpressionVisitor vis =
                            new SubstituteExpressionVisitor(vn, vc,
                                    AggregateNode.class);
                    referencesToSubstitute.add(vis);
                    gbc.setColumnPosition(bottomRCL.size());
                }
            }
        }
        if (aggregateResultColumns != null) {
            // Include grouped aggregate result columns in the result column list
            for (int i = 0; i < aggregateResultColumns.size(); ++i) {
                ResultColumn rc = (ResultColumn) aggregateResultColumns.get(i);
                ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(
                        C_NodeTypes.RESULT_COLUMN,
                        "##UnWindowingColumn" + rc.getExpression().getColumnName(),
                        rc.getExpression(),
                        getContextManager());

                // add this result column to the bottom rcl
                bottomRCL.addElement(newRC);
                newRC.markGenerated();
                newRC.bindResultColumnToExpression();
                newRC.setVirtualColumnId(bottomRCL.size());

                // now add this column to the windowingRCL
                ResultColumn wRC = (ResultColumn) getNodeFactory().getNode(
                        C_NodeTypes.RESULT_COLUMN,
                        "##UnWindowingColumn" + rc.getExpression().getColumnName(),
                        rc.getExpression(),
                        getContextManager());
                windowingRCL.addElement(wRC);
                wRC.markGenerated();
                wRC.bindResultColumnToExpression();
                wRC.setVirtualColumnId(windowingRCL.size());

                VirtualColumnNode vc = (VirtualColumnNode) getNodeFactory().getNode(
                        C_NodeTypes.VIRTUAL_COLUMN_NODE,
                        this, // source result set.
                        wRC,
                        new Integer(windowingRCL.size()),
                        getContextManager());

                ValueNode vn = rc.getExpression();
                SubstituteExpressionVisitor vis =
                        new SubstituteExpressionVisitor(vn, vc,
                                AggregateNode.class);
                referencesToSubstitute.add(vis);

            }
        }

        Collection<OrderedColumn> allColumns  =
            collectColumns(parent.getResultColumns(), wdn.getPartition(), wdn.getOrderByList());


        for (OrderedColumn oc : allColumns) {
            ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##UnWindowingColumn" + oc.getColumnExpression().getColumnName(),
                oc.getColumnExpression(),
                getContextManager());

            // add this result column to the bottom rcl
            bottomRCL.addElement(newRC);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            newRC.setVirtualColumnId(bottomRCL.size());

            // now add this column to the windowingRCL
            ResultColumn wRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##UnWindowingColumn" + oc.getColumnExpression().getColumnName(),
                oc.getColumnExpression(),
                getContextManager());
            windowingRCL.addElement(wRC);
            wRC.markGenerated();
            wRC.bindResultColumnToExpression();
            wRC.setVirtualColumnId(windowingRCL.size());

            /*
             ** Reset the original node to point to the
             ** Group By result set.
             */
            VirtualColumnNode vc = (VirtualColumnNode) getNodeFactory().getNode(
                C_NodeTypes.VIRTUAL_COLUMN_NODE,
                this, // source result set.
                wRC,
                windowingRCL.size(),
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
                new SubstituteExpressionVisitor(vn, vc, AggregateNode.class);
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

        Comparator<SubstituteExpressionVisitor> sorter = new ExpressionSorter();
        Collections.sort(referencesToSubstitute, sorter);
        for (SubstituteExpressionVisitor aReferencesToSubstitute : referencesToSubstitute)
            parent.getResultColumns().accept(aReferencesToSubstitute);
    }

    /**
     * In the query rewrite involving aggregates, add the columns for
     * aggregation.
     *
     * @see #addNewColumnsForAggregation
     */
    private void addAggregateColumns() throws StandardException {
        LanguageFactory lf = getLanguageConnectionContext().getLanguageFactory();
        DataDictionary dd = getDataDictionary();
        // note bottomRCL is a reference to childResult resultColumns
        // setting columns on bottomRCL will also set them on childResult resultColumns
        ResultColumnList bottomRCL = childResult.getResultColumns();
        // note windowingRCL is a reference to resultColumns
        // setting columns on windowingRCL will also set them on resultColumns
        ResultColumnList windowingRCL = resultColumns;

		/*
		 ** Now process all of the functions.  Replace
		 ** every windowFunctionNode with an RC.  We toss out
		 ** the list of RCs, we need to get each RC
		 ** as we process its corresponding windowFunctionNode.
		 */
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
        for (WindowFunctionNode windowFunctionNode : windowFunctions) {
			/*
			** AGG RESULT: Set the windowFunctionNode result to null in the
			** bottom project restrict.
			*/
            ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##WindowResult",
                windowFunctionNode.getNewNullResultExpression(),
                getContextManager());
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            int aggResultVColId = newRC.getVirtualColumnId();

			/*
			** Set the partition windowFunctionNode result column to
			** point to this.  The partition windowFunctionNode result
			** was created when we called ReplaceAggregatesWithCRVisitor()
			*/
            ColumnReference newColumnRef = (ColumnReference) getNodeFactory().getNode(
                C_NodeTypes.COLUMN_REFERENCE,
                newRC.getName(),
                null,
                getContextManager());
            newColumnRef.setSource(newRC);
            newColumnRef.setNestingLevel(this.getLevel());
            newColumnRef.setSourceLevel(this.getLevel());
            ResultColumn tmpRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                newRC.getColumnName(),
                newColumnRef,
                getContextManager());
            tmpRC.markGenerated();
            tmpRC.bindResultColumnToExpression();
            windowingRCL.addElement(tmpRC);
            tmpRC.setVirtualColumnId(windowingRCL.size());

			/*
			** Set the column reference to point to
			** this.
			*/
            newColumnRef = windowFunctionNode.getGeneratedRef();
            newColumnRef.setSource(tmpRC);

			/*
			** AGG INPUT(s): Create ResultColumns in the bottom
			** project restrict that have the expressions
			** to be aggregated
			*/
            ResultColumn[] expressionResults = windowFunctionNode.getNewExpressionResultColumns(dd);
            int[] inputVColIDs = new int[expressionResults.length];
            int i = 0;
            for (ResultColumn resultColumn : expressionResults) {
                resultColumn.markGenerated();
                resultColumn.bindResultColumnToExpression();
                bottomRCL.addElement(resultColumn);
                resultColumn.setVirtualColumnId(bottomRCL.size());
                inputVColIDs[i++] = resultColumn.getVirtualColumnId();

                /*
                ** Add a reference to this column into the
                ** partition RCL.
                */
                tmpRC = getColumnReference(resultColumn, dd);
                windowingRCL.addElement(tmpRC);
                tmpRC.setVirtualColumnId(windowingRCL.size());
            }

			/*
			** AGGREGATOR: Add a getAggregator method call
			** to the bottom result column list.
			*/
            newRC = windowFunctionNode.getNewAggregatorResultColumn(dd);
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            int aggregatorVColId = newRC.getVirtualColumnId();

            /*
			** Create an aggregator result expression
             */
            ResultColumn aggResultRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##WindowExpression",
                windowFunctionNode.getNewNullResultExpression(),
                getContextManager());

			/*
			** Add a reference to this column in the partition RCL.
			*/
            tmpRC = getColumnReference(newRC, dd);
            windowingRCL.addElement(tmpRC);
            tmpRC.setVirtualColumnId(windowingRCL.size());

			/*
			** Piece together a fake one column rcl that we will use
			** to generate a proper result description for input
			** to this agg if it is a user agg.
			*/
            ResultColumnList aggRCL = (ResultColumnList) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN_LIST,
                getContextManager());
            aggRCL.addElement(aggResultRC);

			/*
			** Note that the column ids in the row are 1-based
			*/
            windowInfoList.addElement(new WindowFunctionInfo(
                windowFunctionNode.getAggregateName(),
                windowFunctionNode.getAggregatorClassName(),
                inputVColIDs,       // windowFunctionNode input columns
                aggResultVColId,    // the windowFunctionNode result column
                aggregatorVColId,   // the aggregator column
                lf.getResultDescription(aggRCL.makeResultDescriptors(), "SELECT"),
                createColumnOrdering(windowFunctionNode.getWindow().getPartition()),
                createColumnOrdering(windowFunctionNode.getWindow().getOrderByList()),
                windowFunctionNode.getWindow().getFrameExtent().toMap()
            ));
            this.processedAggregates.add(windowFunctionNode.getWrappedAggregate());
        }
    }

    /**
     * Window functions differ from group by in that not all columns in a select clause
     * need be listed in the partition clause.  We need to collect and pull up all columns
     * in this query so that we can "project" the complete result.  This is done prior to
     * creating the "aggregate" result, which includes the aggregate function, its input
     * columns and its output column.
     *
     * @param resultColumns all result columns projected from below the window function
     * @param partition columns listed in partition clause
     * @param orderByList columns listed in order by clause
     * @return all columns in the query
     */
    private List<OrderedColumn> collectColumns(ResultColumnList resultColumns, Partition partition, OrderByList orderByList) {
        int partitionSize = (partition != null ? partition.size() : 0);
        int oderbySize = (orderByList != null? orderByList.size():0);

        // ordered list of partition and order by columns
        List<OrderedColumn> colSet = new ArrayList<OrderedColumn>(partitionSize + oderbySize + resultColumns.size());
        // Set of column name to quickly check containment
        Set<String> names = new HashSet<String>(partitionSize + oderbySize);

        // partition - we need all of these columns
        if (partition != null) {
            for (int i=0; i<partitionSize; ++i) {
                OrderedColumn pCol = (OrderedColumn) partition.elementAt(i);
                colSet.add(pCol);
                names.add(pCol.getColumnExpression().getColumnName());
            }
        }

        // order by - we add order by columns to the end so rows will be ordered by
        // [P1,P2,..][O1,O2,...], where Pn are partition columns and On are order by.
        if (orderByList != null) {
            for (int i=0; i<orderByList.size(); ++i) {
                OrderedColumn oCol = (OrderedColumn) orderByList.elementAt(i);
                colSet.add(oCol);
                names.add(oCol.getColumnExpression().getColumnName());
            }
        }

        // remaining columns from select, preserve their result ordering
        List<OrderedColumn> allColumns = new ArrayList<OrderedColumn>(partitionSize + oderbySize + resultColumns.size());
        for (int i=0; i<resultColumns.size(); i++) {
            ResultColumn aCol = (ResultColumn) resultColumns.elementAt(i);
            BaseColumnNode baseColumnNode = aCol.getBaseColumnNode();
            if (baseColumnNode != null) {
                ColumnReference node = null;
                if (aCol.getExpression() instanceof ColumnReference) {
                    node = (ColumnReference) aCol.getExpression();
                } else if (aCol.getExpression() instanceof VirtualColumnNode) {
                    // if VirtualColumnNode, getSourceColumn():ResultColumn.getExpression():ColumnReference
                    ResultColumn sourceColumn = ((VirtualColumnNode)aCol.getExpression()).getSourceColumn();
                    if (sourceColumn != null && sourceColumn.getExpression() instanceof ColumnReference) {
                        node = (ColumnReference) sourceColumn.getExpression();
                    }
                }
                if (node != null && ! names.contains(baseColumnNode.getColumnName())) {
                    // create fake OrderedColumn to fit into calling code
                    GroupByColumn gbc = new GroupByColumn();
                    gbc.init(node);
                    allColumns.add(gbc);
                }
            }
        }

        // add partition and order by columns to END of the list in order to preserve
        // natural result ordering from child projection
        allColumns.addAll(colSet);
        return allColumns;
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

    public String getFunctionNames() {
        StringBuilder buf= new StringBuilder();
        for (WindowFunctionInfo info : windowInfoList) {
            buf.append(info.getFunctionName()).append(',');
        }
        if (buf.length() > 0) {
            buf.setLength(buf.length()-1);
        }
        return buf.toString();
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        if (SanityManager.DEBUG) {
            StringBuilder buf= new StringBuilder("functions: ");
            for (WindowFunctionInfo info : windowInfoList) {
                buf.append('\n').append(info.toString());
            }
            buf.append("\nWindowDefinition: ").append(wdn.toString()).append(super.toString());
            return buf.toString();
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

            printLabel(depth, "windowFunction:\n");
            int i = 0;
            for (AggregateNode agg : windowFunctions) {
                debugPrint(formatNodeString("[" + i++ + "]:", depth + 1));
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
        childResult = childResult.optimize(
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
                null);

        // RESOLVE: NEED TO FACTOR IN COST OF SORTING AND FIGURE OUT HOW
        // MANY ROWS HAVE BEEN ELIMINATED.
        costEstimate = optimizer.newCostEstimate();

        costEstimate.setCost(childResult.getCostEstimate().getEstimatedCost(),
                             childResult.getCostEstimate().rowCount(),
                             childResult.getCostEstimate().singleScanRowCount());

        return this;
    }

    public ResultColumnDescriptor[] makeResultDescriptors() {
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
        Partition partition = this.wdn.getPartition();
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
		** We have aggregates, so save the windowInfoList
		** in the activation and store the number
		*/
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(windowInfoList != null, "windowInfoList not set up as expected");
        }
        int aggInfoItemIndex = acb.addItem(windowInfoList);

        acb.pushGetResultSetFactoryExpression(mb);


		/* Generate the WindowResultSet:
		 *	arg1: childExpress - Expression for childResult
		 *  arg2: isInSortedOrder - true if source result set in sorted order
		 *  arg3: aggInfoItem - entry in saved objects for the aggregates,
		 *  arg4: Activation
		 *  arg5: number of columns in the result
		 *  arg6: resultSetNumber
		 *  arg7: row count estimate for cost
		 *  arg8: estimated cost
		 */
        // Generate the child ResultSet
        childResult.generate(acb, mb);
        mb.push(isInSortedOrder);
        mb.push(aggInfoItemIndex);

        resultColumns.generateHolder(acb, mb);

        mb.push(resultColumns.getTotalColumnSize());
        mb.push(resultSetNumber);
        mb.push(costEstimate.rowCount());
        mb.push(costEstimate.getEstimatedCost());

        mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "getWindowResultSet",
                      ClassName.NoPutResultSet, 8);

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
                int j = 0;
                for (OrderedColumn orderedColumn : orderedColumnLists[i]) {
                    map.put(j++, orderedColumn);
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
     * @param    dd the data dictionary
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
    private static final class ExpressionSorter implements Comparator<SubstituteExpressionVisitor> {
        public int compare(SubstituteExpressionVisitor o1, SubstituteExpressionVisitor o2) {
            try {
                ValueNode v1 = o1.getSource();
                ValueNode v2 = o2.getSource();
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
