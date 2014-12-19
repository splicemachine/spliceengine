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

import static org.apache.derby.impl.sql.compile.WindowColumnMapping.findMissingKeyNodes;
import static org.apache.derby.impl.sql.compile.WindowColumnMapping.operandMissing;
import static org.apache.derby.impl.sql.compile.WindowColumnMapping.replaceColumnExpression;
import static org.apache.derby.impl.sql.compile.WindowColumnMapping.resetOverColumnsPositionByKey;
import static org.apache.derby.impl.sql.compile.WindowColumnMapping.resetOverColumnsPositionByParent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Vector;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.LanguageFactory;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.NodeFactory;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
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
        this.windowFunctions = (Collection<WindowFunctionNode>) windowFunctions;
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(!this.windowFunctions.isEmpty(),
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
        List<OrderedColumn> keyColumns = this.wdn.getKeyColumns();
        ColumnReference[] crs = new ColumnReference[keyColumns.size()];

        // Now populate the CR array and see if ordered
        int glSize = keyColumns.size();
        int index = 0;
        for (OrderedColumn aCol : keyColumns) {
            if (aCol.getColumnExpression() instanceof ColumnReference) {
                crs[index] = (ColumnReference) aCol.getColumnExpression();
            } else {
                isInSortedOrder = false;
                break;
            }
            index++;
        }
        if (index == glSize) {
            isInSortedOrder = childResult.isOrderedOn(crs, true, null);
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
		* our function result columns and to any function result columns below us.
		*/
        this.parent = this;
        ResultColumnList newBottomRCL = childResult.getResultColumns().copyListAndObjects();
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

        WindowColumnMapping columnMapping = addUnAggColumns();
        appendMissingKeyColumns(columnMapping);
        addAggregateColumns(columnMapping);
    }

    /**
     * Pull parent columns down into this result node and the one below us.
     *
     * @see #addNewColumnsForAggregation
     */
    private WindowColumnMapping addUnAggColumns() throws StandardException {
        // note bottomRCL is a reference to childResult resultColumns
        // setting columns on bottomRCL will also set them on childResult resultColumns
        ResultColumnList bottomRCL = childResult.getResultColumns();
        // note windowingRCL is a reference to resultColumns
        // setting columns on windowingRCL will also set them on resultColumns
        ResultColumnList windowingRCL = resultColumns;
        List<SubstituteExpressionVisitor> referencesToSubstitute = new ArrayList<SubstituteExpressionVisitor>();

        // Add all referenced columns (CRs) and virtual column (VCNs) in select list to windowing node's RCL
        // and substitute references in original node to point to the Windowing
        // result set. (modelled on GroupByNode's action for addUnAggColumns)
        // VCNs happen, for example, when we have a window over a group by.
        WindowColumnMapping columnMapping = new WindowColumnMapping(parent.getResultColumns(),
                                                                    wdn.getOverColumns());

        for (WindowColumnMapping.ParentRef parentRef : columnMapping.getParentColumns()) {
            if (parentRef.ref instanceof WindowFunctionNode) {
                // we handle window functions in another method
                continue;
            }

            if (parentRef.ref instanceof BinaryOperatorNode) {
                if (! (((BinaryOperatorNode)parentRef.ref).getRightOperand() instanceof WindowFunctionNode)) {
                    createUnWindowingColumn(bottomRCL, windowingRCL, referencesToSubstitute,
                                            ((BinaryOperatorNode)parentRef.ref).getRightOperand());
                }
                if (! (((BinaryOperatorNode)parentRef.ref).getLeftOperand() instanceof WindowFunctionNode)) {
                    createUnWindowingColumn(bottomRCL, windowingRCL, referencesToSubstitute,
                                            ((BinaryOperatorNode)parentRef.ref).getLeftOperand());
                }
            } else {
                createUnWindowingColumn(bottomRCL, windowingRCL, referencesToSubstitute, parentRef, parentRef.ref);
            }
        }
        // Make RC -> VC substitutions in parent
        Comparator<SubstituteExpressionVisitor> sorter = new ExpressionSorter();
        Collections.sort(referencesToSubstitute, sorter);
        for (SubstituteExpressionVisitor aReferencesToSubstitute : referencesToSubstitute) {
            parent.getResultColumns().accept(aReferencesToSubstitute);
        }

        return columnMapping;
    }

    private void createUnWindowingColumn(ResultColumnList bottomRCL, ResultColumnList windowingRCL,
                                         List<SubstituteExpressionVisitor> referencesToSubstitute,
                                         ValueNode pRef)
        throws StandardException {
        createUnWindowingColumn(bottomRCL, windowingRCL, referencesToSubstitute, null, pRef);
    }

    private void createUnWindowingColumn(ResultColumnList bottomRCL, ResultColumnList windowingRCL,
                                         List<SubstituteExpressionVisitor> referencesToSubstitute,
                                         WindowColumnMapping.ParentRef parentRef, ValueNode pRef)
        throws StandardException {
        String name = (parentRef != null ? parentRef.getName() : pRef.getColumnName());
        ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(
            C_NodeTypes.RESULT_COLUMN,
            "##UnWinColumn_" + name,
            pRef,
            getContextManager());

        // add this result column to the bottom rcl
        bottomRCL.addElement(newRC);
        newRC.markGenerated();
        newRC.bindResultColumnToExpression();
        newRC.setVirtualColumnId(bottomRCL.size());

        // now add this column to the windowingRCL
        ResultColumn wRC = (ResultColumn) getNodeFactory().getNode(
            C_NodeTypes.RESULT_COLUMN,
            "##UnWinColumn_" + name,
            pRef,
            getContextManager());
        windowingRCL.addElement(wRC);
        wRC.markGenerated();
        wRC.bindResultColumnToExpression();
        int columnPosition = windowingRCL.size();
        wRC.setVirtualColumnId(columnPosition);

        // reset the partition and/or order by column's column position in the
        // Window result since we're rearranging here
        if (parentRef != null) {
            // if parentRef == null, we're not dealing with a column among the OVER() columns
            resetOverColumnsPositionByParent(parentRef, wdn.getOverColumns(), columnPosition);
        }

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
        SubstituteExpressionVisitor vis =
            new SubstituteExpressionVisitor(pRef, vc, AggregateNode.class);
        referencesToSubstitute.add(vis);
    }

    /**
     * Find function key columns that were not referenced by parent columns (columns in the select clause).
     * The key columns are from the function's OVER() clause. The function uses these key columns to create
     * a "row key" in order to sort them by partition, etc, and, in the case of ranking functions, as function
     * input (operands).<br/>
     * We need to "pull up" these columns from a result node below us, possibly going all the way down to
     * the BaseTable. We pull them up into this WindowResultSetNode so they're accessible. We don't, however,
     * project these columns, since no node above has a need for them. If another, subsequent function has
     * a need for one, it will use this method to pull the column up from this node.
     * <p/>
     * Find the missing key columns in a result node below us and create new column references for them.<br/>
     * Reset the column position of key column and remap references to the newly created references.
     * @param columnMapping the column mapping describing parent select nodes and their child references.
     * @throws StandardException
     */
    private void appendMissingKeyColumns(WindowColumnMapping columnMapping) throws StandardException {
        // note bottomRCL is a reference to childResult resultColumns
        // setting columns on bottomRCL will also set them on childResult resultColumns
        ResultColumnList bottomRCL = childResult.getResultColumns();
        // note windowingRCL is a reference to resultColumns
        // setting columns on windowingRCL will also set them on resultColumns
        ResultColumnList windowingRCL = resultColumns;

        // Add any over() (key) columns that were missing from the select
        for (OrderedColumn keyCol : findMissingKeyNodes(columnMapping.getParentColumns(), wdn.getKeyColumns())) {
            ResultColumn source = null;
            boolean isAggReplacement = keyCol.getColumnExpression() instanceof ColumnReference &&
                (((ColumnReference) keyCol.getColumnExpression()).getGeneratedToReplaceAggregate());
            if (! isAggReplacement) {
                // Don't look down for agg function results -- "source" is already pointing to the right place.
                source = recursiveSourcePuller(childResult, keyCol.getColumnExpression());
            }
            if (source == null) {
                // Create and add result column to the bottom rcl
                ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    "##Key_" + keyCol.getColumnExpression().getColumnName(),
                    keyCol.getColumnExpression(),
                    getContextManager());
                bottomRCL.addElement(newRC);
                newRC.bindResultColumnToExpression();
                newRC.setVirtualColumnId(bottomRCL.size());

                source = newRC;
            }

            // Create an RC that we'll add to the windowing RCL
            ColumnReference tmpColumnRef = (ColumnReference) getNodeFactory().getNode(
                C_NodeTypes.COLUMN_REFERENCE,
                source.getName(),
                null,
                getContextManager());
            tmpColumnRef.setSource(source);
            tmpColumnRef.setNestingLevel(this.getLevel());
            tmpColumnRef.setSourceLevel(this.getLevel());

            // Create a CR and wrap it in just created RC
            ResultColumn wRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##Key_" + tmpColumnRef.getColumnName(),
                tmpColumnRef,
                getContextManager());
            wRC.bindResultColumnToExpression();

            // Add the RC (that wraps the CR) to the windowing RCL
            windowingRCL.addElement(wRC);
            int newColumnNumber = windowingRCL.size();
            wRC.setVirtualColumnId(newColumnNumber);

            // Reset the key column's column position in the Window result since we're rearranging here
            // This will change the function's operand column position so the function pulls its input
            // from the rearranged column
            resetOverColumnsPositionByKey(keyCol, wdn.getOverColumns(), newColumnNumber);
            if (! isAggReplacement) {
                // Since we're creating new column references here, we need to reset the reference to
                // any matching operand so that the function references the new column.
                // Note, we don't do this for CRs that have been generated to replace an agg function
                // because the function operand that references it is already pointing to the generated
                // ref.
                replaceColumnExpression(keyCol, wRC.getExpression(), wdn.getOverColumns());
            }

        }
    }

    int counter = 0;
    private ResultColumn recursiveSourcePuller(ResultSetNode childResultSetNode, ValueNode expression) throws StandardException {
        if (childResultSetNode instanceof FromBaseTable) {
            // we've gone too far
            return null;
        }
        counter = 0;
        Pair rcToChildRcl = findMatchingResultColumn(expression, childResultSetNode);

        ResultColumn childRC = rcToChildRcl.rc;
        if (childRC == null) {
            childRC = recursiveSourcePuller(((SingleChildResultSetNode)childResultSetNode).getChildResult(), expression);
        } else {
            return childRC;
        }

        ResultColumn source = null;
        if (childRC != null) {
            ResultColumnList childResultList = rcToChildRcl.childRcl;
            // Create an RC->CR for matching column that we'll add to the parent RCL upon return
            ColumnReference tmpColumnRef = (ColumnReference) getNodeFactory().getNode(
                C_NodeTypes.COLUMN_REFERENCE,
                childRC.getName(),
                null,
                getContextManager());
            tmpColumnRef.setSource(childRC);
            tmpColumnRef.setNestingLevel(((FromTable) childResultSetNode).getLevel());
            tmpColumnRef.setSourceLevel(((FromTable)childResultSetNode).getLevel());

            // Create a CR and wrap it in just created RC and return it so that parent
            // can add it to its RCL
            ResultColumn parentRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "#"+childRC.getName()+"_RC"+counter++,
                tmpColumnRef,
                getContextManager());
            parentRC.bindResultColumnToExpression();

            // Add the RC (that wraps the CR) to the this child's RCL
            childResultList.addElement(parentRC);
            parentRC.setVirtualColumnId(childResultList.size());

            source = parentRC;
        }

        return source;
    }

    private Pair findMatchingResultColumn(ValueNode expression, ResultSetNode childResultSetNode) throws StandardException {
        ResultColumnList childResultList = childResultSetNode.getResultColumns();

        if (expression instanceof ColumnReference || expression instanceof VirtualColumnNode) {
            // we can only find these instances

            // Check for match with all RCs of this result set node's RCL
            ResultColumn matchedRc = null;
            for (int i = 0; i < childResultList.size(); i++) {
                ResultColumn rc = childResultList.elementAt(i);
                if (!rc.isGenerated()) {
                    // generated nodes are ones we've already created
                    ColumnDescriptor rcDescriptor = rc.getTableColumnDescriptor();
                    if (rcDescriptor == null) {
                        // Try to wrangle a ColumnDescriptor -- not all nodes have one
                        ValueNode exp = rc.getExpression();
                        if (exp instanceof ResultColumn) {
                            rcDescriptor = ((ResultColumn) exp).getTableColumnDescriptor();
                        } else if (exp instanceof VirtualColumnNode &&
                            exp.getSourceResultColumn().getTableColumnDescriptor() == null) {
                            // DB-2170, WF over a view, has RC->VCN->RC->VCN->...->JavaToSQLValueNode and
                            // all RCs had null TableColumnDescriptors. Have to get creative (and approximate)
                            if (rc.columnTypeAndLengthMatch(expression.getSourceResultColumn()) &&
                                 rc.columnNameMatches(expression.getSourceResultColumn().exposedName)) {
                                matchedRc = rc;
                            }
                        } else {
                            rcDescriptor = exp.getSourceResultColumn().getTableColumnDescriptor();
                        }
                    }

                    if (rcDescriptor != null) {
                        if (expression instanceof ColumnReference) {
                            ResultColumn expRC = ((ColumnReference) expression).getSource();
                            if (expRC != null) {
                                ValueNode rcExp = expRC.getExpression();
                                if (rcExp instanceof BaseColumnNode) {
                                    // can't use BaseColumnNode to get ColumnDescriptor -- sourceResultColumn is null
                                    if (rcDescriptor.getColumnName().equals(rcExp.getColumnName())) {
                                        // Found a match
                                        matchedRc = rc;
                                    }
                                } else if (rcDescriptor.equals(expression.getSourceResultColumn().getTableColumnDescriptor())) {
                                    // Found a match
                                    matchedRc = rc;
                                }
                            }
                        }
                    }
                }
                if (matchedRc != null) {
                    // return matchedRC
                    return Pair.newPair(matchedRc, childResultList);
                }
            }
        }
        return Pair.newPair(null,childResultList);
    }

    /**
     * In the query rewrite involving aggregates, add the columns for
     * aggregation.
     *
     * @see #addNewColumnsForAggregation
     */
    private void addAggregateColumns(WindowColumnMapping columnMapping) throws StandardException {
        LanguageFactory lf = getLanguageConnectionContext().getLanguageFactory();
        DataDictionary dd = getDataDictionary();
        // note bottomRCL is a reference to childResult resultColumns
        // setting columns on bottomRCL will also set them on childResult resultColumns
        ResultColumnList bottomRCL = childResult.getResultColumns();
        // note windowingRCL is a reference to resultColumns
        // setting columns on windowingRCL will also set them on resultColumns
        ResultColumnList windowingRCL = resultColumns;

        // Now process all of the functions.  As we process each windowFunctionNode replace the corresponding
        // windowFunctionNode in parent RCL with an RC whose expression resolves to the function's result column.
        // For each windowFunctionNode...
        for (WindowFunctionNode windowFunctionNode : windowFunctions) {
            // We don't handle window references here
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(
                    !(windowFunctionNode.getWindow() instanceof WindowReferenceNode),
                    "unresolved window-reference: " +
                        windowFunctionNode.getWindow().getName());
            }

			/*
			** AGG RESULT: Set the windowFunctionNode result to null in the
			** bottom project restrict.
			*/
            ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##" + windowFunctionNode.getAggregateName() + "Result",
                windowFunctionNode.getNewNullResultExpression(),
                getContextManager());
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            int aggResultVColId = newRC.getVirtualColumnId();

			/*
			** Set the windowFunctionNode result column to
			** point to this.
			*/
            ResultColumn tmpRC = createColumnReferenceWrapInResultColumn(newRC, getNodeFactory(),
                                                                         getContextManager(), this.getLevel());
            windowingRCL.addElement(tmpRC);
            tmpRC.setVirtualColumnId(windowingRCL.size());

			/*
			** Set the parent result column (or the appropriate node below it) to reference this tmpRC result column.
			*/
            replaceParentFunctionWithResultReference(parent.getResultColumns(), windowFunctionNode, tmpRC);

			/*
			** AGG INPUT(s): Create ResultColumns in the bottom
			** project restrict that have the expressions
			** to be aggregated
			*/
            // Create function references for all input operands
            ResultColumn[] expressionResults = windowFunctionNode.getNewExpressionResultColumns();
            int[] inputVColIDs = new int[expressionResults.length];
            int i = 0;
            for (ResultColumn resultColumn : expressionResults) {
                ValueNode exp = resultColumn.getExpression();
                if ((exp instanceof ColumnReference || exp instanceof VirtualColumnNode) &&
                    ! inSourceResult(exp, ((SingleChildResultSetNode)childResult).childResult) &&
                    operandMissing(exp, columnMapping.getParentColumns(), wdn.getKeyColumns())) {
                    // if operand is not below us and is missing from select list and keys,
                    // which have already been remapped, pull up column reference and replace operand
                    ResultColumn source = recursiveSourcePuller(((SingleChildResultSetNode)childResult).childResult,
                                                                exp);
                    resultColumn.setExpression(source.getExpression());
                }
                resultColumn.markGenerated();
                resultColumn.bindResultColumnToExpression();
                resultColumn.setName("##" + windowFunctionNode.getAggregateName() + "Input");
                bottomRCL.addElement(resultColumn);
                resultColumn.setVirtualColumnId(bottomRCL.size());
                inputVColIDs[i++] = resultColumn.getVirtualColumnId();

                /*
                ** Add a reference to this column into the
                ** windowing RCL.
                */
                tmpRC = createColumnReferenceWrapInResultColumn(resultColumn, getNodeFactory(),
                                                                getContextManager(), this.getLevel());
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
            newRC.setName("##" + windowFunctionNode.getAggregateName() + "Function");
            bottomRCL.addElement(newRC);
            newRC.setVirtualColumnId(bottomRCL.size());
            int aggregatorVColId = newRC.getVirtualColumnId();

			/*
			** Add a reference to this column in the windowing RCL.
			*/
            tmpRC = createColumnReferenceWrapInResultColumn(newRC, getNodeFactory(),
                                                            getContextManager(), this.getLevel());
            windowingRCL.addElement(tmpRC);
            tmpRC.setVirtualColumnId(windowingRCL.size());

            /*
			** Create an aggregator result expression
             */
            ResultColumn aggResultRC = (ResultColumn) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN,
                "##" + windowFunctionNode.getAggregateName() + "ResultExp",
                windowFunctionNode.getNewNullResultExpression(),
                getContextManager());

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
			** Note that the column ids here are all are 1-based
			*/
            FormatableArrayHolder partitionCols = createColumnOrdering(wdn.getPartition());
            FormatableArrayHolder orderByCols = createColumnOrdering(wdn.getOrderByList());
            FormatableArrayHolder keyCols = createColumnOrdering(wdn.getKeyColumns());
            windowInfoList.addElement(new WindowFunctionInfo(
                windowFunctionNode.getAggregateName(),
                windowFunctionNode.getAggregatorClassName(),
                inputVColIDs,       // windowFunctionNode input columns
                aggResultVColId,    // the windowFunctionNode result column
                aggregatorVColId,   // the aggregator column
                lf.getResultDescription(aggRCL.makeResultDescriptors(), "SELECT"),
                partitionCols,      // window function partition
                orderByCols,        // window function order by
                keyCols,            // deduplicated set of partition and order by cols that will be row key
                windowFunctionNode.getWindow().getFrameExtent().toMap()
            ));
            this.processedAggregates.add(windowFunctionNode);
        }
    }

    private boolean inSourceResult(ValueNode expression, ResultSetNode resultSetNode) throws StandardException {
        return findMatchingResultColumn(expression, resultSetNode).rc != null;
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


	/*
	 *  Optimizable interface
	 */

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
        StringBuilder buf = new StringBuilder();
        for (WindowFunctionInfo info : windowInfoList) {
            buf.append(info.getFunctionName()).append(',');
        }
        if (buf.length() > 0) {
            buf.setLength(buf.length() - 1);
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
            StringBuilder buf = new StringBuilder("functions: ");
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
     * Return whether or not the underlying ResultSet tree will return
     * a single row, at most.
     * This is important for join nodes where we can save the extra next
     * on the right side if we know that it will return at most 1 row.
     *
     * @return Whether or not the underlying ResultSet tree will return a single row.
     * @throws StandardException Thrown on error
     */
    public boolean isOneRowResultSet() throws StandardException {
        // Window functions are not scalar aggs
        return false;
    }

    /**
     * Optimize this WindowResultSetNode.
     *
     * @param dataDictionary The DataDictionary to use for optimization
     * @param predicates     The PredicateList to optimize.  This should
     *                       be a join predicate.
     * @param outerRows      The number of outer joining rows
     * @return ResultSetNode    The top of the optimized subtree
     * @throws StandardException Thrown on error
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
     * generate the sort result set operating over the source
     * resultset.  Adds distinct aggregates to the sort if
     * necessary.
     *
     * @throws StandardException Thrown on error
     */
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException {

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

    private void replaceParentFunctionWithResultReference(ResultColumnList parentResultCols, WindowFunctionNode
        functionNode, ResultColumn newResultColumn) throws StandardException {

        ValueNode replacedNode = null;
        for (int i=0; i<parentResultCols.size(); i++) {
            ResultColumn aParentCol = parentResultCols.getResultColumn(i+1);
            if (functionNode.equals(aParentCol.getExpression())) {
                replacedNode = functionNode.replaceCallWithColumnReference(aParentCol,
                                                                           ((FromTable) childResult).getTableNumber(),
                                                                           this.level,
                                                                           newResultColumn);
                break;
            } else if (aParentCol.getExpression() instanceof BinaryOperatorNode) {
                // TODO: what other skulduggery does Derby have in store in its plan trees?
                BinaryOperatorNode aron = (BinaryOperatorNode) aParentCol.getExpression();
                if (functionNode.equals(aron.getRightOperand())) {

                    replacedNode = functionNode.replaceCallWithColumnReference(null,
                                                                               ((FromTable) childResult).getTableNumber(),
                                                                               this.level,
                                                                               newResultColumn);
                    aron.setRightOperand(replacedNode);
                    break;
                }
                if (functionNode.equals(aron.getLeftOperand())) {

                    replacedNode = functionNode.replaceCallWithColumnReference(null,
                                                                               ((FromTable) childResult).getTableNumber(),
                                                                               this.level,
                                                                               newResultColumn);
                    aron.setLeftOperand(replacedNode);
                    break;
                }
            }
        }
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(replacedNode != null,
                                 "Failed to replace parent function reference; unresolved window function: " +
                                     functionNode.getName());
        }
    }

    /**
     * Create the one-based column ordering for an ordered column list and pack
     * it for shipping.
     *
     * @param orderedColumnList the list of ordered columns to pack.
     * @return The holder for the column ordering array suitable for shipping.
     */
    private FormatableArrayHolder createColumnOrdering(List<OrderedColumn> orderedColumnList) {
        FormatableArrayHolder colOrdering = null;
        if (orderedColumnList != null) {
            IndexColumnOrder[] ordering = new IndexColumnOrder[orderedColumnList.size()];
            int j = 0;
            for (OrderedColumn oc : orderedColumnList) {
                ordering[j++] = new IndexColumnOrder(oc.getColumnPosition(),
                                                     oc.isAscending(),
                                                     oc.isNullsOrderedLow());
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
     * @param targetRC the source
     * @param nodeFactory for creating nodes
     * @param contextManager for creating nodes
     * @param nodeLevel the level we're at in the tree    @return the new result column
     * @throws StandardException on error
     */
    public static ResultColumn createColumnReferenceWrapInResultColumn(ResultColumn targetRC,
                                                                       NodeFactory nodeFactory,
                                                                       ContextManager contextManager,
                                                                       int nodeLevel)
        throws StandardException {

        // create the CR using targetRC as the source and source name
        ColumnReference tmpColumnRef = (ColumnReference) nodeFactory.getNode(
            C_NodeTypes.COLUMN_REFERENCE,
            targetRC.getName(),
            null,
            contextManager);
        tmpColumnRef.setSource(targetRC);
        tmpColumnRef.setNestingLevel(nodeLevel);
        tmpColumnRef.setSourceLevel(nodeLevel);

        // create the RC, wrap the CR
        ResultColumn newRC = (ResultColumn) nodeFactory.getNode(
            C_NodeTypes.RESULT_COLUMN,
            tmpColumnRef.getColumnName(),
            tmpColumnRef,
            contextManager);
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

    private static class Pair {
        final ResultColumn rc;
        final ResultColumnList childRcl;

        private Pair(ResultColumn rc ,ResultColumnList childRcl){
            this.rc = rc;
            this.childRcl = childRcl;
        }

        public static Pair newPair(ResultColumn rc,ResultColumnList childRcl){
            return new Pair(rc,childRcl);
        }
    }

}
