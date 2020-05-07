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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.LanguageFactory;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.impl.sql.execute.IndexColumnOrder;
import com.splicemachine.db.impl.sql.execute.WindowFunctionInfo;
import com.splicemachine.db.impl.sql.execute.WindowFunctionInfoList;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * A WindowResultSetNode represents a result set for a window partitioning operation
 * on a select.  It has the same description as its input result set.<br/>
 * For the most part, it simply delegates operations to its bottomPRSet,
 * which is currently expected to be a ProjectRestrictResultSet generated for a SelectNode.
 * <p/>
 * NOTE: A WindowResultSetNode extends FromTable since it can exist in a FromList.
 * <p/>
 * Modelled on the code in GroupByNode but there are a few capabilities of WindowFunctions that
 * make it a little trickier to implement,
 * <ul>
 *     <li>window functions, as apposed to scalar aggregate functions, can have multiple arguments</li>
 *     <li>window function grouping columns (partition, order by) do not have to be included in the
 *     select clause</li>
 *     <li>it's not uncommon for there to be multiple window functions, some with the same window definition
 *     (same over clause) in one query</li>
 *     <li>the location of the RC where the window function result actually lives is not necessarily
 *     the same location where the function processes. For instance, we're here below the select node
 *     with one or more table scans somewhere below us (possibly thru some number of joins, grouped aggs,
 *     etc.), but our result may be exposed, not in an RC of our parent select, but by some sub-expression
 *     of it.  This expression does not have to be in the ResultSetNode parent/child hierarchy; just
 *     source of some expression in the RCL. An example is:
 *     <code>SELECT CASE WHEN (commission < 2000) THEN min(salary) over(PARTITION BY department)
 *     ELSE -1 END as minimum_sal FROM EMPLOYEES</code>. Although window function processing happens
 *     before the select, the window function result is exposed by a BinaryArithmeticOperatorNode, which is
 *     exposed as part of a ConditionNode, which is exposed by the SelectNode's RCL.</li>
 * </ul>
 * as well as Derby's ancient ways that make it difficult to transform an AST;
 * <ul>
 *     <li>since Derby code was written prior to Java 5 collections, the concept of equals() and hashCode()
 *     are absent</li>
 *     <li>tree nodes are mutable and are mutated during transformation making it useless to implement equals()
 *     and hashCode() anyway</li>
 *     <li>at some point there was an effort to create isEquivalent() methods in the hierarchy. "isEquivalent()"
 *     takes on different meanings; it can be anything from "represents the same object" (same type and all fields
 *     are the same), to "partially the same object" (same type and <i>some</i> of the fields are the same, to
 *     the instances are equal (object ==). Strangely, sometimes even expressions in the same data lineage
 *     are "! isEquivalent()" because not all the fields were copied or they are in different stages of their
 *     life cycles.</li>
 *     <li>object fields that you would like to use for object identification (a ResultColumn's ColumnDefinition
 *     and its constituent pieces, for instance) are many times null or incomplete</li>
 *     <li>there are inconsistently defined methods and fields defined on query tree node hierarchy and do
 *     unexpectedly different things. Examples include:
 *     <ul>
 *         <li>getSourceResultColumn(), getSource(), getOriginalSourceResultColumn()</li>
 *         <li>getColumnPosition(), getVirtualColumnID()</li>
 *         <li>all the different ways to get the "name" of something, some of which return null on the
 *         same object</li>
 *     </ul>
 *     </li>
 * </ul>
 */
public class WindowResultSetNode extends SingleChildResultSetNode {
    private static Logger LOG = Logger.getLogger(WindowResultSetNode.class);

    private WindowDefinitionNode wdn;

    /**
     * The list of all window functions in the query block
     * that contains this partition.
     */
    private Collection<WindowFunctionNode> windowFunctions;

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

    private RCtoCRfactory rCtoCRfactory;

    /**
     * Initializer for a WindowResultSetNode.
     *
     * @param selectPR        The projection from the select node
     * @param windowDef       The window definition including the functions that will
     *                        operate on them.
     * @param tableProperties Properties list associated with the table
     * @param nestingLevel    nestingLevel of this group by node. This is used for
     *                        error checking of group by queries with having clause.
     * @throws StandardException Thrown on error
     */
    public void init(
        Object selectPR,
        Object windowDef,
        Object tableProperties,
        Object nestingLevel) throws StandardException {

        super.init(selectPR, tableProperties);
        setLevel((Integer) nestingLevel);

        this.wdn = (WindowDefinitionNode) windowDef;
        this.windowFunctions = wdn.getWindowFunctions();
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
        windowInfoList = new WindowFunctionInfoList();
        rCtoCRfactory = new RCtoCRfactory(getNodeFactory(), getContextManager());
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
     * Add a whole slew of columns needed for Window Functions Basically, for each function we add at least
     * 3 columns: the function input expression(s) (ranking functions accept many) and the window function
     * column and a column where the function result is stored.  The input expressions are taken directly
     * from the window function node.  The window function is the run time window function.  We add it to
     * the RC list as a new object coming into the sort node.
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
     * <LI> create a new RC in the bottom RCL and set it to the col ref </LI>
     * <LI> create a new RC in the partition RCL and set it to point to the bottom RC </LI>
     * <LI> reset the top PR ref to point to the new partition RCL</LI>
     * </UL>
     * <p/>
     * For each function in windowFunctions <UL>
     * <LI> create RC in FROM TABLE.  Fill it with function Operator.
     * <LI> create RC in FROM TABLE for function result</LI>
     * <LI> create RC in FROM TABLE for window function</LI>
     * <LI> create RCs in PARTITION for function input, set them to point to FROM TABLE RC </LI>
     * <LI> create RC in PARTITION for function result</LI>
     * <LI> create RC in PARTITION for window function</LI>
     * <LI> replace function with reference to RC for function result </LI></UL>.
     * <p/>
     * For a query like,
     * <pre>
     * select c1, sum(c2), rank() over(partition by c3 order by c4[, ...]) from t1 group by c1;
     * </pre>
     * the query tree ends up looking like this:
     * <pre>
     * ProjectRestrictNode RCL -> (ptr to WRN(column[0]), ptr to WRN(column[1]), ptr to WRN(column[4]))
     * |
     * WindowResultSetNode RCL->(C1, SUM(C2), <function-input>, <window function>,
     * RANK(), <function-input>[, <function-input>, ...], <window function>)
     * |
     * ProjectRestrict RCL->(C1, C2, C3, C4)
     * |
     * FromBaseTable
     * </pre>
     * <p/>
     * The RCL of the WindowResultSetNode contains all the window (or over clause columns)
     * followed by at least 3 RC's for each function in this order: the final computed
     * function value (function result), the function input columns and a column for
     * the window function itself.
     * <p/>
     * The window function puts the results in the first column of the RC's
     * and the PR resultset in turn picks up the value from there.
     * <p/>
     * The notation (ptr to WRN(column[0])) basically means that it is
     * a pointer to the 0th RC in the RCL of the WindowResultSetNode.
     * <p/>
     * The addition of these window and function columns to the WindowResultSetNode and
     * to the PRN is performed in splicePreviousResultColumns and addMissingColumns.
     * <p/>
     * Note that that addition of the WindowResultSetNode is done after the
     * query is optimized (in {@link SelectNode#modifyAccessPaths()}) which means a
     * fair amount of patching up is needed to account for generated partition columns.
     *
     * @throws StandardException
     */
    public FromTable processWindowDefinition() throws StandardException {

        // The first thing we do is put ourselves on top of the SELECT.
        // The select becomes the childResult.  So our RCL becomes its RCL
        // (so nodes above it now point to us).  Map our RCL to its columns.
        addNewWindowProjection();

        // Keep track of the refs we've added to our RCLs
        Map<String, ColumnRefPosition> pulledUp = new HashMap<>();

        // Keep track of the expressions we've added to our RCLs
        Map<ResultColumn, ColumnRefPosition> cachedExpressionMap = new HashMap<>();

        // Splice up (repair) existing RCs since we've now broken the connection between parent PR
        // and grandchild ResultSetNode.
        splicePreviousResultColumns(pulledUp);

        // Add columns we reference in the over clause
        addOverColumns(pulledUp, cachedExpressionMap);

        // Add any columns to our projection that are required by nodes above us
        // that we disconnected when we inserted ourselves into the tree
        addMissingColumns();

        // Add columns we need for the functions themselves - [#Result, #Input(s), #Function (class)]
        addWindowFunctionColumns();


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

        return parent;
    }

    /**
     * Add a new PR node for aggregation.  Put the
     * new PR under the sort.
     *
     * @throws StandardException
     */
    private void addNewWindowProjection() throws StandardException {
        /*
        ** The first thing we do is put ourselves on
        ** top of the SELECT.  The select becomes the
        ** childResult.  So our RCL becomes its RCL (so
        ** nodes above it now point to us).  Map our
        ** RCL to its columns.
        * This is done so that we can map parent PR columns to reference
        * our function result columns and to any function result columns below us.
        */

        ResultColumnList newBottomRCL = childResult.getResultColumns().copyListAndObjects();
        resultColumns = childResult.getResultColumns();
        childResult.setResultColumns(newBottomRCL);

        /*
        ** Get the new PR, put above this window result.
        */
        ResultColumnList rclNew = (ResultColumnList) getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN_LIST,
                                                                              getContextManager());
        int sz = resultColumns.size();
        for (int i = 0; i < sz; i++) {
            ResultColumn rc = resultColumns.elementAt(i);
            if (! rc.isGenerated()) {
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
     * Since we pull apart an existing parent/child hierarchy and we don't now what's come before us (below us in the
     * tree), we first need to splice those broken relationships between the parent PR and the (now) grandChild
     * result set node.<br/>
     * We do this by taking the existing RC referenced by each parent RC, which is now 2 levels below it, and
     * creating 2 RC->CR pairs, one for our child RCL and one for ours, and "splicing" them in.
     *
     * @param pulledUp a mapping to CRs we've spliced together that we can point to if it's determined that
     *                 we have a dependency on them.
     * @throws StandardException
     */
    private void splicePreviousResultColumns(Map<String, ColumnRefPosition> pulledUp) throws StandardException {
        ResultColumnList childRCL = childResult.getResultColumns();
        ResultColumnList winRCL = resultColumns;

        for (ResultColumn parentRC : parent.getResultColumns()) {
            ValueNode parentRCExpression = parentRC.getExpression();
            ResultColumn srcRC = getResultColumn(parentRCExpression);
            String baseName = parentRC.getName();
            if (srcRC != null) {
                // create RC->CR->srcRC for bottom PR
                ResultColumn bottomRC = createRC_CR_Pair(srcRC, baseName, getNodeFactory(), getContextManager(), this.getLevel(), this.getLevel());
                childRCL.addElement(bottomRC);
                bottomRC.setVirtualColumnId(childRCL.size());

                // create RC->CR->bottomRC for window PR
                ResultColumn winRC = createRC_CR_Pair(bottomRC, baseName, getNodeFactory(), getContextManager(), this.getLevel(), this.getLevel());
                winRCL.addElement(winRC);
                winRC.setVirtualColumnId(winRCL.size());

                // Track what we've found for reuse (sharing)
                pulledUp.put(getExpressionKey(winRC), new ColumnRefPosition(bottomRC, childRCL.size()));

                // Re-point the parent RC's expression
                replaceResultColumnExpression(parentRC, parentRCExpression, winRC);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("splicePreviousResultColumns() spliced existing " + baseName + " in " + wdn.toString());
                }
            }
        }
    }

    /**
     * create and insert RC->CR pair for matching Result Column found
     *
     * @param srcRC  Result column being found from findOrPullUpRC()
     * @param pulledUp  a mapping to CRs we've spliced together that we can point to if it's determined that
     *                 we have a dependency on them.
     * @param cachedExpressionMap a mapping of an expression's ResultColumn in the child to the source ResultColumn
     *                            in its child, plus the position in the RCL.
     * @param exp over column expression Node
     * @param expRC Result col for over col expression
     * @param expKey Name of over col
     * @param childRCL  ResultSetNode under the SingleChildResultSetNode
     * @param winRCL  window function result colummn list
     * @throws StandardException
     */
    private void createInsertRC_CR_Pair(ResultColumn srcRC, Map<String, ColumnRefPosition> pulledUp,
                                        Map<ResultColumn, ColumnRefPosition> cachedExpressionMap,
                                        ValueNode exp, ResultColumn expRC,
                                        String expKey, ResultColumnList childRCL, ResultColumnList winRCL) throws StandardException {
        if (srcRC != null) {
            // We found the RC referenced by this column from the over clause. Create RC->CR pairs
            // in our two RCLs (our projection -- this RCL -- and the PR we created as a staging
            // below us. Then cache the PR's RC so that we can reference repeated columns to the
            // same RC.
            String baseName = isSimpleColumnExpression(exp) ? exp.getColumnName() :
                               exp instanceof UnaryOperatorNode ? ((UnaryOperatorNode)exp).getOperatorString() : "";

            // create RC->CR->srcRC for bottom PR
            ResultColumn bottomRC = createRC_CR_Pair(srcRC, baseName, getNodeFactory(),
                    getContextManager(), this.getLevel(), this.getLevel());
            childRCL.addElement(bottomRC);
            bottomRC.setVirtualColumnId(childRCL.size());

            // create RC->CR->bottomRC for window PR
            ResultColumn winRC = createRC_CR_Pair(bottomRC, baseName, getNodeFactory(),
                    getContextManager(), this.getLevel(), this.getLevel());
            winRCL.addElement(winRC);
            winRC.setVirtualColumnId(winRCL.size());

            // Cache the ref
            ColumnRefPosition colRefPos = new ColumnRefPosition(bottomRC, childRCL.size());
            if (expKey != null)
                pulledUp.put(expKey, colRefPos);
            else
                cachedExpressionMap.put(srcRC, colRefPos);
        }
    }

    private static ColumnRefPosition findCachedResultColumn(String expKey, ResultColumn srcRC,
                                                            Map<String, ColumnRefPosition> pulledUp,
                                                            Map<ResultColumn, ColumnRefPosition> cachedExpressionMap) {
        ColumnRefPosition crp = null;
        if (expKey != null)
            crp = pulledUp.get(expKey);
        if (crp == null)
            crp = cachedExpressionMap.get(srcRC);
        return crp;
    }

    /**
         * Now we find expressions from the grand child result set node below us that we require as given in the columns
         * defined in our over clause. We create RC->CR pairs and append them to ours and our child's RCL.
         * <p/>
         * Columns from the window function over clause (over columns) are the essence of splice window functions.
         * They're used to form the row key by which rows are sorted and thus their order of evaluation. It's imperative
         * to have their column position properly defined. We assert that they're >= 1 (they're one-based in this class)
         * before the end of processing.
         * <p/>
         * We don't pull up duplicate expressions, one will suffice, but we still need to re-point all over column CRs
         * to the pulled up src CRs.
         * <p/>
         * Also during this stage, we track all the expressions we pull up and visit parent RCs replacing any existing
         * references to the expressions that live below us with VCNs that point to the expressions we've pulled up.
         *
         * @param pulledUp a mapping to CRs we've spliced together that we can point to if it's determined that
         *                 we have a dependency on them.
         * @param cachedExpressionMap a mapping of an expression's ResultColumn in the child to the source ResultColumn
         *                           in its child, plus the position in the RCL.
         * @throws StandardException
         */
    private void addOverColumns(Map<String, ColumnRefPosition> pulledUp,
                                Map<ResultColumn, ColumnRefPosition> cachedExpressionMap) throws StandardException {
        ResultColumnList childRCL = childResult.getResultColumns();
        ResultColumnList winRCL = resultColumns;
        ColumnRefPosition crp;
        ColumnPullupVisitor columnPuller = new ColumnPullupVisitor(WindowFunctionNode.class, LOG, ((SingleChildResultSetNode) childResult).getChildResult(), rCtoCRfactory);

        for (OrderedColumn overCol : wdn.getOverColumns()) {
            ValueNode exp = overCol.getColumnExpression();
            ResultColumn expRC = getResultColumn(exp);
            ResultColumn srcRC = null;
            String expKey = getExpressionKey(expRC);

            if (pulledUp.get(expKey) == null) {
                ResultColumn complexExpressionRCMatch =
                      findEquivalentExpression(exp,
                          ((SingleChildResultSetNode) childResult).getChildResult().
                          getResultColumns());
                if (!contains(expRC, winRCL) &&
                     complexExpressionRCMatch == null) {
                    srcRC = findOrPullUpRC(expRC,
                                ((SingleChildResultSetNode) childResult).getChildResult(),
                                rCtoCRfactory);
                    if (srcRC != null) {
                        createInsertRC_CR_Pair(srcRC, pulledUp, cachedExpressionMap,
                                               exp, expRC, expKey, childRCL, winRCL);

                    } else if (exp instanceof ColumnReference &&
                            ((((ColumnReference)exp).getGeneratedToReplaceAggregate())  ||
                             (expRC != null)) ) {
                        // We didn't find it in an RCL below us, probably because it's an agg function previously
                        // created -- the RCs for those don't have ColumnDescriptors which we need to find an
                        // equivalent RC.
                        // In that case, the best we can do is to use the expression directly.
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("addOverColumns(): missing Over column: " + exp.getColumnName() +
                                          " using existing operand RC: " + expRC.getName() + " in " + wdn.toString());
                        }
                        // Add a reference to this column into the bottom RCL.
                        ResultColumn bottomRC = createRC_CR_Pair(expRC, exp.getColumnName(), getNodeFactory(),
                                                                 getContextManager(), this.getLevel(), this.getLevel());
                        childRCL.addElement(bottomRC);
                        bottomRC.setVirtualColumnId(childRCL.size());

                        // create RC->CR->bottomRC for window PR
                        ResultColumn winRC = createRC_CR_Pair(bottomRC, exp.getColumnName(), getNodeFactory(),
                                                              getContextManager(), this.getLevel(), this.getLevel());
                        winRCL.addElement(winRC);
                        winRC.setVirtualColumnId(winRCL.size());

                        // Cache the ref
                        pulledUp.put(expKey, new ColumnRefPosition(bottomRC, childRCL.size()));
                    }
                    // expRC is null when we have an expression which is not just a simple column
                    // reference, but some combination of operators and expressions.
                    else if (expRC == null) {
                        exp.accept(columnPuller);
                        srcRC = addExpressionToRCL(childResult,
                                             "", exp, getNodeFactory(), getContextManager());

                        // Add a reference to this column into the windowing RCL.
                        expRC = createRC_CR_Pair(srcRC, srcRC.getName(), getNodeFactory(),
                                    getContextManager(), this.getLevel(), this.getLevel());
                        winRCL.addElement(expRC);
                        expRC.setVirtualColumnId(winRCL.size());

                        ColumnRefPosition colRefPos = new ColumnRefPosition(srcRC, srcRC.getVirtualColumnId());
                        cachedExpressionMap.put(srcRC, colRefPos);

                    }
                } else {
                    // We have equivalent match, so find the matching RC and add it in the map
                    if (expRC == null) {
                        srcRC = complexExpressionRCMatch;
                    }
                    else
                        srcRC = findOrPullUpRC(expRC,
                                               ((SingleChildResultSetNode) childResult).getChildResult(),
                                               rCtoCRfactory);

                    // Only add this expression if not already added.
                    crp = findCachedResultColumn(expKey, srcRC, pulledUp, cachedExpressionMap);
                    if (crp == null)
                        createInsertRC_CR_Pair(srcRC, pulledUp, cachedExpressionMap, exp, expRC, expKey, childRCL, winRCL);

                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("addOverColumns() already pulled up "+exp.getColumnName()+" in "+wdn.toString());
                }
            }

            // re-point the overCol exp to the RC's expression we've cached
            crp = findCachedResultColumn(expKey, srcRC, pulledUp, cachedExpressionMap);
            assert crp != null : "Failed to find CR for "+expKey;
            overCol.setColumnExpression(crp.rc.getExpression());
            overCol.setColumnPosition(crp.position);
        }
    }

    /**
     * Now, before we add the function tuples, we search for any existing child expressions that we don't have in
     * our RCLs. These expressions may not be listed directly in our parent's RCL but possibly as a
     * dependency of one of those, e.g., a window function embedded in a CASE statement (ConditionNode).
     * <p/>
     * Again, we create RC->CR pairs for these orphaned expressions to pull references up and use a substitution
     * visitor to replace existing references with our pulled up references.
     *
     * @throws StandardException
     */
    private void addMissingColumns() throws StandardException {
        ResultColumnList childRCL = childResult.getResultColumns();
        ResultColumnList winRCL = resultColumns;

        TreeStitchingVisitor referencesToSubstitute = new TreeStitchingVisitor(WindowFunctionNode.class, LOG);
        for (ResultColumn grandChildRC : ((SingleChildResultSetNode)childResult).childResult.getResultColumns()) {
            if (! (grandChildRC.getName().endsWith("Function") || grandChildRC.getName().endsWith("Input"))) {
                // don't pull up previous function or function input arguments
                ResultColumn winRC;
                String baseName = grandChildRC.getName();

                if (contains(grandChildRC, childRCL)) {
                    // We have a ref to the existing grandchild RC in the existing RCL below us in our RCLs
                    // (it's in childRCL so must be in winRCL -- we never add to one and not the other).
                    // Note that because it's existing in the grandchild RCL means that it existed before
                    // we broke the chain and inserted our PRs. That means we'll still have to visit the
                    // parent RCL (and their progeny) to find the referring expression and replace the
                    // source it's pointing to.
                    winRC = findOrPullUpRC(grandChildRC, this, rCtoCRfactory);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("addMissingColumns() visiting for missing col " + baseName +
                                      " found in win RCL: " + wdn.toString());
                    }
                } else {
                    // If we don't have the existing grandchild RC in the child RCL below us, create
                    // RCs in our child RCL and our win RCL that we can point references to which live above us.

                    // create RC->CR->srcRC for bottom PR
                    ResultColumn bottomRC = createRC_CR_Pair(grandChildRC, baseName, getNodeFactory(),
                                                             getContextManager(), this.getLevel(), this.getLevel());

                    childRCL.addElement(bottomRC);
                    bottomRC.setVirtualColumnId(childRCL.size());

                    // create RC->CR->bottomRC for window PR
                    winRC = createRC_CR_Pair(bottomRC, baseName, getNodeFactory(), getContextManager(),
                                             this.getLevel(), this.getLevel());
                    winRCL.addElement(winRC);
                    winRC.setVirtualColumnId(winRCL.size());

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("addMissingColumns() visiting for missing col " + baseName +
                                      " NOT found in win RCL: " + wdn.toString());
                    }
                }

                // Reset the referencing expression above us to point to the RC in the window RCL.
                // To do this, we'll create a VCN pointing to the right place and use the orphaned RC to
                // find the reference above us. When found, we'll replace the referencing expression with
                // this VCN.
                VirtualColumnNode vcn = (VirtualColumnNode) getNodeFactory().getNode(C_NodeTypes
                                                                                         .VIRTUAL_COLUMN_NODE,
                                                                                     this, // source result set.
                                                                                     winRC,
                                                                                     winRCL.size(),
                                                                                     getContextManager());
                // This substitution visitor for column reference links we broke while inserting ourselves
                referencesToSubstitute.addMapping(grandChildRC, vcn);
            }
        }

        // Visit and replace...
        parent.getResultColumns().accept(referencesToSubstitute);
    }

    /**
     * Finally, we add the window function tuples (<code>{result, input[, input, ...], function class}</code>)
     * and prepare to generate the activation.
     *
     * @see #processWindowDefinition
     */
    private void addWindowFunctionColumns() throws StandardException {
        // note childRCL is a reference to childResult resultColumns
        // setting columns on childRCL will also set them on childResult resultColumns
        ResultColumnList childRCL = childResult.getResultColumns();
        // note winRCL is a reference to resultColumns
        // setting columns on winRCL will also set them on resultColumns
        ResultColumnList winRCL = resultColumns;

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

            // Replace the WF nodes in the projection with a CR whose src will be set to the WF
            // result column created below
            WindowFunctionReplacementVisitor replaceWindowFunctionVisitor =
                new WindowFunctionReplacementVisitor(((FromTable)childResult).getTableNumber(),
                                                     this.level,
                                                     null);
            parent.getResultColumns().accept(replaceWindowFunctionVisitor);

            /*
            ** FUNCTION RESULT: Set the windowFunctionNode result to null in the
            ** bottom project restrict.
            */
            ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN,
                                                                            "##" + windowFunctionNode.getAggregateName() +
                                                                                "Result",
                                                                            windowFunctionNode.getNewNullResultExpression(),
                                                                            getContextManager());
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            childRCL.addElement(newRC);
            newRC.setVirtualColumnId(childRCL.size());
            int fnResultVID = newRC.getVirtualColumnId();

            /*
            ** Set the windowFunctionNode result column to point to this RC.
            */
            ResultColumn tmpRC = createRC_CR_Pair(newRC, newRC.getName(), getNodeFactory(),
                                                  getContextManager(), this.getLevel(), this.getLevel());
            winRCL.addElement(tmpRC);
            tmpRC.setVirtualColumnId(winRCL.size());
            ((ColumnReference)tmpRC.getExpression()).markGeneratedToReplaceWindowFunctionCall();
            // Now set this tmpRC as the src of generated reference that we created during WindowFunctionReplacementVisitor
            windowFunctionNode.getGeneratedRef().setSource(tmpRC);

            /*
            ** FUNCTION OPERANDS(s): Create ResultColumns in the bottom project restrict that are the
            * operands for the function.  Scalar aggregate functions will have only one operand (argument),
            * of course, but ranking functions, for instance, have all ORDER BY columns as arguments.
            */
            // Create function references for all input operands
            List<ValueNode> operands = windowFunctionNode.getOperands();
            int[] inputVIDs = new int[operands.size()];
            int i = 0;
            ColumnPullupVisitor columnPuller =
               new ColumnPullupVisitor(WindowFunctionNode.class, LOG, ((SingleChildResultSetNode) this.childResult).getChildResult(), rCtoCRfactory);
            for (ValueNode exp : operands) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("addWindowFunctionColumns() operand expression instance of : " + exp.getClass().getSimpleName());
                }
                // Get the ResultColumn for this expression, if it has one
                ResultColumn expRC = getResultColumn(exp);
                if (expRC != null) {
                    ResultColumn srcRC = findOrPullUpRC(expRC, ((SingleChildResultSetNode) this.childResult).getChildResult(),
                                                        rCtoCRfactory);

                    if (srcRC != null) {
                        // found this operand expression's matching RC in (grandchild) RCL below us
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("addWindowFunctionColumns() found operand column below us: " + exp.getColumnName() +
                                          " in RC: " + srcRC.getName() + " in " + wdn.toString());
                        }

                        // Add a reference to this column into the bottom RCL.
                        tmpRC = createRC_CR_Pair(srcRC,
                                                 "##" + windowFunctionNode.getAggregateName() + "_Input_" +
                                                     exp.getColumnName(),
                                                 getNodeFactory(), getContextManager(), this.getLevel(), this.getLevel());
                    } else {
                        // we didn't find it in an RCL below us.
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("addWindowFunctionColumns() missing operand column: " + exp.getColumnName() +
                                          ". Using existing operand RC: " + expRC.getName() + " in " + wdn.toString());
                        }

                        // Add a reference to this column into the child RCL (child PR).
                        tmpRC = createRC_CR_Pair(expRC,
                                                 "##" + windowFunctionNode.getAggregateName() + "_Input_" +
                                                     exp.getColumnName(),
                                                 getNodeFactory(), getContextManager(), this.getLevel(), this.getLevel());
                    }
                } else {
                    // The given operand does not have an RC. It may be a non-aggregate function expression, i.e.,
                    // like ConditionNode (case stmt).
                    // We create an RC for it and add that to our child PR (child RCL) for function evaluation.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("addWindowFunctionColumns() operand does not have an RC: " + exp.getColumnName() +
                                      ". Creating one for it in " + wdn.toString());
                    }
                    exp.accept(columnPuller);

                    tmpRC = (ResultColumn) getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN,
                             "##" + windowFunctionNode.getAggregateName() + "_Input_" +
                             exp.getColumnName(), exp, getContextManager());

                    tmpRC.markGenerated();
                    tmpRC.bindResultColumnToExpression();
                }
                // Now add the RC we created to our RCLs (child PR and window)
                childRCL.addElement(tmpRC);
                tmpRC.setVirtualColumnId(childRCL.size());

                inputVIDs[i++] = tmpRC.getVirtualColumnId();
                windowFunctionNode.replaceOperand(exp, tmpRC.getExpression());

                // Add a reference to this column into the windowing RCL.
                tmpRC = createRC_CR_Pair(tmpRC, tmpRC.getName(), getNodeFactory(),
                                         getContextManager(), this.getLevel(), this.getLevel());
                winRCL.addElement(tmpRC);
                tmpRC.setVirtualColumnId(winRCL.size());
            }

            /*
            ** FUNCTION: Add a getAggregator method call
            ** to the bottom result column list.
            */
            newRC = windowFunctionNode.getNewAggregatorResultColumn(getDataDictionary());
            newRC.markGenerated();
            newRC.bindResultColumnToExpression();
            newRC.setName("##" + windowFunctionNode.getAggregateName() + "Function");
            childRCL.addElement(newRC);
            newRC.setVirtualColumnId(childRCL.size());
            int fnVID = newRC.getVirtualColumnId();

            /*
            ** Add a reference to this column in the windowing RCL.
            */
            tmpRC = createRC_CR_Pair(newRC, newRC.getName(), getNodeFactory(),
                                     getContextManager(), this.getLevel(), this.getLevel());
            winRCL.addElement(tmpRC);
            tmpRC.setVirtualColumnId(winRCL.size());

            /*
            ** Create an window function result expression
             */
            ResultColumn fnResultRC = (ResultColumn) getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN,
                                                                                "##" + windowFunctionNode.getAggregateName() + "ResultExp",
                                                                                windowFunctionNode.getNewNullResultExpression(),
                                                                                getContextManager());

            /*
            ** Piece together a fake one column rcl that we will use
            ** to generate a proper result description for input
            ** to this function if it is a user function.
            */
            ResultColumnList udfRCL = (ResultColumnList) getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN_LIST,
                                                                                    getContextManager());
            udfRCL.addElement(fnResultRC);

            LanguageFactory lf = getLanguageConnectionContext().getLanguageFactory();
            /*
            ** Note that the column ids here are all are 1-based
            */
            FormatableArrayHolder partitionCols = createColumnOrdering(wdn.getPartition(), "Partition");
            FormatableArrayHolder orderByCols = createColumnOrdering(wdn.getOrderByList(), "Order By");
            FormatableArrayHolder keyCols = createColumnOrdering(wdn.getKeyColumns(), "Key");
            windowInfoList.addElement(new WindowFunctionInfo(
                windowFunctionNode.getAggregateName(),
                windowFunctionNode.getType(),
                windowFunctionNode.getAggregatorClassName(),
                inputVIDs,       // windowFunctionNode input columns
                fnResultVID,    // the windowFunctionNode result column
                fnVID,   // the aggregator column
                lf.getResultDescription(udfRCL.makeResultDescriptors(), "SELECT"),
                partitionCols,      // window function partition
                orderByCols,        // window function order by
                keyCols,            // deduplicated set of partition and order by cols that will be row key
                windowFunctionNode.getWindow().getFrameExtent().toMap(),
                windowFunctionNode.getFunctionSpecificArgs()
            ));
        }
    }

    ///////////////////////////////////////////////////////////////
    //
    // Ancillary
    //
    ///////////////////////////////////////////////////////////////

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
        ((Optimizable) childResult).optimizeIt(
            optimizer,
            predList,
            outerCost,
            rowOrdering);

        return super.optimizeIt(
            optimizer,
            predList,
            outerCost,
            rowOrdering
        );
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
     * @see com.splicemachine.db.iapi.sql.compile.Optimizable#pushOptPredicate
     */

    public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate)
        throws StandardException {
        return ((Optimizable) childResult).pushOptPredicate(optimizablePredicate);
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
            for (AggregateNode function : windowFunctions) {
                debugPrint(formatNodeString("[" + i++ + "]:", depth + 1));
                function.treePrint(depth + 1);
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
     * @param forSpark
     * @return ResultSetNode    The top of the optimized subtree
     * @throws StandardException Thrown on error
     */

    public ResultSetNode optimize(DataDictionary dataDictionary,
                                  PredicateList predicates,
                                  double outerRows,
                                  boolean forSpark)
        throws StandardException {
        /* We need to implement this method since a PRN can appear above a
         * SelectNode in a query tree.
         */
        childResult = childResult.optimize(
                dataDictionary,
                predicates,
                outerRows,
                forSpark);
        Optimizer optimizer = getOptimizer(
                (FromList) getNodeFactory().getNode(
                        C_NodeTypes.FROM_LIST,
                        getNodeFactory().doJoinOrderOptimization(),
                        getContextManager()),
                predicates,
                dataDictionary,
                null);
        optimizer.setForSpark(forSpark);

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
        costEstimate = childResult.getFinalCostEstimate(true);

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
         *    arg1: childExpress - Expression for childResult
         *  arg2: isInSortedOrder - true if source result set in sorted order
         *  arg3: aggInfoItem - entry in saved objects for the aggregates,
         *  arg4: Activation
         *  arg5: number of columns in the result
         *  arg6: resultSetNumber
         *  arg7: row count estimate for cost
         *  arg8: estimated cost
         *  arg9: explain plan (for this node only)
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
        mb.push(printExplainInformationForActivation());

        mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "getWindowResultSet",
                      ClassName.NoPutResultSet, 9);

    }

    ///////////////////////////////////////////////////////////////
    //
    // UTILITIES
    //
    ///////////////////////////////////////////////////////////////

    /**
     * Attempts to create a string that uniquely identifies the given RC.
     * <p/>
     * Part of the frustration with Derby is the inability to identify the source
     * of a given result column at all points in the AST.  A ResultColumn has a
     * ColumnDescriptor (CD) field, but it's not always populated.  The CD is supposed
     * to have schema, table and column names but they're also not always present or
     * maybe partially present, like just column name.
     * @param rc the RC to identify
     * @return a string that will uniquely identify the given RC
     */
    private static String getExpressionKey(ResultColumn rc) {
        if (rc == null) {
            return null;
        }
        String[] nameComponents = new String[3];
        ColumnDescriptor colDesc = rc.getTableColumnDescriptor();
        if (colDesc != null) {
            // prefer the column descriptor if available
            nameComponents[2] = colDesc.getColumnName();
            TableDescriptor tableDesc = colDesc.getTableDescriptor();
            if (tableDesc != null) {
                nameComponents[0] = tableDesc.getSchemaName();
                nameComponents[1] = tableDesc.getName();
            }
        }
        if (colDesc == null || nameComponents[0] == null) {
            // either column descriptor is null or column descriptor's table descriptor is null.
            // use the result column info
            nameComponents = new String[] {rc.getSourceSchemaName(), rc.getSourceTableName(), rc.getName()};
        }
        StringBuilder name = new StringBuilder();
        for (String nameComponent : nameComponents) {
            name.append(nameComponent).append('.');
        }
        return name.toString();
    }

    /**
     * Determine if the given RCL contains an RC equivalent to the the given RC.<br/>
     * Equivalency is determined by an RC representing the same column in the same table in the same schema
     * as determined by the RC's ColumnDescriptor and the base column node it originates.
     * For expressions, equivalency is determined by comparing the expression tree via the
     * isEquivalent method.  If this indicates true, or if the RC is the same exact reference
     * in both rc and rcl, then contains() returns true.
     * @param rc the RC for which to search
     * @param rcl the RCL to search
     * @return true if the given RCL contains an equivalent RC
     * @throws StandardException
     */
    private static boolean contains(ResultColumn rc, ResultColumnList rcl) throws StandardException {
        if (rc == null || rcl == null)
            return false;
        if (findEquivalent(rc, rcl) == null)
            return findExactExpression(rc.getExpression(), rcl) != null;
        else
            return true;
    }

    /**
     * Find the matching RC in or below <code>currentNode</code> and return it or its equivalent.<br/>
     * Equivalency is determined by an RC representing the same column in the same table in the same schema
     * as defined by the RC's ColumnDescriptor and the base column node it originates
     * <p/>
     * If the matching RC is below the <code>currentNode</code>, RC->CR pairs are created and added on the end
     * of intermediate result set node RCLs. If a matching RC is not found, null is returned.
     * @param rc the result column that we're looking for in or below <code>currentNode</code>
     * @param currentNode the result set node (and its progeny) in which to look for a matching result column
     * @return a result column residing in <code>currentNode</code> that matches the given <code>rcToMatch</code>
     * @throws StandardException
     */
    public static ResultColumn findOrPullUpRC(ResultColumn rc, ResultSetNode currentNode, RCtoCRfactory rCtoCRfactory)
        throws StandardException {
        if (rc == null)
            return null;

        if (currentNode instanceof FromBaseTable) {
            // we've gone too far
            return null;
        }

        ResultColumn matchedRC = findEquivalent(rc, currentNode.getResultColumns());
        if (matchedRC == null)
            matchedRC = findExactExpression(rc.getExpression(), currentNode.getResultColumns());
        // match at this level. if not null, return it to next stack frame. if null, recurse...
        if (matchedRC == null) {
            if (currentNode instanceof SingleChildResultSetNode) {
                matchedRC = findOrPullUpRC(rc, ((SingleChildResultSetNode)currentNode).getChildResult(), rCtoCRfactory);
            }

            // we've either returned a matching RC from previous stack frame or we never found one (null)
            if (matchedRC != null) {

                // Create an RC->CR->matchedRC for matching column that we'll add to the parent RCL upon return
                ResultColumn parentRC = rCtoCRfactory.createRC_CR_Pair(matchedRC, matchedRC.getName(),
                                                                       ((FromTable)currentNode).getLevel(),
                                                                       ((FromTable)currentNode).getLevel());
                currentNode.getResultColumns().addElement(parentRC);
                parentRC.setVirtualColumnId(currentNode.getResultColumns().size());
                matchedRC = parentRC;
            }
        }
        return matchedRC;
    }

    /**
     * Test to see if an expression is composed completely of a simple <code>ColumnReference</code>,
     * <code>VirtualColumnNode</code> or <code>BaseColumnNode</code>.
     * @param expression The expression to test.
     * @return True, if expression is a ColumnReference, VirtualColumnNode or BaseColumnNode, otherwise false.
     * @throws StandardException
     */
    public static boolean isSimpleColumnExpression (ValueNode expression) {
        return expression instanceof ColumnReference ||
               expression instanceof VirtualColumnNode ||
               expression instanceof BaseColumnNode;
    }

    // To find any expression in the RCL which could replace "expression", while maintaining the
    // query behavior and valid result set.  Note, nodes like JavaToSQLValueNode which represent
    // Java functions could be nondeterministic, so can never be used to replace another expression.
    // For example two different calls to the RANDOM function should each be evaluated to return
    // two different numbers instead of reusing the result of the first function call.
    public static ResultColumn findEquivalentExpression(ValueNode expression, ResultColumnList rcl) throws StandardException{
        if (expression == null)
            return null;
        if (isSimpleColumnExpression(expression))
            return null;

        for (ResultColumn col : rcl) {
            if (col.getExpression() != null &&
                    col.getExpression().isEquivalent(expression)) {
                return col;
            }
        }
        return null;
    }

    // To find the ResultColumn node which is holding the exact node passed in as "expression"
    // (the same exact node in memory, not a clone).
    public static ResultColumn findExactExpression(ValueNode expression, ResultColumnList rcl) throws StandardException{
        if (expression == null)
            return null;

        for (ResultColumn col : rcl) {
            if (col.getExpression() != null &&
                col.getExpression() == expression) {
                return col;
            }
        }
        return null;
    }

    public static ResultColumn findEquivalent(ResultColumn srcRC, ResultColumnList rcl) throws StandardException{
        if (srcRC == null)
            return null;
        if (srcRC.getExpression() == null)
            return null;
        boolean simpleColumnExpression = isSimpleColumnExpression(srcRC.getExpression());

        Pair<ColumnDescriptor, BaseColumnNode> basePair = findResultcolumnIdentifier(srcRC);
        if (basePair.getSecond() == null)
            simpleColumnExpression = false;
        for (ResultColumn col : rcl) {
            if (simpleColumnExpression) {
                if (isMatchingResultColumn(col, basePair))
                    return col;
            }
            else {
                if (col.isEquivalent(srcRC))
                    return col;
            }
        }
        return null;
    }

    /**
     * Find the column descriptor and base column node that can identify a given RC.
     * <p/>
     * Part of the frustration with Derby is the inability to identify the source
     * of a given result column at all points in the AST.
     * <p/>
     * This recursive method is an effort to follow the the lineage of an RC to its origin.
     * The previous approach to use columnDescriptor by itself to identify an RC is
     * problematic as it cannot differentiate the result columns originated from the
     * multiple instances of the same table.
     * @param rc the result column to identify
     * @return the column descriptor and base column pair corresponding to the RC or null if none found.
     */
    private static Pair findResultcolumnIdentifier(ResultColumn rc) {
        ColumnDescriptor columnDescriptor = null;

        while (rc != null) {
            if (columnDescriptor == null)
                columnDescriptor = rc.getTableColumnDescriptor();
            ValueNode exp = rc.getExpression();
            if (exp != null && exp instanceof BaseColumnNode) {
                return new Pair<>(columnDescriptor, exp);
            }

            rc = (exp != null ? getResultColumn(exp) : null);
        }
        return new Pair<>(columnDescriptor, null);
    }

    // base column node is the main variable we use to find matching result column, if two result columns originate from
    // the same source, their column descriptor should also be the same, so we use column descriptor as a way to possibly
    // filter out unmatched result column quicker
    private static boolean isMatchingResultColumn(ResultColumn rc, Pair<ColumnDescriptor, BaseColumnNode> searchingPair) {
        // if we cannot locate the original base column node, we cannot tell if it is a match or not, thus return false.
        if (searchingPair.getSecond() == null)
            return false;
        // if column descriptor is not null, we may be able to use it to rule out unmatched fields quickly
        boolean testCD = searchingPair.getFirst() != null;
        while (rc != null) {
            if (testCD) {
                ColumnDescriptor cd = rc.getTableColumnDescriptor();
                if (cd != null) {
                    if (cd != searchingPair.getFirst()) {
                        // if column descriptor does not match, the rc cannot be a match
                        return false;
                    } else {
                        // column descriptor matches, we no longer need to test, just focus on the comparison of base column node
                        testCD = false;
                    }
                }
            }
            ValueNode exp = rc.getExpression();
            if (exp != null && exp instanceof BaseColumnNode) {
                if (exp == searchingPair.getSecond())
                    return true;
                else
                    return false;
            }
            rc = (exp != null ? getResultColumn(exp) : null);
        }
        return false;
    }

    /**
     * Get the immediate child source result column from an expression, if one exists.
     * <p/>
     * In derby there's usually many ways to do something and not all of them result the same.
     * In this case, we want the RC that the given expression is using as its source. If you
     * call {@link ColumnReference#getSourceResultColumn()}, you get the source of its source,
     * not what you'd expect.  Thus, we have this encapsulated logic so we don't have to test
     * the expression everywhere.
     * @param expression the expression for which you want its source RC.
     * @return the expression's immediate source RC or null if it doesn't have one.
     */
    private static ResultColumn getResultColumn(ValueNode expression) {
        ResultColumn rc = expression.getSourceResultColumn();
        if (expression instanceof ColumnReference) {
            rc = ((ColumnReference)expression).getSource();
        }
        return rc;
    }

    /**
     * Create the appropriate type of ValueNode with sourceRC being its source result column and
     * replace the given ValueNode expression in its referencing parentRC.
     * @param parentRC the result column in which to place the new expression.
     * @param expressionToReplace the expression to replace
     * @param sourceRC the source RC to which to point our new expression.
     * @throws StandardException
     */
    private void replaceResultColumnExpression(ResultColumn parentRC, ValueNode expressionToReplace, ResultColumn sourceRC)
        throws StandardException {
        ValueNode newExpression;
        if (expressionToReplace instanceof VirtualColumnNode) {
            newExpression = (VirtualColumnNode) getNodeFactory().getNode(C_NodeTypes.VIRTUAL_COLUMN_NODE,
                                                                         this, // source result set.
                                                                         sourceRC,
                                                                         this.getResultColumns().size(),
                                                                         getContextManager());
        } else {
            // just create a CR to replace the expression. This needs to be a CR because the setters don't exist
            // on ValueNode
            ColumnReference newCR = (ColumnReference) getNodeFactory().getNode(C_NodeTypes.COLUMN_REFERENCE,
                                                                               expressionToReplace.getColumnName(),
                                                                               null,
                                                                               getContextManager());
            newCR.setSource(sourceRC);
            newCR.setNestingLevel(this.getLevel());
            newCR.setSourceLevel(this.getLevel());
            if (sourceRC.getTableNumber() != -1) {
                newCR.setTableNumber(sourceRC.getTableNumber());
            }
            newExpression = newCR;
        }
        parentRC.setExpression(newExpression);
    }

    /**
     * Create the one-based column ordering for an ordered column list and pack
     * it for shipping.
     *
     * @param orderedColumnList the list of ordered columns to pack.
     * @return The holder for the column ordering array suitable for shipping.
     */
    private FormatableArrayHolder createColumnOrdering(List<OrderedColumn> orderedColumnList, String label) {
        FormatableArrayHolder colOrdering = null;
        if (orderedColumnList != null) {
            IndexColumnOrder[] ordering = new IndexColumnOrder[orderedColumnList.size()];
            int j = 0;
            for (OrderedColumn oc : orderedColumnList) {
                // This assertion will pay off
                assert oc.getColumnPosition() > 0 : label+" column ordering must be > 0!";
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
     * Convenience class to minimize argument passing and getter calling required to create nodes.
     */
    public static class RCtoCRfactory {
        private final NodeFactory nodeFactory;
        private final ContextManager contextManager;


        private RCtoCRfactory(NodeFactory nodeFactory, ContextManager contextManager) {
            this.nodeFactory = nodeFactory;
            this.contextManager = contextManager;
        }

        public ResultColumn createRC_CR_Pair(ResultColumn sourceRC, String name, int srcLevel, int nestingLevel) throws StandardException {
            return WindowResultSetNode.createRC_CR_Pair(sourceRC, name, nodeFactory, contextManager, srcLevel, nestingLevel);
        }
    }

    /**
     * Create a new result column referencing the one passed in.<br/>
     * Creates a column reference whose source is the sourceRC, then creates a result column whose
     * expression is the newly created column reference (<code>RC->CR->sourceRC</code>).
     *
     * @param sourceRC the source result column.
     * @param name name the pair. Can be overwritten upon return.
     * @param nodeFactory for creating nodes
     * @param contextManager for creating nodes
     * @param sourceLevel the level we're at in the tree
     * @param nestingLevel the level of subquery nesting
     * @return the new result column
     * @throws StandardException on error
     */
    private static ResultColumn createRC_CR_Pair(ResultColumn sourceRC,
                                                 String name,
                                                 NodeFactory nodeFactory,
                                                 ContextManager contextManager,
                                                 int sourceLevel, int nestingLevel) throws StandardException {

        // create the CR using sourceRC as the source and source name
        ColumnReference columnRef = (ColumnReference) nodeFactory.getNode(C_NodeTypes.COLUMN_REFERENCE,
                                                                          name,
                                                                          null,
                                                                          contextManager);
        columnRef.setSource(sourceRC);
        columnRef.setNestingLevel(nestingLevel);
        columnRef.setSourceLevel(sourceLevel);
        if (sourceRC.getTableNumber() != -1) {
            columnRef.setTableNumber(sourceRC.getTableNumber());
        }

        // create the RC, wrap the CR
        ResultColumn resultColumn = (ResultColumn) nodeFactory.getNode(C_NodeTypes.RESULT_COLUMN,
                                                                       name,
                                                                       columnRef,
                                                                       contextManager);
        resultColumn.markGenerated();
        resultColumn.bindResultColumnToExpression();
        if (sourceRC.getTableColumnDescriptor() != null) {
            resultColumn.setColumnDescriptor(sourceRC.getTableColumnDescriptor());
        }
        if (sourceRC.getSourceSchemaName() != null) {
            resultColumn.setSourceSchemaName(sourceRC.getSourceSchemaName());
        }
        if (sourceRC.getSourceTableName() != null) {
            resultColumn.setSourceTableName(sourceRC.getSourceTableName());
        }

        return resultColumn;
    }

    /**
     * Create a new result column referencing the expression passed in.<br/>
     * If resultSet is a ProjectRestrictNode, a new ResultColumn is added
     * to its ResultColumnList, and is given the expression passed in.
     * If resultSet
     *
     * @param resultSet The ResultSetNode to modify and add expression to its RCL.
     * @param name name of the ResultColumn added.
     * @param expression The expression involving columns to add to the RCL.
     * @param nodeFactory for creating nodes
     * @param contextManager for creating nodes
     * @return the new result column
     * @throws StandardException on error
     */
    private static ResultColumn addExpressionToRCL(ResultSetNode resultSet,
                                                   String name,
                                                   ValueNode expression,
                                                   NodeFactory nodeFactory,
                                                   ContextManager contextManager) throws StandardException {


        ResultColumnList resultColumnList = resultSet.getResultColumns();
        ResultColumnList origResultColumnList = resultColumnList;

        resultColumnList = origResultColumnList;
        ResultColumn resultColumn = (ResultColumn)
        nodeFactory.getNode(C_NodeTypes.RESULT_COLUMN,
                            name,
                            expression,
                            contextManager);
        resultColumn.markGenerated();
        resultColumn.bindResultColumnToExpression();
        resultColumnList.addResultColumn(resultColumn);
        return resultColumn;
    }

    /**
     * Structure used to track an RC's position in an RCL as they're found.
     */
    private static class ColumnRefPosition {
        final ResultColumn rc;
        final int position;
        public ColumnRefPosition(ResultColumn rc, int position) {
            this.rc = rc;
            this.position = position;
        }
    }

    @Override
    public String printExplainInformation(String attrDelim) throws StandardException {
        return spaceToLevel() + "WindowFunction" + "(" + "n=" + getResultSetNumber() + attrDelim +
            getFinalCostEstimate(false).prettyProjectionString(attrDelim) + ")";
    }

    @Override
    public String toHTMLString() {
        StringBuilder buf = new StringBuilder();
        buf.append("resultSetNumber: ").append(getResultSetNumber()).append("<br/>")
            .append("level: ").append(getLevel()).append("<br/>")
            .append("correlationName: ").append(getCorrelationName()).append("<br/>")
            .append("corrTableName: ").append(Objects.toString(corrTableName)).append("<br/>")
            .append("tableNumber: ").append(getTableNumber()).append("<br/>")
            .append("<br/>WindowFunctions: <br/>");
        for (WindowFunctionInfo info : windowInfoList) {
            buf.append(info.toHTMLString()).append("<br/>");
        }
        buf.append("WindowDefinition: <br/>").append(wdn.toHTMLString()).append("<br/>");
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
            StringBuilder buf = new StringBuilder();
            buf.append("WindowDefinition: ").append(wdn.toString()).append(super.toString());
            buf.append("\nFunctions: ");
            for (WindowFunctionInfo info : windowInfoList) {
                buf.append('\n').append(info.toString());
            }
            return buf.toString();
        } else {
            return "";
        }
    }
    /**
     * TODO: JC - Added just to make PlanPrinter compile. Remove when PlanPrinter gone.
     */
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
}
