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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.FormatableIntHolder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.util.PropertyUtil;
import com.splicemachine.db.impl.ast.CollectingVisitor;
import com.splicemachine.db.impl.ast.PredicateUtils;
import com.splicemachine.db.impl.ast.RSUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
import splice.com.google.common.base.Joiner;
import splice.com.google.common.base.Predicates;
import splice.com.google.common.collect.Lists;

import java.util.*;

import static com.splicemachine.db.impl.sql.compile.BinaryOperatorNode.AND;

/**
 * A JoinNode represents a join result set for either of the basic DML
 * operations: SELECT and INSERT.  For INSERT - SELECT, any of the
 * fields in a JoinNode can be used (the JoinNode represents
 * the (join) SELECT statement in the INSERT - SELECT).  For INSERT,
 * the resultColumns in the selectList will contain the names of the columns
 * being inserted into or updated.
 */

public class JoinNode extends TableOperatorNode{
    /* Join semantics */
    public static final int INNERJOIN=1;
    public static final int CROSSJOIN=2;
    public static final int LEFTOUTERJOIN=3;
    public static final int RIGHTOUTERJOIN=4;
    public static final int FULLOUTERJOIN=5;
    public static final int UNIONJOIN=6;
    public PredicateList joinPredicates;
    public SparkExpressionNode sparkExpressionTree = null;
    // Splice Additions
    public int[] leftHashKeys;
    public int[] rightHashKeys;
    public int[] rightHashKeySortOrders = null;
    protected boolean flattenableJoin=true;
    List<AggregateNode> aggregateVector;
    SubqueryList subqueryList;
    ValueNode joinClause;
    boolean joinClauseNormalized;
    ResultColumnList usingClause;
    //User provided optimizer overrides
    Properties joinOrderStrategyProperties;
    /**
     * If this flag is true, this node represents a natural join.
     */
    private boolean naturalJoin;
    private boolean optimized;
    private PredicateList leftPredicateList;
    private PredicateList rightPredicateList;

    protected enum CONVERSION {CONVERTLEFT, CONVERTRIGHT, CONVERTINNER, NONE}

    /**
     * Convert the joinType to a string.
     *
     * @param joinType The joinType as an int.
     * @return String        The joinType as a String.
     */
    public static String joinTypeToString(int joinType){
        switch(joinType){
            case INNERJOIN:
                return "INNER JOIN";
            case CROSSJOIN:
                return "CROSS JOIN";
            case LEFTOUTERJOIN:
                return "LEFT OUTER JOIN";
            case RIGHTOUTERJOIN:
                return "RIGHT OUTER JOIN";
            case FULLOUTERJOIN:
                return "FULL OUTER JOIN";
            case UNIONJOIN:
                return "UNION JOIN";
            default:
                if(SanityManager.DEBUG){
                    SanityManager.ASSERT(false,"Unexpected joinType");
                }
                return null;
        }
    }

    /*
     *  Optimizable interface
     */

    /**
     * Initializer for a JoinNode.
     *
     * @param leftResult                  The ResultSetNode on the left side of this join
     * @param rightResult                 The ResultSetNode on the right side of this join
     * @param onClause                    The ON clause
     * @param usingClause                 The USING clause
     * @param selectList                  The result column list for the join
     * @param tableProperties             Properties list associated with the table
     * @param joinOrderStrategyProperties User provided optimizer overrides
     * @throws StandardException Thrown on error
     */
    public void init(
            Object leftResult,
            Object rightResult,
            Object onClause,
            Object usingClause,
            Object selectList,
            Object tableProperties,
            Object joinOrderStrategyProperties)
            throws StandardException{
        super.init(leftResult,rightResult,tableProperties);
        resultColumns=(ResultColumnList)selectList;

//        FromTable leftFromTable=(FromTable)leftResultSet;
//        FromTable rightFromTable=(FromTable)rightResultSet;


        joinClause=(ValueNode)onClause;
        joinClauseNormalized=false;
        this.usingClause=(ResultColumnList)usingClause;
        this.joinOrderStrategyProperties=(Properties)joinOrderStrategyProperties;

        /* JoinNodes can be generated in the parser or at the end of optimization.
         * Those generated in the parser do not have resultColumns yet.
         */
        if(resultColumns!=null){
            /* A longer term assertion */
            assert (leftResultSet.getReferencedTableMap()!=null && rightResultSet.getReferencedTableMap()!=null)
                   || (leftResultSet.getReferencedTableMap()==null && rightResultSet.getReferencedTableMap()==null):
                    "left and right referencedTableMaps are expected to either both be non-null or both be null";

            /* Build the referenced table map (left || right) */
            if(leftResultSet.getReferencedTableMap()!=null){
                referencedTableMap=(JBitSet)leftResultSet.getReferencedTableMap().clone();
                referencedTableMap.or(rightResultSet.getReferencedTableMap());
            }
        }
        joinPredicates=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST, getContextManager());

    }

    @Override
    public CostEstimate optimizeIt(Optimizer optimizer,
                                   OptimizablePredicateList predList,
                                   CostEstimate outerCost,
                                   RowOrdering rowOrdering) throws StandardException{
        optimizer.tracer().trace(OptimizerFlag.CALLING_ON_JOIN_NODE,0,0,0.0,null);

        // It's possible that a call to optimize the left/right will cause
        // a new "truly the best" plan to be stored in the underlying base
        // tables.  If that happens and then we decide to skip that plan
        // (which we might do if the call to "considerCost()" below decides
        // the current path is infeasible or not the best) we need to be
        // able to revert back to the "truly the best" plans that we had
        // saved before we got here.  So with this next call we save the
        // current plans using "this" node as the key.  If needed, we'll
        // then make the call to revert the plans in OptimizerImpl's
        // getNextDecoratedPermutation() method.
        updateBestPlanMap(ADD_PLAN,this);

        /*
        ** RESOLVE: Most types of Optimizables only implement estimateCost(),
        ** and leave it up to optimizeIt() in FromTable to figure out the
        ** total cost of the join.  For joins, though, we want to figure out
        ** the best plan for the join knowing how many outer rows there are -
        ** it could affect the join strategy significantly.  So we implement
        ** optimizeIt() here, which overrides the optimizeIt() in FromTable.
        ** This assumes that the join strategy for which this join node is
        ** the inner table is a nested loop join, which will not be a valid
        ** assumption when we implement other strategies like materialization
        ** (hash join can work only on base tables).
        */

        /* RESOLVE - Need to figure out how to really optimize this node. */

        // RESOLVE: NEED TO SET ROW ORDERING OF SOURCES IN THE ROW ORDERING
        // THAT WAS PASSED IN.

        leftResultSet=optimizeSource(optimizer,leftResultSet,getLeftPredicateList(),null);

        /* Move all joinPredicates down to the right.
         * RESOLVE - When we consider the reverse join order then
         * we will have to pull them back up and then push them
         * down to the other side when considering the reverse
         * join order.
         * RESOLVE - This logic needs to be looked at when we
         * implement full outer join.
         */
        // Walk joinPredicates backwards due to possible deletes


        for(int index=joinPredicates.size()-1;index>=0;index--){
            Predicate predicate;

            predicate=joinPredicates.elementAt(index);
            if(!predicate.getPushable()){
                continue;
            }
            joinPredicates.removeElementAt(index);
            optimizer.tracer().trace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION,0,0,0.0,
                                     "JoinNode pushing predicate right.",predicate);
            getRightPredicateList().addElement(predicate);
        }

        CostEstimate lrsCE = leftResultSet.getCostEstimate();
        if (this instanceof FullOuterJoinNode)
            lrsCE.setJoinType(FULLOUTERJOIN);
        else if (this instanceof HalfOuterJoinNode)
            lrsCE.setJoinType(LEFTOUTERJOIN);
        else
            lrsCE.setJoinType(INNERJOIN);
        double savedAccumulatedMemory = lrsCE.getAccumulatedMemory();
        lrsCE.setAccumulatedMemory(leftOptimizer.getAccumulatedMemory());
        rightResultSet=optimizeSource(optimizer,rightResultSet,getRightPredicateList(),lrsCE);
        lrsCE.setJoinType(INNERJOIN);
        lrsCE.setAccumulatedMemory(savedAccumulatedMemory);
        costEstimate=getCostEstimate(optimizer);

        /*
        ** We add the costs for the inner and outer table, but the number
        ** of rows is that for the inner table only.
        */

        costEstimate.setCost(rightResultSet.getCostEstimate());
        costEstimate.setBase(null);
        /*
        ** Some types of joins (e.g. outer joins) will return a different
        ** number of rows than is predicted by optimizeIt() in JoinNode.
        ** So, adjust this value now. This method does nothing for most
        ** join types.
        */

        /* Stupid JL
        adjustNumberOfRowsReturned(costEstimate);
        */


        /*
        ** Get the cost of this result set in the context of the whole plan.
        */
        // when we get here, we are costing the whole JoinNode as the right of another
        // join, and whether the join is outer or inner has been carried in the
        // outerCost's joinType, we shouldn't change either the joinType for the
        // outerCost nor current costEstimate(JoinNode's cost) here.
        getCurrentAccessPath().getJoinStrategy().estimateCost(
                this, predList, null, outerCost, optimizer, costEstimate
        );

        // propagate the sortorder from the outer table if any
        // only do this if the current node is the first resultset in the join sequence as
        // we don't want to propagate sort order from right table of a join.
        RowOrdering joinResultRowOrdering = costEstimate.getRowOrdering();
        // for full outer join,we cannot preserve the row ordering due to the non-matching rows
        // from right, so add both left and right table to the unordered list
        if (!(this instanceof FullOuterJoinNode) &&
                (outerCost.isUninitialized() || (outerCost.localCost()==0d && outerCost.getEstimatedRowCount()==1.0)) &&
                joinResultRowOrdering != null) {
            joinResultRowOrdering.mergeTo(optimizer.getCurrentRowOrdering());
        } else {
            // add the left table to the unordered list
            OptimizableList leftList = leftOptimizer.getOptimizableList();
            for (int i=0; i< leftList.size(); i++)
                optimizer.getCurrentRowOrdering().addUnorderedOptimizable(leftList.getOptimizable(i));
        }
        // add the right table always to the unordered list, as we don't propagate sort order from the right table
        OptimizableList rightList = rightOptimizer.getOptimizableList();
        for (int i=0; i< rightList.size(); i++)
            optimizer.getCurrentRowOrdering().addUnorderedOptimizable(rightList.getOptimizable(i));

        optimizer.considerCost(this,predList,costEstimate,outerCost);

        /* Optimize subqueries only once, no matter how many times we're called */
        if((!optimized) && (subqueryList!=null)){
            /* RESOLVE - Need to figure out how to really optimize this node.
             * Also need to figure out the pushing of the joinClause.
             */
            subqueryList.optimize(optimizer.getDataDictionary(), costEstimate.rowCount(), optimizer.isForSpark());
        }

        optimized=true;

        return costEstimate;
    }

    @Override
    public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate) throws StandardException{
        assert optimizablePredicate instanceof Predicate:"optimizablePredicate expected to be instanceof Predicate";
        assert !optimizablePredicate.hasSubquery() && !optimizablePredicate.hasMethodCall():
                "optimizablePredicate either has a subquery or a method call";

        optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION, 0, 0, 0.0,
                      "JoinNode pushing join predicate.", optimizablePredicate);
        /* Add the matching predicate to the joinPredicates */
        joinPredicates.addPredicate((Predicate)optimizablePredicate);

        /* Remap all of the ColumnReferences to point to the
         * source of the values.
         */
        RemapCRsVisitor rcrv=new RemapCRsVisitor(true);
        ((Predicate)optimizablePredicate).getAndNode().accept(rcrv);

        return true;
    }

    /**
     * (Splice method) Whereas pushOptPredicate assumes the predicate to add
     * is coming from higher in the tree & therefore its column references
     * need to be remapped, this method simply adds the predicate to the node
     * without modification.
     *
     * @param optimizablePredicate the predicate to add
     * @return boolean
     * @throws StandardException
     */
    public boolean addOptPredicate(OptimizablePredicate optimizablePredicate) throws StandardException{
        assert optimizablePredicate instanceof Predicate:"optimizablePredicate expected to be instanceof Predicate";
        assert !optimizablePredicate.hasSubquery() && !optimizablePredicate.hasMethodCall():
                "optimizablePredicate either has a subquery or a method call";

        optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION,0,0,0.0,"JoinNode adding join predicate.",optimizablePredicate);
        joinPredicates.addPredicate((Predicate)optimizablePredicate);

        return true;
    }

    @Override
    public Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException{
        // modify access path for On clause subqueries
        if (subqueryList != null)
            subqueryList.modifyAccessPaths();

        super.modifyAccessPath(outerTables);

        /* By the time we're done here, both the left and right
         * predicate lists should be empty because we pushed everything
         * down.
         */
        if(SanityManager.DEBUG){
            if(!getLeftPredicateList().isEmpty()){
                SanityManager.THROWASSERT(
                        "getLeftPredicateList().size() expected to be 0, not "+
                                getLeftPredicateList().size());
            }

            if(!getRightPredicateList().isEmpty()){
                SanityManager.THROWASSERT(
                        "getRightPredicateList().size() expected to be 0, not "+
                                getRightPredicateList().size());
            }
        }

        return this;
    }

    @Override
    public ResultColumnList getAllResultColumns(TableName allTableName) throws StandardException{
        /* We need special processing when there is a USING clause.
          * The resulting table will be the join columns from
         * the outer table followed by the non-join columns from
         * left side plus the non-join columns from the right side.
         */
        if(usingClause==null){
            return getAllResultColumnsNoUsing(allTableName);
        }

        /* Get the logical left side of the join.
         * This is where the join columns come from.
         * (For RIGHT OUTER JOIN, the left is the right
         * and the right is the left and the JOIN is the NIOJ).
         */
        ResultSetNode logicalLeftRS=getLogicalLeftResultSet();

        // Get the join columns
        ResultColumnList joinRCL=logicalLeftRS.getAllResultColumns(null).getJoinColumns(usingClause);

        // Get the left and right RCLs
        ResultColumnList leftRCL=leftResultSet.getAllResultColumns(allTableName);
        ResultColumnList rightRCL=rightResultSet.getAllResultColumns(allTableName);

        /* Chop the join columns out of the both left and right.
         * Thanks to the ANSI committee, the join columns
         * do not belong to either table.
         */
        if(leftRCL!=null){
            leftRCL.removeJoinColumns(usingClause);
        }
        if(rightRCL!=null){
            rightRCL.removeJoinColumns(usingClause);
        }

        /* If allTableName is null, then we want to return the splicing
         * of the join columns followed by the non-join columns from
         * the left followed by the non-join columns from the right.
         * If not, then at most 1 side should match.
         * NOTE: We need to make sure that the RC's VirtualColumnIds
         * are correct (1 .. size).
         */
        if(leftRCL==null){
            if(rightRCL==null){
                // Both sides are null. This only happens if allTableName is
                // non-null and doesn't match the table name of any of the
                // join tables (DERBY-4414).
                return null;
            }
            rightRCL.resetVirtualColumnIds();
            return rightRCL;
        }else if(rightRCL==null){
            // leftRCL is non-null, otherwise the previous leg of the if
            // statement would have been chosen.
            leftRCL.resetVirtualColumnIds();
            return leftRCL;
        }else{
            /* Both sides are non-null.  This should only happen
             * if allTableName is null.
             */
            if(SanityManager.DEBUG){
                SanityManager.ASSERT(allTableName==null,"alltableName ("+allTableName+") expected to be null");
            }
            joinRCL.destructiveAppend(leftRCL);
            joinRCL.destructiveAppend(rightRCL);
            joinRCL.resetVirtualColumnIds();
            return joinRCL;
        }
    }

    /**
     * Try to find a ResultColumn in the table represented by this FromTable
     * that matches the name in the given ColumnReference.
     *
     * @param columnReference The columnReference whose name we're looking
     *                        for in the given table.
     * @return A ResultColumn whose expression is the ColumnNode
     * that matches the ColumnReference.
     * Returns null if there is no match.
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultColumn getMatchingColumn(ColumnReference columnReference) throws StandardException{
        /* Get the logical left and right sides of the join.
         * (For RIGHT OUTER JOIN, the left is the right
         * and the right is the left and the JOIN is the NIOJ).
         */
        ResultSetNode logicalLeftRS=getLogicalLeftResultSet();
        ResultSetNode logicalRightRS=getLogicalRightResultSet();
        ResultColumn resultColumn=null;
        ResultColumn rightRC=null;
        ResultColumn usingRC=null;

        ResultColumn leftRC=logicalLeftRS.getMatchingColumn(columnReference);

        if(leftRC!=null){
            resultColumn=leftRC;

            if(this instanceof FullOuterJoinNode)
                leftRC.setNullability(true);

            /* Find out if the column is in the using clause */
            if(usingClause!=null){
                usingRC=usingClause.getResultColumn(leftRC.getName());
            }
        }

        /* We only search on the right if the column isn't in the
         * USING clause.
         */
        if(usingRC==null){
            rightRC=logicalRightRS.getMatchingColumn(columnReference);
        }else{
            //If this column represents the join column from the
            // right table for predicate generated for USING/NATURAL
            // of RIGHT OUTER JOIN then flag it such by setting
            // rightOuterJoinUsingClause to true.
            // eg
            //     select c from t1 right join t2 using (c)
            //For "using(c)", a join predicate will be created as
            // follows t1.c=t2.c
            //We are talking about column t2.c of the join predicate.
            if(this instanceof HalfOuterJoinNode && ((HalfOuterJoinNode)this).isRightOuterJoin()){
                leftRC.setRightOuterJoinUsingClause(true);
            }
        }

        if(rightRC!=null){
            /* We must catch ambiguous column references for joins here,
             * since FromList only checks for ambiguous references between
             * nodes, not within a node.
             */
            if(leftRC!=null){
                throw StandardException.newException(SQLState.LANG_AMBIGUOUS_COLUMN_NAME,columnReference.getSQLColumnName());
            }

            // All columns on the logical right side of a "half" outer join
            // can contain nulls. The correct nullability is set by
            // bindResultColumns()/buildRCL(). However, if bindResultColumns()
            // has not been called yet, the caller of this method will see
            // the wrong nullability. This problem is logged as DERBY-2916.
            // Until that's fixed, set the nullability here too.
            if(this instanceof HalfOuterJoinNode || this instanceof FullOuterJoinNode){
                rightRC.setNullability(true);
            }

            resultColumn=rightRC;
        }

        /* Insert will bind the underlying result sets which have
         * tables twice. On the 2nd bind, resultColumns != null,
         * we must return the RC from the JoinNode's RCL which is above
         * the RC that we just found above.  (Otherwise, the source
         * for the ColumnReference will be from the wrong ResultSet
         * at generate().)
         */
        if(resultColumns!=null){
            int rclSize=resultColumns.size();
            for(int index=0;index<rclSize;index++){
                ResultColumn rc=resultColumns.elementAt(index);
                VirtualColumnNode vcn=(VirtualColumnNode)rc.getExpression();
                if(resultColumn==vcn.getSourceColumn()){
                    resultColumn=rc;
                    break;
                }
            }
        }

        return resultColumn;
    }

    /**
     * Bind the expressions under this node.
     */
    @Override
    public void bindExpressions(FromList fromListParam) throws StandardException{
        super.bindExpressions(fromListParam);

        // Now that both the left and the right side of the join have been
        // bound, we know the column names and can transform a natural join
        // into a join with a USING clause.
        if(naturalJoin){
            usingClause=getCommonColumnsForNaturalJoin();
        }
    }

    /**
     * Bind the result columns of this ResultSetNode when there is no
     * base table to bind them to.  This is useful for SELECT statements,
     * where the result columns get their types from the expressions that
     * live under them.
     *
     * @param fromListParam FromList to use/append to.
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindResultColumns(FromList fromListParam) throws StandardException{
        super.bindResultColumns(fromListParam);

        /* Now we build our RCL */
        buildRCL();

        /* We cannot bind the join clause until after we've bound our
         * result columns. This is because the resultColumns from the
         * children are propagated and merged to create our resultColumns
         * in super.bindRCs().  If we bind the join clause prior to that
         * call, then the ColumnReferences in the join clause will point
         * to the children's RCLs at the time that they are bound, but
         * will end up pointing above themselves, to our resultColumns,
         * after the call to super.bindRCS().
         */
        deferredBindExpressions(fromListParam);
    }

    /**
     * Bind the result columns for this ResultSetNode to a base table.
     * This is useful for INSERT and UPDATE statements, where the
     * result columns get their types from the table being updated or
     * inserted into.
     * If a result column list is specified, then the verification that the
     * result column list does not contain any duplicates will be done when
     * binding them by name.
     *
     * @param targetTableDescriptor The TableDescriptor for the table being
     *                              updated or inserted into
     * @param targetColumnList      For INSERT statements, the user
     *                              does not have to supply column
     *                              names (for example, "insert into t
     *                              values (1,2,3)".  When this
     *                              parameter is null, it means that
     *                              the user did not supply column
     *                              names, and so the binding should
     *                              be done based on order.  When it
     *                              is not null, it means do the binding
     *                              by name, not position.
     * @param statement             Calling DMLStatementNode (Insert or Update)
     * @param fromListParam         FromList to use/append to.
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindResultColumns(TableDescriptor targetTableDescriptor,
                                  FromVTI targetVTI,
                                  ResultColumnList targetColumnList,
                                  DMLStatementNode statement,
                                  FromList fromListParam) throws StandardException{
        super.bindResultColumns(targetTableDescriptor,targetVTI,targetColumnList,statement,fromListParam);

        /* Now we build our RCL */
        buildRCL();

        /* We cannot bind the join clause until after we've bound our
         * result columns. This is because the resultColumns from the
         * children are propagated and merged to create our resultColumns
         * in super.bindRCs().  If we bind the join clause prior to that
         * call, then the ColumnReferences in the join clause will point
         * to the children's RCLs at the time that they are bound, but
         * will end up pointing above themselves, to our resultColumns,
         * after the call to super.bindRCS().
         */
        deferredBindExpressions(fromListParam);
    }

    /**
     * Put a ProjectRestrictNode on top of each FromTable in the FromList.
     * ColumnReferences must continue to point to the same ResultColumn, so
     * that ResultColumn must percolate up to the new PRN.  However,
     * that ResultColumn will point to a new expression, a VirtualColumnNode,
     * which points to the FromTable and the ResultColumn that is the source for
     * the ColumnReference.
     * (The new PRN will have the original of the ResultColumnList and
     * the ResultColumns from that list.  The FromTable will get shallow copies
     * of the ResultColumnList and its ResultColumns.  ResultColumn.expression
     * will remain at the FromTable, with the PRN getting a new
     * VirtualColumnNode for each ResultColumn.expression.)
     * We then project out the non-referenced columns.  If there are no referenced
     * columns, then the PRN's ResultColumnList will consist of a single ResultColumn
     * whose expression is 1.
     *
     * @param numTables Number of tables in the DML Statement
     * @param gbl       The group by list, if any
     * @param fromList  The from list, if any
     * @return The generated ProjectRestrictNode atop the original FromTable.
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultSetNode preprocess(int numTables,GroupByList gbl,FromList fromList) throws StandardException{
        ResultSetNode newTreeTop;

        newTreeTop=super.preprocess(numTables,gbl,fromList);

        // delay preprocess of the join clause for flattenable inner join nodes
        // till after the flattening of JoinNode.
        // this way, we have the opportunity to flatten ON clause subquery for inner joins
        if (!delayPreprocessingJoinClause()) {
            preprocessJoinClause(numTables, (FromList) getNodeFactory().getNode(
                    C_NodeTypes.FROM_LIST,
                    getNodeFactory().doJoinOrderOptimization(),
                    getContextManager()),
                    (SubqueryList) getNodeFactory().getNode(
                            C_NodeTypes.SUBQUERY_LIST,
                            getContextManager()),
                    (PredicateList) getNodeFactory().getNode(
                            C_NodeTypes.PREDICATE_LIST,
                            getContextManager()));
        }

        return newTreeTop;
    }


    protected void preprocessJoinClause(int numTables,
                                      FromList outerFromList,
                                      SubqueryList outerSubqueryList,
                                      PredicateList outerPredicateList) throws StandardException{
        /* Put the expression trees in conjunctive normal form.
         * NOTE - This needs to occur before we preprocess the subqueries
         * because the subquery transformations assume that any subquery operator
         * negation has already occurred.
         */
        if(joinClause!=null){
            normExpressions();

            /* Preprocess any subqueries in the join clause */
            if(subqueryList!=null){
                joinClause.preprocess(
                        numTables,
                        outerFromList,
                        outerSubqueryList,
                        outerPredicateList);
            }

            /* Pull apart the expression trees */
            joinPredicates.pullExpressions(numTables,joinClause);
            joinPredicates.categorize();
            optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION, 0, 0, 0.0,
                    "JoinNode pulled join expressions.", joinPredicates);

            if (this instanceof FullOuterJoinNode) {
                // mark join condition as forFullJoin
                for(int index=joinPredicates.size()-1;index>=0;index--){
                    Predicate predicate;

                    predicate=joinPredicates.elementAt(index);
                    predicate.markFullJoinPredicate(true);
                }
            }
            joinClause=null;
        }
    }
    /**
     * Put the expression trees in conjunctive normal form
     *
     * @throws StandardException Thrown on error
     */
    public void normExpressions() throws StandardException{
        if(joinClauseNormalized) return;

        /* For each expression tree:
         *    o Eliminate NOTs (eliminateNots())
         *    o Ensure that there is an AndNode on top of every
         *      top level expression. (putAndsOnTop())
         *    o Finish the job (changeToCNF())
         */
        joinClause=joinClause.eliminateNots(false);
        if(SanityManager.DEBUG){
            if(!(joinClause.verifyEliminateNots())){
                joinClause.treePrint();
                SanityManager.THROWASSERT(
                        "joinClause in invalid form: "+joinClause);
            }
        }
        joinClause=joinClause.putAndsOnTop();
        if(SanityManager.DEBUG){
            if(!((joinClause instanceof AndNode) &&
                    (joinClause.verifyPutAndsOnTop()))){
                joinClause.treePrint();
                SanityManager.THROWASSERT(
                        "joinClause in invalid form: "+joinClause);
            }
        }
        /* For subquery in the ON clause of an inner join that could be flattened,
           we know it will later be moved to WHERE clause and we know how to flatten it,
           so use delayPreprocessingJoinClause() to determine if we should pass down true or false.
         */
        joinClause=joinClause.changeToCNF(delayPreprocessingJoinClause());
        if(SanityManager.DEBUG){
            if(!((joinClause instanceof AndNode) &&
                    (joinClause.verifyChangeToCNF()))){
                joinClause.treePrint();
                SanityManager.THROWASSERT(
                        "joinClause in invalid form: "+joinClause);
            }
        }

        joinClauseNormalized=true;
    }

    /**
     * Push expressions down to the first ResultSetNode which can do expression
     * evaluation and has the same referenced table map.
     * RESOLVE - This means only pushing down single table expressions to
     * DistinctNodes today.  Once we have a better understanding of how
     * the optimizer will work, we can push down join clauses.
     *
     * @param outerPredicateList The PredicateList from the outer RS.
     * @throws StandardException Thrown on error
     */
    @Override
    public void pushExpressions(PredicateList outerPredicateList) throws StandardException{
        FromTable leftFromTable=(FromTable)leftResultSet;
        FromTable rightFromTable=(FromTable)rightResultSet;

        /* OuterJoinNodes are responsible for overriding this
         * method since they have different rules about where predicates
         * can be applied.
         */
        if(SanityManager.DEBUG){
            if(this instanceof HalfOuterJoinNode || this instanceof FullOuterJoinNode){
                SanityManager.THROWASSERT(
                        "JN.pushExpressions() not expected to be called for "+
                                getClass().getName());
            }
        }

        /* We try to push "pushable"
         * predicates to 1 of 3 places:
         *    o Predicates that only reference tables
         *      on the left are pushed to the leftPredicateList.
         *    o Predicates that only reference tables
         *      on the right are pushed to the rightPredicateList.
         *    o Predicates which reference tables on both
         *      sides (and no others) are pushed to
         *      the joinPredicates and may be pushed down
         *      further during optimization.
         */
        // Left only
        pushExpressionsToLeft(outerPredicateList);
        leftFromTable.pushExpressions(getLeftPredicateList());
        // Right only
        pushExpressionsToRight(outerPredicateList);
        rightFromTable.pushExpressions(getRightPredicateList());
        // Join predicates
        grabJoinPredicates(outerPredicateList);

        /* By the time we're done here, both the left and right
         * predicate lists should be empty because we pushed everything
         * down.
         */
        if(SanityManager.DEBUG){
            if(!getLeftPredicateList().isEmpty()){
                SanityManager.THROWASSERT(
                        "getLeftPredicateList().size() expected to be 0, not "+
                                getLeftPredicateList().size());
            }
            if(!getRightPredicateList().isEmpty()){
                SanityManager.THROWASSERT(
                        "getRightPredicateList().size() expected to be 0, not "+
                                getRightPredicateList().size());
            }
        }
    }

    /**
     * Flatten this JoinNode into the outer query block. The steps in
     * flattening are:
     * o  Mark all ResultColumns as redundant, so that they are "skipped over"
     * at generate().
     * o  Append the joinPredicates to the outer list.
     * o  Create a FromList from the tables being joined and return
     * that list so that the caller will merge the 2 lists
     *
     * @param rcl          The RCL from the outer query
     * @param outerPList   PredicateList to append wherePredicates to.
     * @param sql          The SubqueryList from the outer query
     * @param gbl          The group by list, if any
     * @param havingClause The HAVING clause, if any
     * @param numTables     maximum number of tables in the query
     * @return FromList        The fromList from the underlying SelectNode.
     * @throws StandardException Thrown on error
     */
    @Override
    public FromList flatten(ResultColumnList rcl,
                            PredicateList outerPList,
                            SubqueryList sql,
                            GroupByList gbl,
                            ValueNode havingClause,
                            int numTables) throws StandardException{
        /* FullOuterJoinNodes should never get here.
         * (They can be transformed, but never
         * flattened directly.)
         */
        if(SanityManager.DEBUG){
            if(this instanceof FullOuterJoinNode){
                SanityManager.THROWASSERT(
                        "JN.flatten() not expected to be called for "+
                                getClass().getName());
            }
        }

        /* Build a new FromList composed of left and right children
         * NOTE: We must call FL.addElement() instead of FL.addFromTable()
         * since there is no exposed name. (And even if there was,
         * we could care less about unique exposed name checking here.)
         */
        FromList fromList=(FromList)getNodeFactory().getNode(
                C_NodeTypes.FROM_LIST,
                getNodeFactory().doJoinOrderOptimization(),
                getContextManager());
        fromList.addElement(leftResultSet);
        fromList.addElement(rightResultSet);

        /* Mark our RCL as redundant */
        resultColumns.setRedundant();

        /* Remap all ColumnReferences from the outer query to this node.
         * (We replace those ColumnReferences with clones of the matching
         * expression in the left and right's RCL.
         */
        rcl.remapColumnReferencesToExpressions();
        outerPList.remapColumnReferencesToExpressions();
        if(gbl!=null){
            gbl.remapColumnReferencesToExpressions();
        }

        if(havingClause!=null){
            havingClause.remapColumnReferencesToExpressions();
        }

        if (delayPreprocessingJoinClause()) {
            // at this point, we haven't preprocessed the join clause yet,
            // so joinPredicates at this point is still an empty list.
            // Preprocess the join clause now and flatten subquery if any
            preprocessJoinClause(numTables, fromList, subqueryList, joinPredicates);
        }

        if(!joinPredicates.isEmpty()){
            optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION,0,0,0.0,
                                     "JoinNode flattening join predicates to outer query.",joinPredicates);
            outerPList.destructiveAppend(joinPredicates);
        }

        if(subqueryList!=null && !subqueryList.isEmpty()){
            sql.destructiveAppend(subqueryList);
        }

        return fromList;
    }

    /**
     * Currently we don't reordering any outer join w/ inner joins.
     */
    @Override
    public boolean LOJ_reorderable(int numTables) throws StandardException{
        return false;
    }

    /**
     * Transform any Outer Join into an Inner Join where applicable.
     * (Based on the existence of a null intolerant
     * predicate on the inner table.)
     *
     * @param predicateTree The predicate tree for the query block
     * @return The new tree top (OuterJoin or InnerJoin).
     * @throws StandardException Thrown on error
     */
    @Override
    public FromTable transformOuterJoins(ValueNode predicateTree,int numTables) throws StandardException{
        /* Can't flatten if no predicates in where clause. */
        if(predicateTree==null){
            // DERBY-4712. Make sure any nested outer joins know we are non
            // flattenable, too, since they inform their left and right sides
            // which, is they are inner joins, a priori think they are
            // flattenable. If left/right result sets are not outer joins,
            // these next two calls are no-ops.
            leftResultSet = ((FromTable)leftResultSet).transformOuterJoins(null,numTables);
            rightResultSet = ((FromTable)rightResultSet).transformOuterJoins(null,numTables);
            return this;
        }

        /* See if left or right sides can be transformed */
        leftResultSet=((FromTable)leftResultSet).transformOuterJoins(predicateTree,numTables);
        rightResultSet=((FromTable)rightResultSet).transformOuterJoins(predicateTree,numTables);

        return this;
    }

    /**
     * For joins, the tree will be (nodes are left out if the clauses
     * are empty):
     * <p/>
     * ProjectRestrictResultSet -- for the having and the select list
     * SortResultSet -- for the group by list
     * ProjectRestrictResultSet -- for the where and the select list (if no group or having)
     * the result set for the fromList
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void generate(ActivationClassBuilder acb,MethodBuilder mb) throws StandardException{
        generateCore(acb,mb,INNERJOIN,null);
    }

    /**
     * Generate the code for a qualified join node.
     *
     * @throws StandardException Thrown on error
     */
    public void generateCore(ActivationClassBuilder acb,MethodBuilder mb,int joinType) throws StandardException{
        generateCore(acb,mb,joinType,joinClause);
    }

    /**
     * @return The final CostEstimate for this JoinNode, which is sum
     * the costs for the inner and outer table.  The number of rows,
     * though, is that for the inner table only.
     * @see ResultSetNode#getFinalCostEstimate
     * <p/>
     * Get the final CostEstimate for this JoinNode.
     */
    @Override
    public CostEstimate getFinalCostEstimate(boolean useSelf) throws StandardException{
        if (useSelf && trulyTheBestAccessPath != null) {
            return getTrulyTheBestAccessPath().getCostEstimate();
        }
        // If we already found it, just return it.
        if(finalCostEstimate!=null)
            return finalCostEstimate;

//        CostEstimate leftCE=leftResultSet.getFinalCostEstimate();
        CostEstimate rightCE=rightResultSet.getFinalCostEstimate(true);
        finalCostEstimate=getNewCostEstimate();
        finalCostEstimate.setCost(rightCE);
        finalCostEstimate.setBase(null);
        return finalCostEstimate;
    }

    /**
     * Get the lock mode for the target of an update statement
     * (a delete or update).  The update mode will always be row for
     * CurrentOfNodes.  It will be table if there is no where clause.
     *
     * @return The lock mode
     */
    @Override
    public int updateTargetLockMode(){
        /* Always use row locking if we have a join node.
         * We can only have a join node if there is a subquery that
         * got flattened, hence there is a restriction.
         */
        return TransactionController.MODE_RECORD;
    }

    /**
     * Is this FromTable a JoinNode which can be flattened into
     * the parents FromList.
     *
     * @return boolean        Whether or not this FromTable can be flattened.
     */
    @Override
    public boolean isFlattenableJoinNode(){
        return flattenableJoin && (outerJoinLevel == 0);
    }

    public boolean delayPreprocessingJoinClause() {
        return isFlattenableJoinNode();
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

            if(subqueryList!=null){
                printLabel(depth,"subqueryList: ");
                subqueryList.treePrint(depth+1);
            }

            if(joinClause!=null){
                printLabel(depth,"joinClause: ");
                joinClause.treePrint(depth+1);
            }

            if(!joinPredicates.isEmpty()){
                printLabel(depth,"joinPredicates: ");
                joinPredicates.treePrint(depth+1);
            }

            if(usingClause!=null){
                printLabel(depth,"usingClause: ");
                usingClause.treePrint(depth+1);
            }
        }
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException{
        super.acceptChildren(v);
        if(resultColumns!=null){
            resultColumns=(ResultColumnList)resultColumns.accept(v, this);
        }
        if(joinClause!=null){
            joinClause=(ValueNode)joinClause.accept(v, this);
        }
        if(usingClause!=null){
            usingClause=(ResultColumnList)usingClause.accept(v, this);
        }
        if(joinPredicates!=null){
            joinPredicates=(PredicateList)joinPredicates.accept(v, this);
        }
    }

    // This method returns the table references in Join node, and this may be
    // needed for LOJ reordering.  For example, we may have the following query:
    //       (T JOIN S) LOJ (X LOJ Y)
    // The top most LOJ may be a join betw T and X and thus we can reorder the
    // LOJs.  However, as of 10/2002, we don't reorder LOJ mixed with join.
    @Override
    public JBitSet LOJgetReferencedTables(int numTables) throws StandardException{
        JBitSet map=leftResultSet.LOJgetReferencedTables(numTables);
        if(map==null) return null;
        else map.or(rightResultSet.LOJgetReferencedTables(numTables));

        return map;
    }

    @Override
    public void assignResultSetNumber() throws StandardException{
        super.assignResultSetNumber();
        if(subqueryList!=null && !subqueryList.isEmpty()){
            subqueryList.setPointOfAttachment(resultSetNumber);
        }
    }

    /*
     * Splice addition of rebuildRCL method:
     *
     * Similar to existing buildRCL method, except buildRCL is private
     * and we need to be able to invoke this externally, in particular
     * from the splice visitor framework.
     * Also, buildRCL assumes the RCL has not been build yet, by returning
     * immediately if resultColumns != null, whereas here in rebuildRCL
     * we assume Derby has built it already and we need to force a rebuild.
     * We also need to set the result set number again on each
     * of the result columns, which buildRCL does not do.
     * Adding this explicit method to the Derby fork seemed less risky
     * than tweaking the existing buildRCL method or even just making
     * it public.
     */
    @SuppressWarnings("UnusedDeclaration")
    public void rebuildRCL() throws StandardException{
        assert resultColumns!=null;

        setResultColumns(null);
        buildRCL();
        resultColumns.setResultSetNumber(getResultSetNumber());
    }

    /**
     * Some types of joins (e.g. outer joins) will return a different
     * number of rows than is predicted by optimizeIt() in JoinNode.
     * So, adjust this value now. This method does nothing for most
     * join types.
     */
    protected void adjustNumberOfRowsReturned(CostEstimate costEstimate){
    }

    protected void pushExpressionsToLeft(PredicateList outerPredicateList) throws StandardException{
        FromTable leftFromTable=(FromTable)leftResultSet;

        JBitSet leftReferencedTableMap=leftFromTable.getReferencedTableMap();

        /* Build a list of the single table predicates on left result set
         * that we can push down
         */
        // Walk outerPredicateList backwards due to possible deletes
        for(int index=outerPredicateList.size()-1;index>=0;index--){
            JBitSet curBitSet;
            Predicate predicate;

            predicate=outerPredicateList.elementAt(index);
            if(!predicate.getPushable()){
                continue;
            }

            curBitSet=predicate.getReferencedSet();

            /* Do we have a match? */
            if(leftReferencedTableMap.contains(curBitSet)){
                /* Add the matching predicate to the push list */
                optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION, 0, 0, 0.0,
                              "JoinNode pushing outer predicate left.", predicate);
                getLeftPredicateList().addPredicate(predicate);

                /* Remap all of the ColumnReferences to point to the
                 * source of the values.
                 * The tree is something like:
                 *            PRN1
                 *              |
                 *             JN (this)
                 *           /    \
                 *        PRN2    PRN3
                 *        |       |
                 *        FBT1    FBT2
                 *
                 * The ColumnReferences start off pointing to the RCL off of
                 * PRN1.  For optimization, we want them to point to the
                 * RCL off of PRN2.  In order to do that, we remap them
                 * twice here.  If optimization pushes them down to the
                 * base table, it will remap them again.
                 */
                RemapCRsVisitor rcrv=new RemapCRsVisitor(true);
                predicate.getAndNode().accept(rcrv);
                predicate.getAndNode().accept(rcrv);

                /* Remove the matching predicate from the outer list */
                outerPredicateList.removeElementAt(index);
            }
        }
    }

    /**
     * Do the generation work for the join node hierarchy.
     *
     * @param acb        The ActivationClassBuilder
     * @param mb         the method the code is to go into
     * @param joinType   The join type
     * @param joinClause The join clause, if any
     * @throws StandardException Thrown on error
     */
    protected void generateCore(ActivationClassBuilder acb,
                                MethodBuilder mb,
                                int joinType,
                                ValueNode joinClause) throws StandardException{

        sparkExpressionTree = null;
        SparkExpressionNode tmpTree = null;
        JoinStrategy joinStrategy =
         ((Optimizable)rightResultSet).getTrulyTheBestAccessPath().
                                                 getJoinStrategy();
        JoinStrategy.JoinStrategyType joinStrategyType =
                                      joinStrategy.getJoinStrategyType();
        boolean eligibleForNativeSpark =
         (joinStrategyType == JoinStrategy.JoinStrategyType.MERGE_SORT ||
          joinStrategyType == JoinStrategy.JoinStrategyType.BROADCAST);

        if (joinPredicates != null && eligibleForNativeSpark) {
            for (int i = 0; i < joinPredicates.size(); i++) {
                tmpTree =
                  OperatorToString.
                    opToSparkExpressionTree(joinPredicates.elementAt(i).getAndNode(),
                                            leftResultSet.getResultSetNumber(),
                                            rightResultSet.getResultSetNumber());
                if (tmpTree == null) {
                    sparkExpressionTree = null;
                    break;
                }
                if (i == 0)
                    sparkExpressionTree = tmpTree;
                else
                    sparkExpressionTree =
                       SparkLogicalOperator.
                         getNewSparkLogicalOperator(AND, sparkExpressionTree, tmpTree);
            }
        }
        else
            sparkExpressionTree = null;

        /* Put the predicates back into the tree */
        if(joinPredicates!=null){
            // Pulling the equality predicate normal case from the restriction generation.
            if (rightHashKeys ==null || rightHashKeys.length != joinPredicates.size())
                joinClause=joinPredicates.restorePredicates();
  //          joinPredicates=null;
        }

        /* Get the next ResultSet #, so that we can number this ResultSetNode, its
         * ResultColumnList and ResultSet.
         */
        assignResultSetNumber();

        // build up the tree.

        /* Generate the JoinResultSet */
        /* Nested loop and hash are the only join strategy currently supporteds.
         * Right outer joins are transformed into left outer joins.
         */
        String joinResultSetString;

        if (joinType==FULLOUTERJOIN) {
            joinResultSetString=((Optimizable)rightResultSet).getTrulyTheBestAccessPath().
                    getJoinStrategy().fullOuterJoinResultSetMethodName();
        } else if(joinType==LEFTOUTERJOIN){
            joinResultSetString=((Optimizable)rightResultSet).getTrulyTheBestAccessPath().
                    getJoinStrategy().halfOuterJoinResultSetMethodName();
        }else{
            joinResultSetString=((Optimizable)rightResultSet).getTrulyTheBestAccessPath().
                    getJoinStrategy().joinResultSetMethodName();
        }

        acb.pushGetResultSetFactoryExpression(mb);
        int nargs=getJoinArguments(acb,mb,joinClause);

        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,joinResultSetString,ClassName.NoPutResultSet,nargs);
    }

    protected void oneRowRightSide(ActivationClassBuilder acb,MethodBuilder mb) throws StandardException{
        mb.push(rightResultSet.isOneRowResultSet());
        mb.push(((FromTable)rightResultSet).getSemiJoinType());  //join is for inclusion or exclusion join
    }

    /**
     * Return the number of arguments to the join result set.  This will
     * be overridden for other types of joins (for example, outer joins).
     */
    protected int getNumJoinArguments(){
        return 14;
    }

    /**
     * Generate    and add any arguments specifict to outer joins.
     * (Expected to be overriden, where appropriate, in subclasses.)
     *
     * @param acb The ActivationClassBuilder
     * @param mb  the method  the generated code is to go into
     *            <p/>
     *            return The number of args added
     * @throws StandardException Thrown on error
     */
    protected int addOuterJoinArguments(ActivationClassBuilder acb,MethodBuilder mb) throws StandardException{
        return 0;
    }

    protected PredicateList getLeftPredicateList() throws StandardException{
        if(leftPredicateList==null)
            leftPredicateList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,getContextManager());

        return leftPredicateList;
    }

    protected PredicateList getRightPredicateList() throws StandardException{
        if(rightPredicateList==null)
            rightPredicateList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,getContextManager());

        return rightPredicateList;
    }

    /**
     * Find the unreferenced result columns and project them out. This is used in pre-processing joins
     * that are not flattened into the where clause.
     */
    @Override
    void projectResultColumns() throws StandardException{
        leftResultSet.projectResultColumns();
        rightResultSet.projectResultColumns();
        if (!getCompilerContext().isProjectionPruningEnabled())
            resultColumns.pullVirtualIsReferenced();
        super.projectResultColumns();
    }

    /**
     * Mark this node and its children as not being a flattenable join.
     */
    @Override
    void notFlattenableJoin(){
        flattenableJoin=false;
        leftResultSet.notFlattenableJoin();
        rightResultSet.notFlattenableJoin();
    }

    /**
     * Return whether or not the underlying ResultSet tree
     * is ordered on the specified columns.
     * RESOLVE - This method currently only considers the outermost table
     * of the query block.
     *
     * @param crs             The specified ColumnReference[]
     * @param permuteOrdering Whether or not the order of the CRs in the array can be permuted
     * @param fbtVector       Vector that is to be filled with the FromBaseTable
     * @return Whether the underlying ResultSet tree
     * is ordered on the specified column.
     * @throws StandardException Thrown on error
     */
    @Override
    boolean isOrderedOn(ColumnReference[] crs,boolean permuteOrdering,Vector fbtVector) throws StandardException{
        /* RESOLVE - easiest thing for now is to only consider the leftmost child */
        return leftResultSet.isOrderedOn(crs,permuteOrdering,fbtVector);
    }

    void setSubqueryList(SubqueryList subqueryList){
        this.subqueryList=subqueryList;
    }

    void setAggregateVector(List<AggregateNode> aggregateVector){
        this.aggregateVector=aggregateVector;
    }

    /**
     * Flag this as a natural join so that an implicit USING clause will
     * be generated in the bind phase.
     */
    void setNaturalJoin(){
        naturalJoin=true;
    }

    /**
     * Return the logical left result set for this qualified
     * join node.
     * (For RIGHT OUTER JOIN, the left is the right
     * and the right is the left and the JOIN is the NIOJ).
     */
    ResultSetNode getLogicalLeftResultSet(){
        return leftResultSet;
    }

    /**
     * Return the logical right result set for this qualified
     * join node.
     * (For RIGHT OUTER JOIN, the left is the right
     * and the right is the left and the JOIN is the NIOJ).
     */
    ResultSetNode getLogicalRightResultSet(){
        return rightResultSet;
    }

    /**
     * Extract all the column names from a result column list.
     *
     * @param rcl the result column list to extract the names from
     * @return a list of all the column names in the RCL
     */
    private static List<String> extractColumnNames(ResultColumnList rcl){
        //noinspection Convert2Diamond
        List<String> names=new ArrayList<String>();

        for(int i=0;i<rcl.size();i++){
            ResultColumn rc=rcl.elementAt(i);
            names.add(rc.getName());
        }

        return names;
    }

    /**
     * Return a ResultColumnList with all of the columns in this table.
     * (Used in expanding '*'s.)
     * NOTE: Since this method is for expanding a "*" in the SELECT list,
     * ResultColumn.expression will be a ColumnReference.
     * NOTE: This method is handles the case when there is no USING clause.
     * The caller handles the case when there is a USING clause.
     *
     * @param allTableName The qualifier on the "*"
     * @return ResultColumnList    List of result columns from this table.
     * @throws StandardException Thrown on error
     */
    private ResultColumnList getAllResultColumnsNoUsing(TableName allTableName) throws StandardException{
        ResultColumnList leftRCL=leftResultSet.getAllResultColumns(allTableName);
        ResultColumnList rightRCL=rightResultSet.getAllResultColumns(allTableName);
        /* If allTableName is null, then we want to return the spliced
         * left and right RCLs.  If not, then at most 1 side should match.
         */
        if(leftRCL==null){
            return rightRCL;
        }else if(rightRCL==null){
            return leftRCL;
        }else{
            /* Both sides are non-null.  This should only happen
             * if allTableName is null.
             */
            assert allTableName==null:"alltableName ("+allTableName+") expected to be null";

            // Return a spliced copy of the 2 lists
            ResultColumnList tempList=(ResultColumnList)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN_LIST,
                    getContextManager());
            tempList.nondestructiveAppend(leftRCL);
            tempList.nondestructiveAppend(rightRCL);
            return tempList;
        }
    }

    /**
     * Build the RCL for this node.  We propagate the RCLs up from the children and splice them to form this node's RCL.
     */
    public void buildRCL() throws StandardException{
        /* NOTE - we only need to build this list if it does not already exist.  This can happen in the degenerate case
         * of an insert select with a join expression in a derived table within the select. */
        if(resultColumns!=null){
            return;
        }

        /* We get a shallow copy of the left's ResultColumnList and its ResultColumns. (Copy maintains
           ResultColumn.expression for now.) */
        resultColumns=leftResultSet.getResultColumns();

        ResultColumnList leftRCL=resultColumns.copyListAndObjects();
        leftResultSet.setResultColumns(leftRCL);
        for (int index = 0; index < resultColumns.size(); ++index) {
            ResultColumn rc = resultColumns.elementAt(index);
            rc.setFromLeftChild(true);
        }

        /* Replace ResultColumn.expression with new VirtualColumnNodes in the ProjectRestrictNode's ResultColumnList.
           (VirtualColumnNodes include pointers to source ResultSetNode, this, and source ResultColumn.) */
        resultColumns.genVirtualColumnNodes(leftResultSet,leftRCL,false);

        /* If this is a right outer join or full join, we can get nulls on the left side, so change the types of the left result set
           to be nullable. */
        if(this instanceof HalfOuterJoinNode && ((HalfOuterJoinNode)this).isRightOuterJoin() ||
           this instanceof FullOuterJoinNode){
            resultColumns.setNullability(true);
        }

        /* Now, repeat the process with the right's RCL */
        ResultColumnList tmpRCL=rightResultSet.getResultColumns();
        ResultColumnList rightRCL=tmpRCL.copyListAndObjects();
        rightResultSet.setResultColumns(rightRCL);

        for (int index = 0; index < tmpRCL.size(); ++index) {
            ResultColumn rc = tmpRCL.elementAt(index);
            rc.setFromLeftChild(false);
        }

        /* Replace ResultColumn.expression with new VirtualColumnNodes in the ProjectRestrictNode's ResultColumnList.
         * (VirtualColumnNodes include pointers to source ResultSetNode, this, and source ResultColumn.)
         */
        tmpRCL.genVirtualColumnNodes(rightResultSet,rightRCL,false);
        tmpRCL.adjustVirtualColumnIds(resultColumns.size());

       /* If this is a left outer join or full join, we can get nulls on the right side, so change the types of the right result set
        * to be nullable. */
        if(this instanceof HalfOuterJoinNode && !((HalfOuterJoinNode)this).isRightOuterJoin() ||
           this instanceof FullOuterJoinNode){
            tmpRCL.setNullability(true);
        }

        /* Now we append the propagated RCL from the right to the one from the left and call it our own. */
        resultColumns.nondestructiveAppend(tmpRCL);
    }

    private void deferredBindExpressions(FromList fromListParam) throws StandardException{
        /* Bind the expressions in the join clause */
        subqueryList=(SubqueryList)getNodeFactory().getNode(C_NodeTypes.SUBQUERY_LIST,getContextManager());
        //noinspection Convert2Diamond
        aggregateVector=new ArrayList<AggregateNode>();

        CompilerContext cc=getCompilerContext();

        /* ON clause */
        if(joinClause!=null){
            /* JoinNode.deferredBindExpressions() may be called again after the outer join rewrite
               optimization, at this stage, we don't want to simplify the ON clause predicate again, especially
               the top AND node with a boolean true.
             */
            if (!joinClauseNormalized) {
                Visitor constantExpressionVisitor =
                        new ConstantExpressionVisitor(SelectNode.class);
                joinClause = (ValueNode) joinClause.accept(constantExpressionVisitor);

                if (!getCompilerContext().getDisablePredicateSimplification()) {
                    Visitor predSimplVisitor =
                            new PredicateSimplificationVisitor(fromListParam,
                                    SelectNode.class);

                    joinClause = (ValueNode) joinClause.accept(predSimplVisitor);
                }
            }

            /* Create a new fromList with only left and right children before
             * binding the join clause. Valid column references in the join clause
             * are limited to columns from the 2 tables being joined. This
             * algorithm enforces that.
             */
            FromList fromList=(FromList)getNodeFactory().getNode(
                    C_NodeTypes.FROM_LIST,
                    getNodeFactory().doJoinOrderOptimization(),
                    getContextManager());
            fromList.addElement(leftResultSet);
            fromList.addElement(rightResultSet);

            int previousReliability=orReliability(CompilerContext.ON_CLAUSE_RESTRICTION);
            joinClause=joinClause.bindExpression(fromList,subqueryList,aggregateVector);
            cc.setReliability(previousReliability);

            // SQL 2003, section 7.7 SR 5
            SelectNode.checkNoWindowFunctions(joinClause,"ON");
            SelectNode.checkNoGroupingFunctions(joinClause,"ON");

            /*
            ** We cannot have aggregates in the ON clause.
            ** In the future, if we relax this, we'll need
            ** to be able to pass the aggregateVector up
            ** the tree.
            */
            if(!aggregateVector.isEmpty()){
                throw StandardException.newException(SQLState.LANG_NO_AGGREGATES_IN_ON_CLAUSE);
            }
        }
        /* USING clause */
        else if(usingClause!=null){
            /* Build a join clause from the usingClause, using the
             * exposed names in the left and right RSNs.
             * For each column in the list, we generate 2 ColumnReferences,
             * 1 for the left and 1 for the right.  We bind each of these
             * to the appropriate side and build an equality predicate
             * between the 2.  We bind the = and AND nodes by hand because
             * we have to bind the ColumnReferences a side at a time.
             * We need to bind the CRs a side at a time to ensure that
             * we don't find an bogus ambiguous column reference. (Bug 377)
             */
            joinClause=(ValueNode)getNodeFactory().getNode(
                    C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                    Boolean.TRUE,
                    getContextManager());

            int usingSize=usingClause.size();
            for(int index=0;index<usingSize;index++){
                BinaryComparisonOperatorNode equalsNode;
                ColumnReference leftCR;
                ColumnReference rightCR;
                ResultColumn rc=usingClause.elementAt(index);

                /* Create and bind the left CR */
                fromListParam.insertElementAt(leftResultSet,0);
                leftCR=(ColumnReference)getNodeFactory().getNode(
                        C_NodeTypes.COLUMN_REFERENCE,
                        rc.getName(),
                        ((FromTable)leftResultSet).getTableName(),
                        getContextManager());
                leftCR=(ColumnReference)leftCR.bindExpression(
                        fromListParam,subqueryList,
                        aggregateVector);
                fromListParam.removeElementAt(0);

                /* Create and bind the right CR */
                fromListParam.insertElementAt(rightResultSet,0);
                rightCR=(ColumnReference)getNodeFactory().getNode(
                        C_NodeTypes.COLUMN_REFERENCE,
                        rc.getName(),
                        ((FromTable)rightResultSet).getTableName(),
                        getContextManager());
                rightCR=(ColumnReference)rightCR.bindExpression(
                        fromListParam,subqueryList,
                        aggregateVector);
                fromListParam.removeElementAt(0);

                /* Create and insert the new = condition */
                equalsNode=(BinaryComparisonOperatorNode)
                        getNodeFactory().getNode(
                                C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
                                leftCR,
                                rightCR,
                                getContextManager());
                equalsNode.bindComparisonOperator();

                // Create a new join clause by ANDing the new = condition and
                // the old join clause.
                AndNode newJoinClause=(AndNode)getNodeFactory().getNode(
                        C_NodeTypes.AND_NODE,
                        equalsNode,
                        joinClause,
                        getContextManager());

                newJoinClause.postBindFixup();

                joinClause=newJoinClause;
            }
        }

        if(joinClause!=null){
            /* If joinClause is a parameter, (where ?), then we assume
             * it will be a nullable boolean.
             */
            if(joinClause.requiresTypeFromContext()){
                joinClause.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID,true));
            }

            /*
            ** Is the datatype of the JOIN clause BOOLEAN?
            **
            ** NOTE: This test is not necessary in SQL92 entry level, because
            ** it is syntactically impossible to have a non-Boolean JOIN clause
            ** in that level of the standard.  But we intend to extend the
            ** language to allow Boolean user functions in the JOIN clause,
            ** so we need to test for the error condition.
            */
            TypeId joinTypeId=joinClause.getTypeId();

            /* If the where clause is not a built-in type, then generate a bound
             * conversion tree to a built-in type.
             */
            if(joinTypeId.userType()){
                joinClause=joinClause.genSQLJavaSQLTree();
            }

            if(!joinClause.getTypeServices().getTypeId().equals(TypeId.BOOLEAN_ID)){
                throw StandardException.newException(SQLState.LANG_NON_BOOLEAN_JOIN_CLAUSE,
                        joinClause.getTypeServices().getTypeId().getSQLTypeName()
                );
            }
        }
    }

    /**
     * Generate a result column list with all the column names that appear on
     * both sides of the join operator. Those are the columns to use as join
     * columns in a natural join.
     *
     * @return RCL with all the common columns
     * @throws StandardException on error
     */
    private ResultColumnList getCommonColumnsForNaturalJoin() throws StandardException{
        ResultColumnList leftRCL=getLeftResultSet().getAllResultColumns(null);
        ResultColumnList rightRCL=getRightResultSet().getAllResultColumns(null);

        List<String> columnNames=extractColumnNames(leftRCL);
        columnNames.retainAll(extractColumnNames(rightRCL));

        ResultColumnList commonColumns=(ResultColumnList)getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN_LIST,
                getContextManager());

        for(String name : columnNames){
            ResultColumn rc=(ResultColumn)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,name,null,getContextManager());
            commonColumns.addResultColumn(rc);
        }

        return commonColumns;
    }

    private void pushExpressionsToRight(PredicateList outerPredicateList) throws StandardException{
        FromTable rightFromTable=(FromTable)rightResultSet;

        JBitSet rightReferencedTableMap=rightFromTable.getReferencedTableMap();

        /* Build a list of the single table predicates on right result set
         * that we can push down
         */
        // Walk outerPredicateList backwards due to possible deletes
        for(int index=outerPredicateList.size()-1;index>=0;index--){
            JBitSet curBitSet;
            Predicate predicate;

            predicate=outerPredicateList.elementAt(index);
            if(!predicate.getPushable()){
                continue;
            }

            curBitSet=predicate.getReferencedSet();

            /* Do we have a match? */
            if(rightReferencedTableMap.contains(curBitSet)){
                /* Add the matching predicate to the push list */
                optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION, 0, 0, 0.0,
                              "JoinNode pushing outer predicate right.", predicate);
                getRightPredicateList().addPredicate(predicate);

                /* Remap all of the ColumnReferences to point to the
                 * source of the values.
                 * The tree is something like:
                 *            PRN1
                 *              |
                 *             JN (this)
                 *           /    \
                 *        PRN2    PRN3
                 *        |       |
                 *        FBT1    FBT2
                 *
                 * The ColumnReferences start off pointing to the RCL off of
                 * PRN1.  For optimization, we want them to point to the
                 * RCL off of PRN3.  In order to do that, we remap them
                 * twice here.  If optimization pushes them down to the
                 * base table, it will remap them again.
                 */
                RemapCRsVisitor rcrv=new RemapCRsVisitor(true);
                predicate.getAndNode().accept(rcrv);
                predicate.getAndNode().accept(rcrv);

                /* Remove the matching predicate from the outer list */
                outerPredicateList.removeElementAt(index);
            }
        }
    }

    private void grabJoinPredicates(PredicateList outerPredicateList) throws StandardException{
        FromTable leftFromTable=(FromTable)leftResultSet;
        FromTable rightFromTable=(FromTable)rightResultSet;

        JBitSet leftReferencedTableMap=leftFromTable.getReferencedTableMap();
        JBitSet rightReferencedTableMap=rightFromTable.getReferencedTableMap();

        /* Build a list of the join predicates that we can push down */
        // Walk outerPredicateList backwards due to possible deletes
        for(int index=outerPredicateList.size()-1;index>=0;index--){
            JBitSet curBitSet;
            Predicate predicate;

            predicate=outerPredicateList.elementAt(index);
            if(!predicate.getPushable()){
                continue;
            }

            curBitSet=predicate.getReferencedSet();

            /* Do we have a match? */
            JBitSet innerBitSet=(JBitSet)rightReferencedTableMap.clone();
            innerBitSet.or(leftReferencedTableMap);
            if(innerBitSet.contains(curBitSet)){
                /* Add the matching predicate to the push list */
                optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION, 0, 0, 0.0,
                              "JoinNode pushing outer join predicate.", predicate);
                joinPredicates.addPredicate(predicate);

                /* Remap all of the ColumnReferences to point to the
                 * source of the values.
                 * The tree is something like:
                 *            PRN1
                 *              |
                 *             JN (this)
                 *           /    \
                 *        PRN2    PRN3
                 *        |       |
                 *        FBT1    FBT2
                 *
                 * The ColumnReferences start off pointing to the RCL off of
                 * PRN1.  For optimization, we want them to point to the
                 * RCL off of PRN2 or PRN3.  In order to do that, we remap them
                 * twice here.  If optimization pushes them down to the
                 * base table, it will remap them again.
                 */
                RemapCRsVisitor rcrv=new RemapCRsVisitor(true);
                predicate.getAndNode().accept(rcrv);
                predicate.getAndNode().accept(rcrv);

                /* Remove the matching predicate from the outer list */
                outerPredicateList.removeElementAt(index);
            }
        }
    }

    /**
     * Get the arguments to the join result set.
     *
     * @param acb        The ActivationClassBuilder for the class we're building.
     * @param mb         the method the generated code is going into
     * @param joinClause The join clause, if any
     * @return The array of arguments to the join result set
     * @throws StandardException Thrown on error
     */
    private int getJoinArguments(ActivationClassBuilder acb,
                                 MethodBuilder mb,
                                 ValueNode joinClause)
            throws StandardException{
        int numArgs=getNumJoinArguments();

        leftResultSet.generate(acb,mb); // arg 1
        mb.push(leftResultSet.resultColumns.size()); // arg 2

        if(isNestedLoopOverHashableJoin()){

            NonLocalColumnReferenceVisitor visitor=new NonLocalColumnReferenceVisitor();
            rightResultSet.accept(visitor);
            List nonLocalRefs=visitor.getNonLocalColumnRefs();

            ColumnMappingUtils.updateColumnMappings(leftResultSet.getResultColumns(),nonLocalRefs.iterator());
        }

        if(rightResultSet instanceof Optimizable && (isHashableJoin(rightResultSet) || isCrossJoin())){
            // Look for unary path to FromBaseTable, to prune preds from nonStoreRestrictionList
            FromBaseTable table=null;
            if(rightResultSet instanceof ProjectRestrictNode){
                ResultSetNode child=((ProjectRestrictNode)rightResultSet).getChildResult();
                if(child instanceof FromBaseTable){
                    table=(FromBaseTable)child;
                }else if(child instanceof IndexToBaseRowNode){
                    table=((IndexToBaseRowNode)child).source;
                }
            }else if(rightResultSet instanceof FromBaseTable){
                table=(FromBaseTable)rightResultSet;
            }
            // If found, clear join predicates, b/c they will be handled by the join
            if(table!=null){
                for(int i=table.nonStoreRestrictionList.size()-1;i>=0;i--){
                    Predicate op=(Predicate)table.nonStoreRestrictionList.getOptPredicate(i);
                    if(op.isJoinPredicate() || op.getPulled() || op.isFullJoinPredicate()){
                        table.nonStoreRestrictionList.removeOptPredicate(i);
                    }
                }
            }
        }

        rightResultSet.generate(acb,mb); // arg 3
        mb.push(rightResultSet.resultColumns.size()); // arg 4

        if(rightResultSet instanceof Optimizable &&
                (isHashableJoin(rightResultSet) || isCrossJoin())){
            // Add the left & right hash arrays to the method call
            int leftHashKeysItem=acb.addItem(FormatableIntHolder.getFormatableIntHolders(leftHashKeys));
            mb.push(leftHashKeysItem);
            numArgs++;

            int rightHashKeysItem=acb.addItem(FormatableIntHolder.getFormatableIntHolders(rightHashKeys));
            mb.push(rightHashKeysItem);
            numArgs++;

            //if it is merge join, we may be able to leverage the first qualified left row to combine with the existing scan start key of
            // right table, and further restrict the scan of the right table(this optimization is in AbstractMergeJionFlatMapFunction.initRightScan()).
            // There could be two scenarios that this optimization can be applied:
            // 1. right table is directly a TableScan or IndexScan(FromBaseTableNode)
            // 2. there is one or more ProjectRestrictNode on top of the TableScan/IndexScan, but the hash fields are a direct mapping from
            //    the base table fields
            // For both case, we need to compute a mapping of the rightHashKeys to the base table column to extend the scan start key on
            // the base table

            if (isMergeJoin()) {
                int[] rightHashKeyToBaseTableMap = mapRightHashKeysToBaseTableColumns(rightHashKeys, rightResultSet);
                int rightHashKeyToBaseTableMapItem = (rightHashKeyToBaseTableMap == null) ?
                        -1 :
                        acb.addItem(FormatableIntHolder.getFormatableIntHolders(rightHashKeyToBaseTableMap));
                mb.push(rightHashKeyToBaseTableMapItem);
                numArgs++;

                // pass down the sort order information for the right hash fields
                int rightHashKeySortOrderItem = (rightHashKeySortOrders == null) ?
                        -1 : acb.addItem(FormatableIntHolder.getFormatableIntHolders(rightHashKeySortOrders));
                mb.push(rightHashKeySortOrderItem);
                numArgs++;
            }

            if (isBroadcastJoin()) {
                boolean noCacheBroadcastJoinRight = ((Optimizable)rightResultSet).hasJoinPredicatePushedDownFromOuter();
                mb.push(noCacheBroadcastJoinRight);
                numArgs++;
            }

        }
        // Get our final cost estimate based on child estimates.
        costEstimate=getFinalCostEstimate(false);

        // for the join clause, we generate an exprFun
        // that evaluates the expression of the clause
        // against the current row of the child's result.
        // if the join clause is empty, we generate a function
        // that just returns true. (Performance tradeoff: have
        // this function for the empty join clause, or have
        // all non-empty join clauses check for a null at runtime).

        // generate the function and initializer:
        // Note: Boolean lets us return nulls (boolean would not)
        // private Boolean exprN()
        // {
        //   return <<joinClause.generate(ps)>>;
        // }
        // static Method exprN = method pointer to exprN;

        // if there is no join clause, we just pass a null Expression.
        if(joinClause==null){
            mb.pushNull(ClassName.GeneratedMethod); // arg 5
        }else{
            // this sets up the method and the static field.
            // generates:
            //     Object userExprFun { }
            MethodBuilder userExprFun=acb.newUserExprFun();

            // join clause knows it is returning its value;

            /* generates:
             *    return <joinClause.generate(acb)>;
             * and adds it to userExprFun
             */
            joinClause.generate(acb,userExprFun);
            userExprFun.methodReturn();

            // we are done modifying userExprFun, complete it.
            userExprFun.complete();

            // join clause is used in the final result set as an access of the new static
            // field holding a reference to this new method.
            // generates:
            //    ActivationClass.userExprFun
            // which is the static field that "points" to the userExprFun
            // that evaluates the where clause.
            acb.pushMethodReference(mb,userExprFun); // arg 5


            // Generate a method that returns the text format of a join predicate
            MethodBuilder userExprToStringFun=acb.newUserExprToStringFun();
            userExprToStringFun.push("TODOJL");
            userExprToStringFun.methodReturn();
            userExprToStringFun.complete();

        }

        mb.push(resultSetNumber); // arg 6

        addOuterJoinArguments(acb,mb);

        // Does right side return a single row
        oneRowRightSide(acb,mb);

        // Is the right from SSQ
        mb.push(rightResultSet.getFromSSQ());

        if (isCrossJoin()) {
            Optimizable rightRS = (Optimizable)rightResultSet;
            Properties rightProperties = rightRS.getProperties();
            String hintedValue = null;
            if (rightProperties != null) {
                hintedValue = rightProperties.getProperty("broadcastCrossRight");
            }
            if (hintedValue == null) {
                mb.push(rightRS.getTrulyTheBestAccessPath().getJoinStrategy().getBroadcastRight(rightResultSet.getFinalCostEstimate(true).getBase()));
            } else {
                mb.push(Boolean.parseBoolean(hintedValue));
            }
            numArgs++;
        }

        // estimated row count
        mb.push(costEstimate.rowCount());

        // estimated cost
        mb.push(costEstimate.getEstimatedCost());

        //User may have supplied optimizer overrides in the sql
        //Pass them onto execute phase so it can be shown in
        //run time statistics.
        if(joinOrderStrategyProperties!=null)
            mb.push(PropertyUtil.sortProperties(joinOrderStrategyProperties));
        else
            mb.pushNull("java.lang.String");

        mb.push(printExplainInformationForActivation());

        if (sparkExpressionTree != null) {
            String sparkExpressionTreeAsString =
                    Base64.encodeBase64String(SerializationUtils.serialize(sparkExpressionTree));
            mb.push(sparkExpressionTreeAsString);
        }
        else
            mb.pushNull("java.lang.String");

        return numArgs;
    }

    private boolean isNestedLoopOverHashableJoin(){
        boolean result=false;
        if(leftResultSet instanceof JoinNode){
            JoinNode leftChildJoinNode=(JoinNode)leftResultSet;
            result=isHashableJoin(leftChildJoinNode.rightResultSet) && !isHashableJoin(rightResultSet);
        }

        return result;
    }

    private boolean isHashableJoin(ResultSetNode node){
        boolean result=false;
        if(node instanceof Optimizable){
            Optimizable nodeOpt=(Optimizable)node;
            result=nodeOpt.getTrulyTheBestAccessPath().getJoinStrategy() instanceof HashableJoinStrategy;
        }

        return result;
    }

    public boolean isCrossJoin() {
        boolean result = false;
        if (rightResultSet instanceof Optimizable) {
            Optimizable nodeOpt=(Optimizable)rightResultSet;
            result= nodeOpt.getTrulyTheBestAccessPath().getJoinStrategy().getJoinStrategyType() == JoinStrategy.JoinStrategyType.CROSS;
        }
        return result;
    }

    public boolean
    isMergeJoin() {
        boolean result = false;
        if (rightResultSet instanceof Optimizable) {
            Optimizable nodeOpt=(Optimizable)rightResultSet;
            result=nodeOpt.getTrulyTheBestAccessPath().getJoinStrategy().getJoinStrategyType() == JoinStrategy.JoinStrategyType.MERGE;
        }
        return result;
    }

    public boolean isBroadcastJoin() {
        boolean result = false;
        if (rightResultSet instanceof Optimizable) {
            Optimizable nodeOpt=(Optimizable)rightResultSet;
            result=nodeOpt.getTrulyTheBestAccessPath().getJoinStrategy().getJoinStrategyType() == JoinStrategy.JoinStrategyType.BROADCAST;
        }
        return result;
    }

    @Override
    public String printExplainInformation(String attrDelim) throws StandardException {
        JoinStrategy joinStrategy = RSUtils.ap(this).getJoinStrategy();
        StringBuilder sb = new StringBuilder();
        sb.append(spaceToLevel())
                .append(joinStrategy.getJoinStrategyType().niceName()).append(rightResultSet.isNotExists()?"Anti":"").append("Join(")
                .append("n=").append(getResultSetNumber())
                .append(attrDelim).append(getFinalCostEstimate(false).prettyProcessingString(attrDelim));
        if (joinPredicates != null) {
            List<String> joinPreds = Lists.transform(PredicateUtils.PLtoList(joinPredicates), PredicateUtils.predToString);
            if (joinPreds != null && !joinPreds.isEmpty())
                sb.append(attrDelim).append("preds=[").append(Joiner.on(",").skipNulls().join(joinPreds)).append("]");
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public double getMemoryUsage4BroadcastJoin(){
        double memoryAccumulated = 0.0d;

        // Add the memory usage if the right table is joined with left through broadcast join.
        // There should only be one element in the right child of the JoinNode, which
        // could be a base table or some intermediate join result.
        Optimizable rightOptimizable = rightOptimizer.getOptimizableList().getOptimizable(0);
        assert rightOptimizable != null : "Right optimizable of a join is not expected to be null";

        AccessPath rightAP = rightOptimizable.getTrulyTheBestAccessPath();
        if (rightAP == null || rightAP.getJoinStrategy() == null ||
                rightAP.getJoinStrategy().getJoinStrategyType() != JoinStrategy.JoinStrategyType.BROADCAST)
            return memoryAccumulated;

        memoryAccumulated += rightAP.getCostEstimate().getBase().getEstimatedHeapSize();

        // Left table of the join could be a base table or another JoinNode. Unlike the right table,
        // if left table is another JoinNode, we need to drill down the JoinNode to continue accumulating
        // the memory consumption if they are done through consecutive broadcast joins
        // for example, given the following query, and assume both joins pick broadcast join:
        //    select 1 from t1 left join t2 on t1.a1=t2.a2 left join t3 on t1.a1=t3.a3;
        // the join nodes are organized as follows:
        //                          JoinNode
        //                             |
        //                    |---------------------|
        //                    |                     |
        //                   JoinNode               |
        //                    |                     |
        //           |---------------------|        |
        //           |                     |        t3
        //           t1                    t2
        // After collecting the memory usage for the broadcast join with t3, we should continue to look inside the
        // JoinNode on the left and accumulate the memory for the broadcast join with t2.
        //
        // In contrast, we should not walk recursively down the right branch. For example, given the following query,
        // and assume both joins pick broadcast joins:
        //   select 1 from --splice-properties joinOrder=fixed
        //   t1 left join (t2 left join t3 on t2.a2=t3.a3) on t1.a1=t3.a3;
        // the join nodes are organized as follows:
        //                          JoinNode
        //                             |
        //                    |---------------------|
        //                    |                     |
        //                    t1                JoinNode
        //                                          |
        //                                |---------------------|
        //                                |                     |
        //                                t2                    t3
        // In this case, even if we pick two broadcast joins, they should not be treated as consecutive,
        // and the two joins cannot be pipelined as the first case, as we have to wait for the whole join result
        // of (t2+=t3) to be available to start the broadcast join with t1.
        // The memory usage we pick here should be just from the broadcast join that joins the join result of t2,t3.
        memoryAccumulated += leftOptimizer.getAccumulatedMemory();

        return memoryAccumulated;
    }

    @Override
    protected boolean canBeOrdered(){
        return true;
    }

    public List<QueryTreeNode> collectReferencedColumns() throws StandardException {
        CollectingVisitor<QueryTreeNode> cnVisitor = new CollectingVisitor(
                Predicates.or(Predicates.instanceOf(ColumnReference.class),
                        Predicates.instanceOf(VirtualColumnNode.class)));
        // collect column references from different components

        if (joinClause != null)
            joinClause.accept(cnVisitor);

        for (int i=0; i<resultColumns.size(); i++) {
            ResultColumn rc = resultColumns.elementAt(i);
            if (rc.isReferenced())
                rc.accept(cnVisitor);
        }

        return cnVisitor.getCollected();

    }


    /**
     * prune the unreferenced result columns of FromSubquery node and FromBaseTable node
     */
    public Visitable projectionListPruning(boolean considerAllRCs) throws StandardException {
        // collect referenced columns.
        List<QueryTreeNode> refedcolmnList = collectReferencedColumns();

        // clear the referenced fields for both source tables
        leftResultSet.resultColumns.setColumnReferences(false, true);
        rightResultSet.resultColumns.setColumnReferences(false, true);

        markReferencedResultColumns(refedcolmnList);

        return this;
    }

    // map right table of merge join's hash key to the corresponding base table field (0-based)
    private int[] mapRightHashKeysToBaseTableColumns(int[] rightHashKeys, ResultSetNode rightResultSet) {
        int[] rightHashKeysToBaseTableMap = null;

        //get the FromBaseTableNode corresponding to the TableScan/IndexScan
        ResultSetNode baseTableNode = rightResultSet;
        while ((baseTableNode != null) && !(baseTableNode instanceof FromBaseTable)) {
            if (baseTableNode instanceof ProjectRestrictNode)
                baseTableNode = ((ProjectRestrictNode) baseTableNode).getChildResult();
            else {
                /* not a direct mapping, return immediately */
                return rightHashKeysToBaseTableMap;
            }
        }
        if (baseTableNode == null)
            return rightHashKeysToBaseTableMap;

        // get the mapping of target to source table field for the FromBaseTable node.
        FormatableBitSet referencedCols = ((FromBaseTable) baseTableNode).getReferencedCols();
        int[] colToBaseTableMap = new int[referencedCols.getNumBitsSet()];
        int count = 0;
        for (int i = referencedCols.anySetBit(); i >= 0; i = referencedCols.anySetBit(i)) {
            colToBaseTableMap[count] = i;
            count++;
        }

        rightHashKeysToBaseTableMap = new int[rightHashKeys.length];

        for (int i=0; i< rightHashKeys.length; i++) {
            rightHashKeysToBaseTableMap[i] = -1;
            ResultColumn rs = rightResultSet.getResultColumns().elementAt(rightHashKeys[i]);
            while (rs != null) {
                // we have arrived at a FromBaseTable node, and the hash field maps to a single base table field
                if (rs.getExpression() instanceof BaseColumnNode) {
                    rightHashKeysToBaseTableMap[i] = colToBaseTableMap[rs.getVirtualColumnId() - 1];
                    break;
                }

                ValueNode source = rs.getExpression();
                if (source instanceof VirtualColumnNode) {
                    rs = ((VirtualColumnNode)source).getSourceResultColumn();
                } else if (source instanceof ColumnReference) {
                    rs = ((ColumnReference)source).getSourceResultColumn();
                } else {
                    // for all other scenarios including expressions, it is not a direct mapping,
                    // so we will not continue the mapping.
                    break;
                }
            }
        }
        return rightHashKeysToBaseTableMap;
    }

    public void resetOptimized() {
        optimized = false;
    }
}
