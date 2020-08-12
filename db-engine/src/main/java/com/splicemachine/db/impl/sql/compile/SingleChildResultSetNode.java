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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.util.JBitSet;

import java.util.Collection;
import java.util.Vector;

/**
 * A SingleChildResultSetNode represents a result set with a single child.
 */

public abstract class SingleChildResultSetNode extends FromTable{
    /**
     * ResultSetNode under the SingleChildResultSetNode
     */
    protected ResultSetNode childResult;

    // Does this node have the truly... for the underlying tree
    protected boolean hasTrulyTheBestAccessPath;


    @Override
    public boolean isParallelizable(){
        return childResult.isParallelizable();
    }

    /**
     * Initialilzer for a SingleChildResultSetNode.
     *
     * @param childResult     The child ResultSetNode
     * @param tableProperties Properties list associated with the table
     */

    @Override
    public void init(Object childResult,Object tableProperties){
        /* correlationName is always null */
        super.init(null,tableProperties);
        this.childResult=(ResultSetNode)childResult;

        /* Propagate the child's referenced table map, if one exists */
        if(this.childResult.getReferencedTableMap()!=null){
            referencedTableMap=
                    (JBitSet)this.childResult.getReferencedTableMap().clone();
        }
    }

    @Override
    public AccessPath getTrulyTheBestAccessPath(){
        if(hasTrulyTheBestAccessPath){
            return super.getTrulyTheBestAccessPath();
        }

        if(childResult instanceof Optimizable)
            return ((Optimizable)childResult).getTrulyTheBestAccessPath();

        return super.getTrulyTheBestAccessPath();
    }

    @Override
    public double getMemoryUsage4BroadcastJoin(){
        if(hasTrulyTheBestAccessPath){
            return super.getMemoryUsage4BroadcastJoin();
        }

        if(childResult instanceof Optimizable)
            return ((Optimizable)childResult).getMemoryUsage4BroadcastJoin();

        return super.getMemoryUsage4BroadcastJoin();
    }

    /**
     * Return the childResult from this node.
     *
     * @return ResultSetNode    The childResult from this node.
     */
    public ResultSetNode getChildResult(){
        return childResult;
    }

    /**
     * Set the childResult for this node.
     *
     * @param childResult The new childResult for this node.
     */
    void setChildResult(ResultSetNode childResult){
        this.childResult=childResult;
    }

    @Override
    public void pullOptPredicates(OptimizablePredicateList optimizablePredicates) throws StandardException{
        if(childResult instanceof Optimizable){
            ((Optimizable)childResult).pullOptPredicates(optimizablePredicates);
        }
    }

    @Override
    public boolean forUpdate(){
        if(childResult instanceof Optimizable){
            return ((Optimizable)childResult).forUpdate();
        }else{
            return super.forUpdate();
        }
    }

    @Override
    public void initAccessPaths(Optimizer optimizer){
        super.initAccessPaths(optimizer);
        if(childResult instanceof Optimizable){
            ((Optimizable)childResult).initAccessPaths(optimizer);
        }
    }

    @Override
    public void resetAccessPaths() {
        super.resetAccessPaths();
        if (childResult instanceof Optimizable) {
            ((Optimizable)childResult).resetAccessPaths();
        }
    }

    /**
     * @see Optimizable#updateBestPlanMap
     * <p/>
     * Makes a call to add/load/remove a plan mapping for this node,
     * and then makes the necessary call to recurse on this node's
     * child, in order to ensure that we've handled the full plan
     * all the way down this node's subtree.
     */
    @Override
    public void updateBestPlanMap(short action,Object planKey) throws StandardException{
        super.updateBestPlanMap(action,planKey);

        // Now walk the child.  Note that if the child is not an
        // Optimizable and the call to child.getOptimizerImpl()
        // returns null, then that means we haven't tried to optimize
        // the child yet.  So in that case there's nothing to
        // add/load.

        if(childResult instanceof Optimizable){
            ((Optimizable)childResult).updateBestPlanMap(action,planKey);
        }else if(childResult.getOptimizerImpl()!=null){
            childResult.getOptimizerImpl().updateBestPlanMaps(action,planKey);
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

            if(childResult!=null){
                printLabel(depth,"childResult: ");
                childResult.treePrint(depth+1);
            }
        }
    }

    /**
     * Search to see if a query references the specifed table name.
     *
     * @param name      Table name (String) to search for.
     * @param baseTable Whether or not name is for a base table
     * @throws StandardException Thrown on error
     * @return true if found, else false
     */
    @Override
    public boolean referencesTarget(String name,boolean baseTable) throws StandardException{
        return childResult.referencesTarget(name,baseTable);
    }

    /**
     * Return true if the node references SESSION schema tables (temporary or permanent)
     *
     * @throws StandardException Thrown on error
     * @return true if references SESSION schema tables, else false
     */
    @Override
    public boolean referencesSessionSchema() throws StandardException{
        return childResult.referencesSessionSchema();
    }

    /**
     * Set the (query block) level (0-based) for this FromTable.
     *
     * @param level The query block level for this FromTable.
     */
    @Override
    public void setLevel(int level){
        super.setLevel(level);
        if(childResult instanceof FromTable){
            ((FromTable)childResult).setLevel(level);
        }
    }

    /**
     * Return whether or not this ResultSetNode contains a subquery with a
     * reference to the specified target.
     *
     * @param name      The table name.
     * @param baseTable Whether or not the name is for a base table.
     * @return boolean    Whether or not a reference to the table was found.
     * @throws StandardException Thrown on error
     */
    @Override
    boolean subqueryReferencesTarget(String name,boolean baseTable) throws StandardException{
        return childResult.subqueryReferencesTarget(name,baseTable);
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
    public ResultSetNode preprocess(int numTables, GroupByList gbl, FromList fromList) throws StandardException{
        childResult=childResult.preprocess(numTables,gbl,fromList);

        /* Build the referenced table map */
        referencedTableMap=(JBitSet)childResult.getReferencedTableMap().clone();

        return this;
    }

    /**
     * Add a new predicate to the list.  This is useful when doing subquery
     * transformations, when we build a new predicate with the left side of
     * the subquery operator and the subquery's result column.
     *
     * @param predicate The predicate to add
     * @return ResultSetNode    The new top of the tree.
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultSetNode addNewPredicate(Predicate predicate) throws StandardException{
        childResult=childResult.addNewPredicate(predicate);
        return this;
    }

    /**
     * Push expressions down to the first ResultSetNode which can do expression
     * evaluation and has the same referenced table map.
     * RESOLVE - This means only pushing down single table expressions to
     * DistinctNodes today.  Once we have a better understanding of how
     * the optimizer will work, we can push down join clauses.
     *
     * @param predicateList The PredicateList.
     * @throws StandardException Thrown on error
     */
    @Override
    public void pushExpressions(PredicateList predicateList) throws StandardException{
        if(childResult instanceof FromTable){
            ((FromTable)childResult).pushExpressions(predicateList);
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
        /* Flattening currently involves merging predicates and FromLists.
         * We don't have a FromList, so we can't flatten for now.
         */
        /* RESOLVE - this will introduce yet another unnecessary PRN */
        return false;
    }

    /**
     * Ensure that the top of the RSN tree has a PredicateList.
     *
     * @param numTables The number of tables in the query.
     * @return ResultSetNode    A RSN tree with a node which has a PredicateList on top.
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultSetNode ensurePredicateList(int numTables) throws StandardException{
        return this;
    }

    /**
     * Optimize this SingleChildResultSetNode.
     *
     * @param dataDictionary The DataDictionary to use for optimization
     * @param predicates     The PredicateList to optimize.  This should
     *                       be a join predicate.
     * @param outerRows      The number of outer joining rows
     * @param forSpark
     * @throws StandardException Thrown on error
     * @return ResultSetNode    The top of the optimized subtree
     */
    @Override
    public ResultSetNode optimize(DataDictionary dataDictionary,
                                  PredicateList predicates,
                                  double outerRows,
                                  boolean forSpark) throws StandardException{
        /* We need to implement this method since a NRSN can appear above a
         * SelectNode in a query tree.
         */
        childResult=childResult.optimize(
                dataDictionary,
                predicates,
                outerRows,
                forSpark);

        Optimizer optimizer= getOptimizer(
                (FromList)getNodeFactory().getNode(
                        C_NodeTypes.FROM_LIST,
                        getNodeFactory().doJoinOrderOptimization(),
                        getContextManager()),
                predicates,
                dataDictionary,
                null);
        optimizer.setForSpark(forSpark);
        costEstimate=optimizer.newCostEstimate();
        costEstimate.setCost(childResult.getCostEstimate().getEstimatedCost(),
                childResult.getCostEstimate().rowCount(),
                childResult.getCostEstimate().singleScanRowCount());

        return this;
    }

    @Override
    public ResultSetNode modifyAccessPaths() throws StandardException{
        childResult=childResult.modifyAccessPaths();

        return this;
    }

    @Override
    public ResultSetNode changeAccessPath(JBitSet joinedTableSet) throws StandardException{
        childResult=childResult.changeAccessPath(joinedTableSet);
        return this;
    }

    /**
     * Determine whether or not the specified name is an exposed name in
     * the current query block.
     *
     * @param name       The specified name to search for as an exposed name.
     * @param schemaName Schema name, if non-null.
     * @param exactMatch Whether or not we need an exact match on specified schema and table
     *                   names or match on table id.
     * @return The FromTable, if any, with the exposed name.
     * @throws StandardException Thrown on error
     */
    @Override
    protected FromTable getFromTableByName(String name,String schemaName,boolean exactMatch) throws StandardException{
        return childResult.getFromTableByName(name,schemaName,exactMatch);
    }

    /**
     * Decrement (query block) level (0-based) for this FromTable.
     * This is useful when flattening a subquery.
     *
     * @param decrement The amount to decrement by.
     */
    @Override
    void decrementLevel(int decrement){
        super.decrementLevel(decrement);
        childResult.decrementLevel(decrement);
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
        return childResult.updateTargetLockMode();
    }

    /**
     * Return whether or not the underlying ResultSet tree
     * is ordered on the specified columns.
     * RESOLVE - This method currently only considers the outermost table
     * of the query block.
     *
     * @throws StandardException Thrown on error
     * @param    crs                    The specified ColumnReference[]
     * @param    permuteOrdering        Whether or not the order of the CRs in the array can be permuted
     * @param    fbtVector            Vector that is to be filled with the FromBaseTable
     * @return Whether the underlying ResultSet tree
     * is ordered on the specified column.
     */
    @Override
    boolean isOrderedOn(ColumnReference[] crs,boolean permuteOrdering,Vector fbtVector) throws StandardException{
        return childResult.isOrderedOn(crs,permuteOrdering,fbtVector);
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
        // Default is false
        return childResult.isOneRowResultSet();
    }

    /**
     * Return whether or not the underlying ResultSet tree is for a NOT EXISTS join.
     *
     * @return Whether or not the underlying ResultSet tree is for a NOT EXISTS.
     */
    @Override
    public boolean isNotExists(){
        return childResult.isNotExists();
    }

    /**
     * Determine whether we need to do reflection in order to do the projection.
     * Reflection is only needed if there is at least 1 column which is not
     * simply selecting the source column.
     *
     * @return Whether or not we need to do reflection in order to do
     * the projection.
     */
    protected boolean reflectionNeededForProjection(){
        return !(resultColumns.allExpressionsAreColumns(childResult));
    }

    @Override
    public void adjustForSortElimination(){
        childResult.adjustForSortElimination();
    }

    @Override
    public void adjustForSortElimination(RequiredRowOrdering rowOrdering) throws StandardException{
        childResult.adjustForSortElimination(rowOrdering);
    }

    /**
     * Get the final CostEstimate for this node.
     *
     * @return The final CostEstimate for this node, which is
     * the final cost estimate for the child node.
     */
    @Override
    public CostEstimate getFinalCostEstimate(boolean useSelf) throws StandardException{
        /*
        ** The cost estimate will be set here if either optimize() or
        ** optimizeIt() was called on this node.  It's also possible
        ** that optimization was done directly on the child node,
        ** in which case the cost estimate will be null here.
        */
        if(costEstimate==null)
            return childResult.getFinalCostEstimate(true);
        else{
            return costEstimate;
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
        if(childResult!=null){
            childResult=(ResultSetNode)childResult.accept(v, this);
        }
    }

    @Override
    public ConstantAction makeConstantAction() throws StandardException{
        if(childResult!=null) return childResult.makeConstantAction();
        return null;
    }
    @Override
    public void buildTree(Collection<QueryTreeNode> tree, int depth) throws StandardException {
        setDepth(depth);
        tree.add(this);
        childResult.buildTree(tree,depth+1);
    }
}
