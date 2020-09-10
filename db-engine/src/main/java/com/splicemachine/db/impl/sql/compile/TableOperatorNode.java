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
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.util.JBitSet;

import java.util.Collection;

/**
 * A TableOperatorNode represents a relational operator like UNION, INTERSECT,
 * JOIN, etc. that takes two tables as parameters and returns a table.  The
 * parameters it takes are represented as ResultSetNodes.
 * <p/>
 * Currently, all known table operators are binary operators, so there are no
 * subclasses of this node type called "BinaryTableOperatorNode" and
 * "UnaryTableOperatorNode".
 */

// Splice fork: changed TableOperatorNode to public from package protected.
// It's understandable that this is an abstract class, but it's really annoying
// that we can't even refer to it from outside as an abstract type.
public abstract class TableOperatorNode extends FromTable{
    ResultSetNode leftResultSet;
    ResultSetNode rightResultSet;
    Optimizer leftOptimizer;
    Optimizer rightOptimizer;
    private boolean leftModifyAccessPathsDone;
    private boolean rightModifyAccessPathsDone;

    /**
     * Initializer for a TableOperatorNode.
     *
     * @param leftResultSet   The ResultSetNode on the left side of this node
     * @param rightResultSet  The ResultSetNode on the right side of this node
     * @param tableProperties Properties list associated with the table
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object leftResultSet, Object rightResultSet, Object tableProperties) throws StandardException{
        /* correlationName is always null */
        init(null,tableProperties);
        this.leftResultSet=(ResultSetNode)leftResultSet;
        this.rightResultSet=(ResultSetNode)rightResultSet;
    }

    /**
     * DERBY-4365
     * Bind untyped nulls to the types in the given ResultColumnList.
     * This is used for binding the nulls in row constructors and
     * table constructors.
     *
     * @param rcl The ResultColumnList with the types to bind nulls to
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindUntypedNullsToResultColumns(ResultColumnList rcl) throws StandardException{
        leftResultSet.bindUntypedNullsToResultColumns(rcl);
        rightResultSet.bindUntypedNullsToResultColumns(rcl);
    }

    @Override
    public Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException{
        boolean callModifyAccessPaths=false;

        if(leftResultSet instanceof FromTable){
            if(leftOptimizer!=null){
                /* We know leftOptimizer's list of Optimizables consists of
                 * exactly one Optimizable, and we know that the Optimizable
                 * is actually leftResultSet (see optimizeSource() of this
                 * class). That said, the following call to modifyAccessPaths()
                 * will effectively replace leftResultSet as it exists in
                 * leftOptimizer's list with a "modified" node that *may* be
                 * different from the original leftResultSet--for example, it
                 * could be a new DISTINCT node whose child is the original
                 * leftResultSet.  So after we've modified the node's access
                 * path(s) we have to explicitly set this.leftResulSet to
                 * point to the modified node. Otherwise leftResultSet would
                 * continue to point to the node as it existed *before* it was
                 * modified, and that could lead to incorrect behavior for
                 * certain queries.  DERBY-1852.
                 */
                leftOptimizer.modifyAccessPaths(null);
                leftResultSet=(ResultSetNode)leftOptimizer.getOptimizableList().getOptimizable(0);
            }else{
                leftResultSet= (ResultSetNode)((FromTable)leftResultSet).modifyAccessPath(outerTables);
            }
            leftModifyAccessPathsDone=true;
        }else{
            callModifyAccessPaths=true;
        }

        if(rightResultSet instanceof FromTable){
            if(rightOptimizer!=null){
                /* For the same reasons outlined above we need to make sure
                 * we set rightResultSet to point to the *modified* right result
                 * set node, which sits at position "0" in rightOptimizer's
                 * list.
                 */
                rightOptimizer.modifyAccessPaths(leftResultSet.getReferencedTableMap());
                rightResultSet=(ResultSetNode)rightOptimizer.getOptimizableList().getOptimizable(0);
            }else{
                rightResultSet= (ResultSetNode)((FromTable)rightResultSet).modifyAccessPath(outerTables);
            }
            rightModifyAccessPathsDone=true;
        }else{
            callModifyAccessPaths=true;
        }

        if(callModifyAccessPaths){
            return (Optimizable)modifyAccessPaths();
        }
        return this;
    }

    @Override
    public void verifyProperties(DataDictionary dDictionary)
            throws StandardException{
        if(leftResultSet instanceof Optimizable){
            ((Optimizable)leftResultSet).verifyProperties(dDictionary);
        }
        if(rightResultSet instanceof Optimizable){
            ((Optimizable)rightResultSet).verifyProperties(dDictionary);
        }

        super.verifyProperties(dDictionary);
    }

    /**
     * @see Optimizable#updateBestPlanMap
     * <p/>
     * Makes a call to add/load/remove the plan mapping for this node,
     * and then makes the necessary call to recurse on this node's
     * left and right child, in order to ensure that we've handled
     * the full plan all the way down this node's subtree.
     */
    @Override
    public void updateBestPlanMap(short action,Object planKey) throws StandardException{
        super.updateBestPlanMap(action,planKey);

        // Now walk the children.  Note that if either child is not
        // an Optimizable and the call to child.getOptimizerImpl()
        // returns null, then that means we haven't tried to optimize
        // the child yet.  So in that case there's nothing to
        // add/load.

        if(leftResultSet instanceof Optimizable){
            ((Optimizable)leftResultSet).updateBestPlanMap(action, planKey);
        }else if(leftResultSet.getOptimizerImpl()!=null){
            leftResultSet.getOptimizerImpl().updateBestPlanMaps(action,planKey);
        }

        if(rightResultSet instanceof Optimizable){
            ((Optimizable)rightResultSet).updateBestPlanMap(action, planKey);
        }else if(rightResultSet.getOptimizerImpl()!=null){
            rightResultSet.getOptimizerImpl().updateBestPlanMaps(action,planKey);
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
            return "nestedInParens: "+false+"\n"+
                    super.toString();
        }else{
            return "";
        }
    }

    @Override
    public String toHTMLString() {
        return "nestedInParens: " + false + "<br/>" + super.toHTMLString();
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

            if(leftResultSet!=null){
                printLabel(depth,"leftResultSet: ");
                leftResultSet.treePrint(depth+1);
            }

            if(rightResultSet!=null){
                printLabel(depth,"rightResultSet: ");
                rightResultSet.treePrint(depth+1);
            }
        }
    }

    /**
     * Get the leftResultSet from this node.
     *
     * @return ResultSetNode    The leftResultSet from this node.
     */
    public ResultSetNode getLeftResultSet(){
        return leftResultSet;
    }

    /**
     * Get the rightResultSet from this node.
     *
     * @return ResultSetNode    The rightResultSet from this node.
     */
    public ResultSetNode getRightResultSet(){
        return rightResultSet;
    }

    /**
     * Set the (query block) level (0-based) for this FromTable.
     *
     * @param level The query block level for this FromTable.
     */
    @Override
    public void setLevel(int level){
        super.setLevel(level);
        if(leftResultSet instanceof FromTable){
            ((FromTable)leftResultSet).setLevel(level);
        }
        if(rightResultSet instanceof FromTable){
            ((FromTable)rightResultSet).setLevel(level);
        }
    }

    /**
     * Return the exposed name for this table, which is the name that
     * can be used to refer to this table in the rest of the query.
     *
     * @return The exposed name for this table.
     */
    @Override
    public String getExposedName(){
        return null;
    }

    /**
     * Mark whether or not this node is nested in parens.  (Useful to parser
     * since some trees get created left deep and others right deep.)
     * The resulting state of this cal was never used so its
     * field was removed to save runtimespace for this node.
     * Further cleanup can be done including parser changes
     * if this call is really nor required.
     *
     * @param nestedInParens Whether or not this node is nested in parens.
     */
    public void setNestedInParens(boolean nestedInParens){ }


    /**
     * Bind the non VTI tables in this TableOperatorNode. This means getting
     * their TableDescriptors from the DataDictionary.
     * We will build an unbound RCL for this node.  This RCL must be
     * "bound by hand" after the underlying left and right RCLs
     * are bound.
     *
     * @param dataDictionary The DataDictionary to use for binding
     * @param fromListParam  FromList to use/append to.
     * @throws StandardException Thrown on error
     * @return ResultSetNode        Returns this.
     */
    @Override
    public ResultSetNode bindNonVTITables(DataDictionary dataDictionary,FromList fromListParam)throws StandardException{
        return bindNonVTITables(dataDictionary, fromListParam, false);
    }

    public ResultSetNode bindNonVTITables(DataDictionary dataDictionary,FromList fromListParam, boolean bindRightOnly)throws StandardException{
        if (!bindRightOnly)
            leftResultSet=leftResultSet.bindNonVTITables(dataDictionary,fromListParam);
        rightResultSet=rightResultSet.bindNonVTITables(dataDictionary,fromListParam);
        /* Assign the tableNumber */
        if(tableNumber==-1)  // allow re-bind, in which case use old number
            tableNumber=getCompilerContext().getNextTableNumber();

        return this;
    }

    /**
     * Bind the VTI tables in this TableOperatorNode. This means getting
     * their TableDescriptors from the DataDictionary.
     * We will build an unbound RCL for this node.  This RCL must be
     * "bound by hand" after the underlying left and right RCLs
     * are bound.
     *
     * @param fromListParam FromList to use/append to.
     * @throws StandardException Thrown on error
     * @return ResultSetNode        Returns this.
     */
    @Override
    public ResultSetNode bindVTITables(FromList fromListParam) throws StandardException{
        return bindVTITables(fromListParam, false);
    }

    public ResultSetNode bindVTITables(FromList fromListParam, boolean bindRightOnly) throws StandardException{
        if (!bindRightOnly)
            leftResultSet=leftResultSet.bindVTITables(fromListParam);
        rightResultSet=rightResultSet.bindVTITables(fromListParam);

        return this;
    }
    /**
     * Bind the expressions under this TableOperatorNode.  This means
     * binding the sub-expressions, as well as figuring out what the
     * return type is for each expression.
     *
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindExpressions(FromList fromListParam) throws StandardException{
        bindExpressions(fromListParam, false);
    }

    public void bindExpressions(FromList fromListParam, boolean bindRightOnly) throws StandardException{
        /*
        ** Parameters not allowed in select list of either side of union,
        ** except when the union is for a table constructor.
        */
        if (!bindRightOnly) {
            if (!(this instanceof UnionNode) || !((UnionNode) this).tableConstructor()) {
                leftResultSet.rejectParameters();
                rightResultSet.rejectParameters();
            }

            leftResultSet.bindExpressions(fromListParam);
        }
        rightResultSet.bindExpressions(fromListParam);
    }
    /**
     * Check for (and reject) ? parameters directly under the ResultColumns.
     * This is done for SELECT statements.  For TableOperatorNodes, we
     * simply pass the check through to the left and right children.
     *
     * @throws StandardException Thrown if a ? parameter found
     *                           directly under a ResultColumn
     */
    @Override
    public void rejectParameters() throws StandardException{
        leftResultSet.rejectParameters();
        rightResultSet.rejectParameters();
    }

    /**
     * Bind the expressions in this ResultSetNode if it has tables.  This means binding the
     * sub-expressions, as well as figuring out what the return type is for
     * each expression.
     *
     * @param fromListParam FromList to use/append to.
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindExpressionsWithTables(FromList fromListParam) throws StandardException{
        /*
        ** Parameters not allowed in select list of either side of a set operator,
        ** except when the set operator is for a table constructor.
        */
        if(!(this instanceof UnionNode) || !((UnionNode)this).tableConstructor()){
            leftResultSet.rejectParameters();
            rightResultSet.rejectParameters();
        }

        leftResultSet.bindExpressionsWithTables(fromListParam);
        rightResultSet.bindExpressionsWithTables(fromListParam);
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
        bindResultColumns(fromListParam, false);
    }

    public void bindResultColumns(FromList fromListParam, boolean bindRightOnly) throws StandardException{
        if (!bindRightOnly)
            leftResultSet.bindResultColumns(fromListParam);
        rightResultSet.bindResultColumns(fromListParam);

        if (this instanceof UnionNode) {
            assert leftResultSet.getResultColumns().size() == rightResultSet.getResultColumns().size();
            for (int i = 0; i < leftResultSet.getResultColumns().size(); ++i) {
                ResultColumn left = leftResultSet.getResultColumns().elementAt(i);
                ResultColumn right = rightResultSet.getResultColumns().elementAt(i);
                right.setName(left.getName());
            }
        }
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
        leftResultSet.bindResultColumns(targetTableDescriptor,
                targetVTI,
                targetColumnList,
                statement,fromListParam);
        rightResultSet.bindResultColumns(targetTableDescriptor,
                targetVTI,
                targetColumnList,
                statement,fromListParam);
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
        FromTable result=leftResultSet.getFromTableByName(name,schemaName,exactMatch);

        /* We search both sides for a TableOperatorNode (join nodes)
         * but only the left side for a UnionNode.
         */
        if(result==null){
            result=rightResultSet.getFromTableByName(name,schemaName,exactMatch);
        }
        return result;
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
        leftResultSet=leftResultSet.preprocess(numTables,gbl,fromList);
        /* If leftResultSet is a FromSubquery, then we must explicitly extract
         * out the subquery (flatten it).  (SelectNodes have their own
         * method of flattening them.
         */
        if(leftResultSet instanceof FromSubquery){
            leftResultSet=((FromSubquery)leftResultSet).extractSubquery(numTables);
        }
        rightResultSet=rightResultSet.preprocess(numTables,gbl,fromList);
        /* If rightResultSet is a FromSubquery, then we must explicitly extract
         * out the subquery (flatten it).  (SelectNodes have their own
         * method of flattening them.
         */
        if(rightResultSet instanceof FromSubquery){
            rightResultSet=((FromSubquery)rightResultSet).extractSubquery(numTables);
        }

        /* Build the referenced table map (left || right) */
        referencedTableMap=(JBitSet)leftResultSet.getReferencedTableMap().clone();
        referencedTableMap.or(rightResultSet.getReferencedTableMap());
        referencedTableMap.set(tableNumber);

        /* Only generate a PRN if this node is not a flattenable join node. */
        if(isFlattenableJoinNode()){
            return this;
        }else{
            /* Project out any unreferenced RCs before we generate the PRN.
             * NOTE: We have to do this at the end of preprocess since it has to
             * be from the bottom up.  We can't do it until the join expression is
             * bound, since the join expression may contain column references that
             * are not referenced anywhere else above us.
             */
            return genProjectRestrict();
        }
    }

    @Override
    public ResultSetNode genProjectRestrict()
            throws StandardException {
        projectResultColumns();
        return super.genProjectRestrict();
    }
    /**
     * Find the unreferenced result columns and project them out. This is used in pre-processing joins
     * that are not flattened into the where clause.
     */
    @Override
    void projectResultColumns() throws StandardException{
        resultColumns.doProjection(true);
    }

    /**
     * Optimize a TableOperatorNode.
     *
     * @param dataDictionary The DataDictionary to use for optimization
     * @param predicateList  The PredicateList to apply.
     * @param forSpark
     * @throws StandardException Thrown on error
     * @return ResultSetNode    The top of the optimized query tree
     */
    @Override
    public ResultSetNode optimize(DataDictionary dataDictionary,
                                  PredicateList predicateList,
                                  double outerRows,
                                  boolean forSpark) throws StandardException{
        /* Get an optimizer, so we can get a cost structure */
        Optimizer optimizer = getOptimizer(
                (FromList)getNodeFactory().getNode(
                        C_NodeTypes.FROM_LIST,
                        getNodeFactory().doJoinOrderOptimization(),
                        this,
                        getContextManager()),
                predicateList,
                dataDictionary,
                null);
        optimizer.setForSpark(forSpark);

        costEstimate=optimizer.newCostEstimate();

        /* RESOLVE: This is just a stub for now */
        leftResultSet=leftResultSet.optimize(
                dataDictionary,
                predicateList,
                outerRows,
                forSpark);
        rightResultSet=rightResultSet.optimize(
                dataDictionary,
                predicateList,
                outerRows,
                forSpark);

        /* The cost is the sum of the two child costs */
        costEstimate.setCost(leftResultSet.getCostEstimate().getEstimatedCost(),
                leftResultSet.getCostEstimate().rowCount(),
                leftResultSet.getCostEstimate().singleScanRowCount()+
                        rightResultSet.getCostEstimate().singleScanRowCount());

        costEstimate.add(rightResultSet.costEstimate,costEstimate);

        return this;
    }

    @Override
    public ResultSetNode modifyAccessPaths() throws StandardException{
        /* Beetle 4454 - union all with another union all would modify access
         * paths twice causing NullPointerException, make sure we don't
         * do this again, if we have already done it in modifyAccessPaths(outertables)
         */
        if(!leftModifyAccessPathsDone){
            if(leftOptimizer!=null){
                /* We know leftOptimizer's list of Optimizables consists of
                 * exactly one Optimizable, and we know that the Optimizable
                 * is actually leftResultSet (see optimizeSource() of this
                 * class). That said, the following call to modifyAccessPaths()
                 * will effectively replace leftResultSet as it exists in
                 * leftOptimizer's list with a "modified" node that *may* be
                 * different from the original leftResultSet--for example, it
                 * could be a new DISTINCT node whose child is the original
                 * leftResultSet.  So after we've modified the node's access
                 * path(s) we have to explicitly set this.leftResulSet to
                 * point to the modified node. Otherwise leftResultSet would
                 * continue to point to the node as it existed *before* it was
                 * modified, and that could lead to incorrect behavior for
                 * certain queries.  DERBY-1852.
                 */
                leftOptimizer.modifyAccessPaths(null);
                leftResultSet=(ResultSetNode)leftOptimizer.getOptimizableList().getOptimizable(0);
            }else{
                // If this is a SetOperatorNode then we may have pushed
                // predicates down to the children.  If that's the case
                // then we need to pass those predicates down as part
                // of the modifyAccessPaths call so that they can be
                // pushed one last time, in prep for generation.
                if(this instanceof SetOperatorNode){
                    SetOperatorNode setOp=(SetOperatorNode)this;
                    leftResultSet=leftResultSet.modifyAccessPaths(
                            setOp.getLeftOptPredicateList());
                }else
                    leftResultSet=leftResultSet.modifyAccessPaths();
            }
        }
        if(!rightModifyAccessPathsDone){
            if(rightOptimizer!=null){
                /* For the same reasons outlined above we need to make sure
                 * we set rightResultSet to point to the *modified* right result
                 * set node, which sits at position "0" in rightOptimizer's
                 * list.
                 */
                rightOptimizer.modifyAccessPaths(leftResultSet.getReferencedTableMap());
                rightResultSet=(ResultSetNode)rightOptimizer.getOptimizableList().getOptimizable(0);
            }else{
                if(this instanceof SetOperatorNode){
                    SetOperatorNode setOp=(SetOperatorNode)this;
                    rightResultSet=rightResultSet.modifyAccessPaths(
                            setOp.getRightOptPredicateList());
                }else
                    rightResultSet=rightResultSet.modifyAccessPaths();
            }
        }
        return this;
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
        return leftResultSet.referencesTarget(name,baseTable) || rightResultSet.referencesTarget(name,baseTable);
    }

    /**
     * Return true if the node references SESSION schema tables (temporary or permanent)
     *
     * @throws StandardException Thrown on error
     * @return true if references SESSION schema tables, else false
     */
    @Override
    public boolean referencesSessionSchema() throws StandardException{
        return leftResultSet.referencesSessionSchema() || rightResultSet.referencesSessionSchema();
    }

    /**
     * Optimize a source result set to this table operator.
     *
     * @throws StandardException Thrown on error
     */
    protected ResultSetNode optimizeSource(Optimizer optimizer,
                                           ResultSetNode sourceResultSet,
                                           PredicateList predList,
                                           CostEstimate outerCost) throws StandardException{
        ResultSetNode retval;

        if(sourceResultSet instanceof FromTable){
            FromList optList=(FromList)getNodeFactory().getNode(
                    C_NodeTypes.FROM_LIST,
                    getNodeFactory().doJoinOrderOptimization(),
                    sourceResultSet,
                    getContextManager());

            /* If there is no predicate list, create an empty one */
            if(predList==null)
                predList=(PredicateList)getNodeFactory().getNode(
                        C_NodeTypes.PREDICATE_LIST,
                        getContextManager());

            LanguageConnectionContext lcc=getLanguageConnectionContext();
            OptimizerFactory optimizerFactory=lcc.getOptimizerFactory();
            optimizer=optimizerFactory.getOptimizer(optList,
                    predList,
                    getDataDictionary(),
                    null,
                    getCompilerContext().getMaximalPossibleTableCount(),
                    lcc);
            optimizer.prepForNextRound();

            if(sourceResultSet==leftResultSet){
                leftOptimizer=optimizer;
            }else if(sourceResultSet==rightResultSet){
                rightOptimizer=optimizer;
            }else{
                if(SanityManager.DEBUG)
                    SanityManager.THROWASSERT("Result set being optimized is neither left nor right");
            }

            /*
            ** Set the estimated number of outer rows from the outer part of
            ** the plan.
            */

            // Encapsulate this transfer logic.
            if (outerCost != null)
                optimizer.transferOuterCost(outerCost);

            /* Optimize the underlying result set */
            while(optimizer.nextJoinOrder()){
                while(optimizer.getNextDecoratedPermutation()){
                    optimizer.costPermutation();
                }
            }

            optimizer.verifyBestPlanFound(); //modify the access paths to the correct version
            retval=sourceResultSet;
        }else{
            retval=sourceResultSet.optimize(
                    optimizer.getDataDictionary(),
                    predList,
                    outerCost == null? 1.0 : outerCost.rowCount(),
                    optimizer.isForSpark());
        }

        return retval;
    }

    /**
     * Decrement (query block) level (0-based) for
     * all of the tables in this ResultSet tree.
     * This is useful when flattening a subquery.
     *
     * @param decrement The amount to decrement by.
     */
    @Override
    void decrementLevel(int decrement){
        leftResultSet.decrementLevel(decrement);
        rightResultSet.decrementLevel(decrement);
    }

    @Override
    void adjustForSortElimination(){
        leftResultSet.adjustForSortElimination();
        rightResultSet.adjustForSortElimination();
    }

    @Override
    void adjustForSortElimination(RequiredRowOrdering rowOrdering) throws StandardException{
        leftResultSet.adjustForSortElimination(rowOrdering);
        rightResultSet.adjustForSortElimination(rowOrdering);
    }

    @Override
    public void acceptChildren(Visitor v) throws StandardException{
        super.acceptChildren(v);
        if(leftResultSet!=null){
            leftResultSet=(ResultSetNode)leftResultSet.accept(v, this);
        }
        if(rightResultSet!=null){
            rightResultSet=(ResultSetNode)rightResultSet.accept(v, this);
        }
    }

    @Override
    public boolean needsSpecialRCLBinding(){
        return true;
    }

    @Override
    public void buildTree(Collection<QueryTreeNode> tree, int depth) throws StandardException {
        setDepth(depth);
        tree.add(this);
        rightResultSet.buildTree(tree,depth+1);
        leftResultSet.buildTree(tree,depth+1);
    }

}
