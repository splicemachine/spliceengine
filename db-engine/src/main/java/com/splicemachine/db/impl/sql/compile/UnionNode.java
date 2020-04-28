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
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.util.JBitSet;

import java.lang.reflect.Modifier;

/**
 * A UnionNode represents a UNION in a DML statement.  It contains a boolean
 * telling whether the union operation should eliminate duplicate rows.
 */

public class UnionNode extends SetOperatorNode{
    /* Only optimize it once */
    /* Only call addNewNodes() once */
    private boolean addNewNodesCalled;

    /* Is this a UNION ALL generated for a table constructor -- a VALUES expression with multiple rows. */
    boolean tableConstructor;

    /* True if this is the top node of a table constructor */
    boolean topTableConstructor;

    /* below variables are for recursive queries */
    boolean isRecursive;
    /* the description of the result columns */
    TableDescriptor viewDescriptor;


    /**
     * Initializer for a UnionNode.
     *
     * @param leftResult       The ResultSetNode on the left side of this union
     * @param rightResult      The ResultSetNode on the right side of this union
     * @param all              Whether or not this is a UNION ALL.
     * @param tableConstructor Whether or not this is from a table constructor.
     * @param tableProperties  Properties list associated with the table
     * @throws StandardException Thrown on error
     */
    @Override
    public void init(Object leftResult,
                     Object rightResult,
                     Object all,
                     Object tableConstructor,
                     Object tableProperties) throws StandardException{
        super.init(leftResult,rightResult,all,tableProperties);

        /* Is this a UNION ALL for a table constructor? */
        this.tableConstructor=(Boolean)tableConstructor;

        this.isRecursive = false;
    } // end of init

    /**
     * Mark this as the top node of a table constructor.
     */
    public void markTopTableConstructor(){
        topTableConstructor=true;
    }

    /**
     * Tell whether this is a UNION for a table constructor.
     */
    boolean tableConstructor(){
        return tableConstructor;
    }

    /**
     * Check for (and reject) ? parameters directly under the ResultColumns.
     * This is done for SELECT statements.  Don't reject parameters that
     * are in a table constructor - these are allowed, as long as the
     * table constructor is in an INSERT statement or each column of the
     * table constructor has at least one non-? column.  The latter case
     * is checked below, in bindExpressions().
     *
     * @throws StandardException Thrown if a ? parameter found
     *                           directly under a ResultColumn
     */
    @Override
    public void rejectParameters() throws StandardException{
        if(!tableConstructor())
            super.rejectParameters();
    }

    /**
     * Set the type of column in the result column lists of each
     * source of this union tree to the type in the given result column list
     * (which represents the result columns for an insert).
     * This is only for table constructors that appear in insert statements.
     *
     * @param typeColumns The ResultColumnList containing the desired result
     *                    types.
     * @throws StandardException Thrown on error
     */
    @Override
    void setTableConstructorTypes(ResultColumnList typeColumns) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(resultColumns.size()<=typeColumns.size(),
                    "More columns in ResultColumnList than in base table.");
        }

        ResultSetNode rsn;

        /*
        ** Should only set types of ? parameters to types of result columns
        ** if it's a table constructor.
        */
        if(tableConstructor()){
            /* By looping through the union nodes, we avoid recursion */
            for(rsn=this;rsn instanceof UnionNode;){
                UnionNode union=(UnionNode)rsn;

                /*
                ** Assume that table constructors are left-deep trees of UnionNodes
                ** with RowResultSet nodes on the right.
                */
                if(SanityManager.DEBUG)
                    SanityManager.ASSERT(
                            union.rightResultSet instanceof RowResultSetNode,
                            "A "+union.rightResultSet.getClass().getName()+
                                    " is on the right of a union in a table constructor");

                union.rightResultSet.setTableConstructorTypes( typeColumns);

                rsn=union.leftResultSet;
            }

            /* The last node on the left should be a result set node */
            if(SanityManager.DEBUG)
                SanityManager.ASSERT(rsn instanceof RowResultSetNode,
                        "A "+rsn.getClass().getName()+
                                " is at the left end of a table constructor");

            rsn.setTableConstructorTypes(typeColumns);
        }
    }

    /**
     * Make the RCL of this node match the target node for the insert. If this
     * node represents a table constructor (a VALUES clause), we replace the
     * RCL with an enhanced one if necessary, and recursively enhance the RCL
     * of each child node. For table constructors, we also need to check that
     * we don't attempt to override auto-increment columns in each child node
     * (checking the top-level RCL isn't sufficient since a table constructor
     * may contain the DEFAULT keyword, which makes it possible to specify a
     * column without overriding its value).
     * <p/>
     * If this node represents a regular UNION, put a ProjectRestrictNode on
     * top of this node and enhance the RCL in that node.
     */
    @Override
    ResultSetNode enhanceRCLForInsert( InsertNode target,boolean inOrder,int[] colMap) throws StandardException{
        if(tableConstructor()){
            leftResultSet=target.enhanceAndCheckForAutoincrement( leftResultSet,inOrder,colMap);
            rightResultSet=target.enhanceAndCheckForAutoincrement( rightResultSet,inOrder,colMap);
            if(!inOrder || resultColumns.size()<target.resultColumnList.size()){
                resultColumns=getRCLForInsert(target,colMap);
            }
            return this;
        }else{
            // This is a regular UNION, so fall back to the default
            // implementation that adds a ProjectRestrictNode on top.
            return super.enhanceRCLForInsert(target,inOrder,colMap);
        }
    }

    /*
     *  Optimizable interface
     */

    @Override
    public CostEstimate optimizeIt(Optimizer optimizer,
                                   OptimizablePredicateList predList,
                                   CostEstimate outerCost,
                                   RowOrdering rowOrdering) throws StandardException{
        /*
        ** RESOLVE: Most types of Optimizables only implement estimateCost(),
        ** and leave it up to optimizeIt() in FromTable to figure out the
        ** total cost of the join.  For unions, though, we want to figure out
        ** the best plan for the sources knowing how many outer rows there are -
        ** it could affect their strategies significantly.  So we implement
        ** optimizeIt() here, which overrides the optimizeIt() in FromTable.
        ** This assumes that the join strategy for which this union node is
        ** the inner table is a nested loop join, which will not be a valid
        ** assumption when we implement other strategies like materialization
        ** (hash join can work only on base tables).
        */

        /* optimize() both resultSets */

        // If we have predicates from an outer block, we want to try
        // to push them down to this node's children.  However, we can't
        // just push the predicates down as they are; instead, we
        // need to scope them for the child result sets first, and
        // then push the scoped versions.  This is all done in the
        // call to pushOptPredicate() here; for more, see that method's
        // definition in SetOperatorNode.  NOTE: If we're considering a
        // hash join then we do not push the predicates because we'll
        // need the predicates to be at this level in order to find
        // out if one of them is an equijoin predicate that can be used
        // for the hash join.
        if((predList!=null) && getCurrentAccessPath().getJoinStrategy().getJoinStrategyType().isAllowsJoinPredicatePushdown()) {
            for(int i=predList.size()-1;i>=0;i--){
                if(pushOptPredicate(predList.getOptPredicate(i)))
                    predList.removeOptPredicate(i);
            }
        }

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

        leftResultSet=optimizeSource(optimizer, leftResultSet, getLeftOptPredicateList(), null);

        rightResultSet=optimizeSource(optimizer, rightResultSet, getRightOptPredicateList(), null);

        CostEstimate leftCost = leftResultSet.getCostEstimate();
        CostEstimate rightCost = rightResultSet.getCostEstimate();
        CostEstimate costEstimate=getCostEstimate(optimizer);

        double remoteCost;
        double localCost;
        if(all){
            /*
             * This is a Union All. In Splice the UnionAll scans the left, then scans the right,
             * so the cost is
             *
             * remoteCost = left.remoteCost + right.remoteCost
             * localCost = left.localCost + right.localCost
             * outputRows = left.outputRows + right.outputRows
             * heapSize = left.heapSize + right.heapSize
             * partitions = left.partitions + right.partitions
             */
            remoteCost = leftCost.remoteCost()+rightCost.remoteCost();
            localCost = leftCost.localCost()+rightCost.localCost();
        }else{
            /*
             * This is a Union, which will impose a sort distinct over top of us. In this case,
             * we want to inherit the costs of what goes below us, since the union won't do anything.
             * *However*, we can't really do that, because the Sort costing assumes that it's operating
             * over a single table, so we need to represent this as a single table (with an eye to the
             * fact that we are actually going to be under a distinct node here).
             *
             * Since we will be under a parallel distinct node, the cost is the max between
             * the local+remote costs of both tables (since we'll need to write at least one entry for
             * each row)
             */
            localCost = Math.max(leftCost.localCost(),rightCost.localCost());
            remoteCost = Math.max(leftCost.remoteCost(),rightCost.remoteCost());
        }
        double outputRows = leftCost.rowCount()+rightCost.rowCount();
        double heapSize = leftCost.getEstimatedHeapSize()+rightCost.getEstimatedHeapSize();
        int partitions = leftCost.partitionCount() + rightCost.partitionCount();


        costEstimate.setLocalCost(localCost);
        costEstimate.setRemoteCost(remoteCost);
        costEstimate.setRowCount(outputRows);
        costEstimate.setEstimatedHeapSize((long)heapSize);
        costEstimate.setNumPartitions(partitions);
        costEstimate.setLocalCostPerPartition(costEstimate.localCost(), costEstimate.partitionCount());
        costEstimate.setRemoteCostPerPartition(costEstimate.remoteCost(), costEstimate.partitionCount());


        /*
        ** Get the cost of this result set in the context of the whole plan.
        */
        getCurrentAccessPath().getJoinStrategy().estimateCost(this, predList, null, outerCost, optimizer, costEstimate);

        optimizer.considerCost(this,predList,costEstimate,outerCost);

        return costEstimate;
    }

    /**
     * DERBY-649: Handle pushing predicates into UnionNodes. It is possible to push
     * single table predicates that are binaryOperations or inListOperations.
     * <p/>
     * Predicates of the form <columnReference> <RELOP> <constant> or <columnReference>
     * IN <constantList> are currently handled. Since these predicates would allow
     * optimizer to pick available indices, pushing them provides maximum benifit.
     * <p/>
     * It should be possible to expand this logic to cover more cases. Even pushing
     * expressions (like a+b = 10) into SELECTs would improve performance, even if
     * they don't allow use of index. It would mean evaluating expressions closer to
     * data and hence could avoid sorting or other overheads that UNION may require.
     * <p/>
     * Note that the predicates are not removed after pushing. This is to ensure if
     * pushing is not possible or only partially feasible.
     *
     * @param predicateList List of single table predicates to push
     * @exception StandardException        Thrown on error
     */
    @Override
    public void pushExpressions(PredicateList predicateList) throws StandardException{
        // do not push predicate inside recursive union
        if (isRecursive)
            return;

        // If left or right side is a UnionNode, further push the predicate list
        // Note, it is OK not to push these predicates since they are also evaluated
        // in the ProjectRestrictNode. There are other types of operations possible
        // here in addition to UnionNode or SelectNode, like RowResultSetNode.
        if(leftResultSet instanceof UnionNode) {
            optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION, 0, 0, 0.0,
                          "UnionNode pushing predicates into left UnionNode.", predicateList);
            ((UnionNode) leftResultSet).pushExpressions(predicateList);
        } else if(leftResultSet instanceof SelectNode) {
            optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION, 0, 0, 0.0,
                          "UnionNode pushing predicates into left SelectNode.", predicateList);
            predicateList.pushExpressionsIntoSelect((SelectNode) leftResultSet, true);
        }

        if(rightResultSet instanceof UnionNode) {
            optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION, 0, 0, 0.0,
                          "UnionNode pushing predicates into right UnionNode.", predicateList);
            ((UnionNode) rightResultSet).pushExpressions(predicateList);
        } else if(rightResultSet instanceof SelectNode) {
            optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION,0,0,0.0,
                                     "UnionNode pushing predicates into right SelectNode.",predicateList);
            predicateList.pushExpressionsIntoSelect((SelectNode) rightResultSet, true);
        }
    }

    @Override
    public Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException{
        Optimizable retOptimizable;
        retOptimizable=super.modifyAccessPath(outerTables);

        /* We only want call addNewNodes() once */
        if(addNewNodesCalled){
            return retOptimizable;
        }
        return (Optimizable)addNewNodes();
    }

    @Override
    public ResultSetNode modifyAccessPaths() throws StandardException{
        ResultSetNode retRSN;
        retRSN=super.modifyAccessPaths();

        /* We only want call addNewNodes() once */
        if(addNewNodesCalled){
            return retRSN;
        }
        return addNewNodes();
    }

    /**
     * Add any new ResultSetNodes that are necessary to the tree.
     * We wait until after optimization to do this in order to
     * make it easier on the optimizer.
     *
     * @return (Potentially new) head of the ResultSetNode tree.
     * @throws StandardException Thrown on error
     */
    private ResultSetNode addNewNodes() throws StandardException{
        ResultSetNode treeTop=this;

        /* Only call addNewNodes() once */
        if(addNewNodesCalled){
            return this;
        }

        addNewNodesCalled=true;

        /* RESOLVE - We'd like to generate any necessary NormalizeResultSets
         * above our children here, in the tree.  However, doing so causes
         * the following query to fail because the where clause goes against
         * the NRS instead of the Union:
         *        SELECT TABLE_TYPE
         *        FROM SYS.SYSTABLES,
         *            (VALUES ('T','TABLE') ,
         *                ('S','SYSTEM TABLE') , ('V', 'VIEW')) T(TTABBREV,TABLE_TYPE)
         *        WHERE TTABBREV=TABLETYPE;
         * Thus, we are forced to skip over generating the nodes in the tree
         * and directly generate the execution time code in generate() instead.
         * This solves the problem for some unknown reason.
         */

        /* Simple solution (for now) to eliminating duplicates -
         * generate a distinct above the union.
         */
        if(!all){
            /* We need to generate a NormalizeResultSetNode above us if the column
             * types and lengths don't match.  (We need to do it here, since they
             * will end up agreeing in the PRN, which will be the immediate
             * child of the DistinctNode, which means that the NormalizeResultSet
             * won't get generated above the PRN.)
             */
            if(!columnTypesAndLengthsMatch()){
                treeTop= (ResultSetNode)getNodeFactory().getNode(C_NodeTypes.NORMALIZE_RESULT_SET_NODE,
                        treeTop,
                        null,
                        null,
                        Boolean.FALSE,
                        getContextManager());
            }

            treeTop=(ResultSetNode)getNodeFactory().getNode(C_NodeTypes.DISTINCT_NODE,
                    treeTop.genProjectRestrict(),
                    Boolean.FALSE,
                    tableProperties,
                    getContextManager());
            /* HACK - propagate our table number up to the new DistinctNode
             * so that arbitrary hash join will work correctly.  (Otherwise it
             * could have a problem dividing up the predicate list at the end
             * of modifyAccessPath() because the new child of the PRN above
             * us would have a tableNumber of -1 instead of our tableNumber.)
             */
            ((FromTable)treeTop).setTableNumber(tableNumber);
            treeTop.setReferencedTableMap((JBitSet) referencedTableMap.clone());
            ((DistinctNode)treeTop).estimateCost(null, null, null, optimizer, null);
            all=true;
        }

        /* Generate the OrderByNode if a sort is still required for
         * the order by.
         */
        if(orderByList!=null){
            treeTop=(ResultSetNode)getNodeFactory().getNode(C_NodeTypes.ORDER_BY_NODE,
                    treeTop,
                    orderByList,
                    tableProperties,
                    getContextManager());
        }


        if(offset!=null || fetchFirst!=null){
            ResultColumnList newRcl= treeTop.getResultColumns().copyListAndObjects();
            newRcl.genVirtualColumnNodes(treeTop,treeTop.getResultColumns());

            treeTop=(ResultSetNode)getNodeFactory().getNode( C_NodeTypes.ROW_COUNT_NODE,
                    treeTop,
                    newRcl,
                    offset,
                    fetchFirst,
                    hasJDBClimitClause,
                    getContextManager());
        }

        return treeTop;
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
            return "tableConstructor: "+tableConstructor+"\n"+super.toString();
        }else{
            return "";
        }
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
        super.bindExpressions(fromListParam);

        /*
        ** Each ? parameter in a table constructor that is not in an insert
        ** statement takes its type from the first non-? in its column
        ** of the table constructor.  It's an error to have a column that
        ** has all ?s.  Do this only for the top of the table constructor
        ** list - we don't want to do this for every level of union node
        ** in the table constructor.  Also, don't do this for an INSERT -
        ** the types of the ? parameters come from the columns being inserted
        ** into in that case.
        */
        if(topTableConstructor && (!insertSource)){
            /*
            ** Step through all the rows in the table constructor to
            ** get the type of the first non-? in each column.
            */
            DataTypeDescriptor[] types= new DataTypeDescriptor[leftResultSet.getResultColumns().size()];

            ResultSetNode rsn;
            int numTypes=0;

            /* By looping through the union nodes, we avoid recursion */
            for(rsn=this;rsn instanceof SetOperatorNode;){
                SetOperatorNode setOperator=(SetOperatorNode)rsn;

                /*
                ** Assume that table constructors are left-deep trees of
                ** SetOperatorNodes with RowResultSet nodes on the right.
                */
                if(SanityManager.DEBUG)
                    SanityManager.ASSERT(
                            setOperator.rightResultSet instanceof RowResultSetNode,
                            "A "+setOperator.rightResultSet.getClass().getName()+
                                    " is on the right side of a setOperator in a table constructor");

                RowResultSetNode rrsn= (RowResultSetNode)setOperator.rightResultSet;

                numTypes+=getParamColumnTypes(types,rrsn);

                rsn=setOperator.leftResultSet;
            }

            /* The last node on the left should be a result set node */
            assert rsn instanceof RowResultSetNode;

            numTypes+=getParamColumnTypes(types,(RowResultSetNode)rsn);

            /* Are there any columns that are all ? parameters? */
            if(numTypes<types.length){
                throw StandardException.newException(SQLState.LANG_TABLE_CONSTRUCTOR_ALL_PARAM_COLUMN);
            }

            /*
            ** Loop through the nodes again. This time, look for parameter
            ** nodes, and give them the type from the type array we just
            ** constructed.
            */
            for(rsn=this;rsn instanceof SetOperatorNode;){
                SetOperatorNode setOperator=(SetOperatorNode)rsn;
                RowResultSetNode rrsn=(RowResultSetNode)setOperator.rightResultSet;

                setParamColumnTypes(types,rrsn);

                rsn=setOperator.leftResultSet;
            }

            setParamColumnTypes(types,(RowResultSetNode)rsn);
        }
    }

    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException{
        /*  By the time we get here we should be a union all.
         *  (We created a DistinctNode above us, if needed,
         *  to eliminate the duplicates earlier.)
         */
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(all, "all expected to be true");
        }

        /* Get the next ResultSet #, so that we can number this ResultSetNode, its
         * ResultColumnList and ResultSet.
         */
        assignResultSetNumber();

        // Get our final cost estimate based on the child estimates.
        costEstimate=getFinalCostEstimate(false);

        LocalField recursiveUnionResultSetRef = null;

        if (isRecursive) {
            recursiveUnionResultSetRef = acb.newFieldDeclaration(Modifier.PRIVATE, ClassName.NoPutResultSet);
        }

        acb.newFieldDeclaration(Modifier.PRIVATE, ClassName.NoPutResultSet);

        // build up the tree.

        acb.pushGetResultSetFactoryExpression(mb); // instance for getUnionResultSet


        /* Generate the left and right ResultSets */
        leftResultSet.generate(acb,mb);

        /* Do we need a NormalizeResultSet above the left ResultSet? */
        if(!resultColumns.isExactTypeAndLengthMatch(leftResultSet.getResultColumns())){
            acb.pushGetResultSetFactoryExpression(mb);
            mb.swap();
            generateNormalizationResultSet(acb,mb,getCompilerContext().getNextResultSetNumber(),makeResultDescription());
        }

        rightResultSet.generate(acb,mb);

        /* Do we need a NormalizeResultSet above the right ResultSet? */
        if(!resultColumns.isExactTypeAndLengthMatch(rightResultSet.getResultColumns())){
            acb.pushGetResultSetFactoryExpression(mb);
            mb.swap();
            generateNormalizationResultSet(acb,mb,getCompilerContext().getNextResultSetNumber(),makeResultDescription()
            );
        }

        /* Generate the UnionResultSet:
         *    arg1: leftExpression - Expression for leftResultSet
         *    arg2: rightExpression - Expression for rightResultSet
         *  arg3: Activation
         *  arg4: resultSetNumber
         *  arg5: estimated row count
         *  arg6: estimated cost
         *  arg7: close method
         */

        mb.push(resultSetNumber);
        mb.push(costEstimate.rowCount());
        mb.push(costEstimate.getEstimatedCost());
        mb.push(printExplainInformationForActivation());

        String methodName;
        int numArgs = 6;
        if (isRecursive) {
            // also pass down the recursion limit
            // get the loop limit from configuration
            Integer maxIterationObject = (Integer)getLanguageConnectionContext().getSessionProperties().getProperty(SessionProperties.PROPERTYNAME.RECURSIVEQUERYITERATIONLIMIT);
            int iterationLimit = maxIterationObject == null ? -1: maxIterationObject.intValue();
            mb.push(iterationLimit);
            numArgs ++;
            methodName = "getRecursiveUnionResultSet";
        } else {
            methodName = "getUnionResultSet";
        }


        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,methodName, ClassName.NoPutResultSet,numArgs);

        if (isRecursive) {
            mb.setField(recursiveUnionResultSetRef);

            //generate the code to set recursiveUnionReferene in the SelfReferenceOperation
            CollectNodesVisitor cnv=
                    new CollectNodesVisitor(SelfReferenceNode.class,null);
            this.rightResultSet.accept(cnv);
            for(Object o : cnv.getList()){
                // make sure the SelfReference points to this recursive UnionNode, in the present of nested recursive union,
                // we may see different SelfReference pointing to different level of RecursiveUnion.
                if (((SelfReferenceNode)o).getRecursiveUnionRoot() != this)
                    continue;
                LocalField selfReferenceResultSetRef = ((SelfReferenceNode)o).getResultSetRef();
                mb.getField(selfReferenceResultSetRef);
                mb.cast(ClassName.SelfReferenceResultSet);
                mb.getField(recursiveUnionResultSetRef);
                mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.SelfReferenceResultSet, "setRecursiveUnionReference", "void", 1);
            }

            mb.getField(recursiveUnionResultSetRef);
        }

    }

    /**
     * @return The final CostEstimate for this UnionNode, which is
     * the sum of the two child costs.
     * @see ResultSetNode#getFinalCostEstimate
     * <p/>
     * Get the final CostEstimate for this UnionNode.
     */
    @Override
    public CostEstimate getFinalCostEstimate(boolean useSelf) throws StandardException{
        if (useSelf && trulyTheBestAccessPath != null) {
            return getTrulyTheBestAccessPath().getCostEstimate();
        }

        // If we already found it, just return it.
        if(finalCostEstimate!=null)
            return finalCostEstimate;

        if (trulyTheBestAccessPath != null && getTrulyTheBestAccessPath().getCostEstimate().getBase() != null) {
            finalCostEstimate = getTrulyTheBestAccessPath().getCostEstimate().getBase();
            return finalCostEstimate;
        }


        CostEstimate leftCE=leftResultSet.getFinalCostEstimate(true);
        CostEstimate rightCE=rightResultSet.getFinalCostEstimate(true);

        finalCostEstimate=getNewCostEstimate();
        finalCostEstimate.setCost(leftCE.getEstimatedCost(), leftCE.rowCount(),
                leftCE.singleScanRowCount()+ rightCE.singleScanRowCount());

        finalCostEstimate.add(rightCE,finalCostEstimate);
        return finalCostEstimate;
    }

    String getOperatorName(){
        return "UNION";
    }
    @Override
    public String printExplainInformation(String attrDelim) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb = sb.append(spaceToLevel())
                .append(isRecursive?"RecursiveUnion":"Union").append("(")
                .append("n=").append(getResultSetNumber());
        sb.append(attrDelim).append(costEstimate.prettyProcessingString(attrDelim));
        sb = sb.append(")");
        return sb.toString();
    }


    public void setIsRecursive(boolean isRecursive) {
        this.isRecursive = isRecursive;
    }

    public boolean getIsRecursive() {
        return isRecursive;
    }

    public void setViewDescreiptor(TableDescriptor viewDescreiptor) {
        this.viewDescriptor = viewDescreiptor;
    }
}
