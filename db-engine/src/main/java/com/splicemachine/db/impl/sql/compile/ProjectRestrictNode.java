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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.ast.PredicateUtils;
import com.splicemachine.db.impl.ast.RSUtils;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.spark_project.guava.base.Joiner;
import org.spark_project.guava.collect.Lists;

import java.lang.reflect.Modifier;
import java.util.*;

import static java.lang.String.format;

/**
 * A ProjectRestrictNode represents a result set for any of the basic DML
 * operations: SELECT, INSERT, UPDATE, and DELETE.  For INSERT with
 * a VALUES clause, restriction will be null. For both INSERT and UPDATE,
 * the resultColumns in the selectList will contain the names of the columns
 * being inserted into or updated.
 * <p/>
 * NOTE: A ProjectRestrictNode extends FromTable since it can exist in a FromList.
 */

public class ProjectRestrictNode extends SingleChildResultSetNode{
    /**
     * The ValueNode for the restriction to be evaluated here.
     */
    public ValueNode restriction;

    /**
     * Constant expressions to be evaluated here.
     */
    ValueNode constantRestriction=null;

    /**
     * Restriction as a PredicateList
     */
    public PredicateList restrictionList;

    /**
     * List of subqueries in projection
     */
    SubqueryList projectSubquerys;

    /**
     * List of subqueries in restriction
     */
    SubqueryList restrictSubquerys;

    private boolean accessPathModified;

    /* Should we get the table number from this node,
     * regardless of the class of our child.
     */
    private boolean getTableNumberHere;

    /**
     * Initializer for a ProjectRestrictNode.
     *
     * @param childResult       The child ResultSetNode
     * @param projection        The result column list for the projection
     * @param restriction       An expression representing the restriction to be
     *                          evaluated here.
     * @param restrictionList   Restriction as a PredicateList
     * @param projectSubquerys  List of subqueries in the projection
     * @param restrictSubquerys List of subqueries in the restriction
     * @param tableProperties   Properties list associated with the table
     */
    @Override
    public void init(Object childResult,
            Object projection,
            Object restriction,
            Object restrictionList,
            Object projectSubquerys,
            Object restrictSubquerys,
            Object tableProperties){
        super.init(childResult,tableProperties);
        resultColumns=(ResultColumnList)projection;
        this.restriction=(ValueNode)restriction;
        this.restrictionList=(PredicateList)restrictionList;
        this.projectSubquerys=(SubqueryList)projectSubquerys;
        this.restrictSubquerys=(SubqueryList)restrictSubquerys;

		/* A PRN will only hold the tableProperties for
         * a result set tree if its child is not an
		 * optimizable.  Otherwise, the properties will
		 * be transferred down to the child.
		 */
        if(tableProperties!=null &&
                (childResult instanceof Optimizable)){
            ((Optimizable)childResult).setProperties(getProperties());
            setProperties(null);
        }

        if (childResult instanceof ResultSetNode && ((ResultSetNode) childResult).getContainsSelfReference())
            containsSelfReference = true;
    }

	/*
	 *  Optimizable interface
	 */

    @Override
    public boolean nextAccessPath(Optimizer optimizer,
                                  OptimizablePredicateList predList,
                                  RowOrdering rowOrdering) throws StandardException{
		/*
		** If the child result set is an optimizable, let it choose its next
		** access path.  If it is not an optimizable, we have to tell the
		** caller that there is an access path the first time we are called
		** for this position in the join order, and that there are no more
		** access paths for subsequent calls for this position in the join
		** order.  The startOptimizing() method is called once on each
		** optimizable when it is put into a join position.
		*/
        if(childResult instanceof Optimizable){
            return ((Optimizable)childResult).nextAccessPath(optimizer, restrictionList, rowOrdering);
        }else{
            return super.nextAccessPath(optimizer,predList,rowOrdering);
        }
    }

    @Override
    public void rememberAsBest(int planType,Optimizer optimizer) throws StandardException{
        super.rememberAsBest(planType,optimizer);
        if(childResult instanceof Optimizable)
            ((Optimizable)childResult).rememberAsBest(planType,optimizer);
    }

    @Override
    public void startOptimizing(Optimizer optimizer,RowOrdering rowOrdering){
        if(childResult instanceof Optimizable){
            ((Optimizable)childResult).startOptimizing(optimizer,rowOrdering);
        }else{
            super.startOptimizing(optimizer,rowOrdering);
        }
    }

    @Override
    public int getTableNumber(){
		/* GROSS HACK - We need to get the tableNumber after
		 * calling modifyAccessPaths() on the child when doing
		 * a hash join on an arbitrary result set.  The problem
		 * is that the child will always be an optimizable at this
		 * point.  So, we 1st check to see if we should get it from
		 * this node.  (We set the boolean to true in the appropriate
		 * place in modifyAccessPaths().)
		 */
        if(getTableNumberHere){
            return super.getTableNumber();
        }

        if(childResult instanceof Optimizable)
            return ((Optimizable)childResult).getTableNumber();

        return super.getTableNumber();
    }

    @Override
    public CostEstimate optimizeIt(Optimizer optimizer,
                                   OptimizablePredicateList predList,
                                   CostEstimate outerCost,
                                   RowOrdering rowOrdering) throws StandardException{
		/*
		** RESOLVE: Most types of Optimizables only implement estimateCost(),
		** and leave it up to optimizeIt() in FromTable to figure out the
		** total cost of the join.  A ProjectRestrict can have a non-Optimizable
		** child, though, in which case we want to tell the child the
		** number of outer rows - it could affect the join strategy
		** significantly.  So we implement optimizeIt() here, which overrides
		** the optimizeIt() in FromTable.  This assumes that the join strategy
		** for which this join node is the inner table is a nested loop join,
		** which will not be a valid assumption when we implement other
		** strategies like materialization (hash join can work only on
		** base tables).  The join strategy for a base table under a
		** ProjectRestrict is set in the base table itself.
		*/

        CostEstimate childCost;

        costEstimate=getCostEstimate(optimizer);

		/*
		** Don't re-optimize a child result set that has already been fully
		** optimized.  For example, if the child result set is a SelectNode,
		** it will be changed to a ProjectRestrictNode, which we don't want
		** to re-optimized.
		*/
        // NOTE: TO GET THE RIGHT COST, THE CHILD RESULT MAY HAVE TO BE
        // OPTIMIZED MORE THAN ONCE, BECAUSE THE NUMBER OF OUTER ROWS
        // MAY BE DIFFERENT EACH TIME.
        // if (childResultOptimized)
        // 	return costEstimate;

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

		/* If the childResult is instanceof Optimizable, then we optimizeIt.
		 * Otherwise, we are going into a new query block.  If the new query
		 * block has already had its access path modified, then there is
		 * nothing to do.  Otherwise, we must begin the optimization process
		 * anew on the new query block.
		 */
        if(childResult instanceof Optimizable){
            childCost=((Optimizable)childResult).optimizeIt(optimizer,restrictionList,outerCost,rowOrdering);
			/* Copy child cost to this node's cost */
            costEstimate.setCost(childCost);


            // Note: we don't call "optimizer.considerCost()" here because
            // a) the child will make that call as part of its own
            // "optimizeIt()" work above, and b) the child might have
            // different criteria for "considering" (i.e. rejecting or
            // accepting) a plan's cost than this ProjectRestrictNode does--
            // and we don't want to override the child's decision.  So as
            // with most operations in this class, if the child is an
            // Optimizable, we just let it do its own work and make its
            // own decisions.
        }else if(!accessPathModified){
            if(SanityManager.DEBUG){
                if(!((childResult instanceof SelectNode))){
                    SanityManager.THROWASSERT(
                            "childResult is expected to be instanceof "+
                                    "SelectNode or RowResultSetNode - it is a "+
                                    childResult.getClass().getName());
                }
            }

            // Set outer table rows to be 1, because select node should be evaluated independently with outer table
            childResult=childResult.optimize(optimizer.getDataDictionary(), restrictionList, 1);

            // If only select from one table, use the table statistics to estimate cost
            //Table
            SelectNode selectNode = (SelectNode)childResult;
            List<FromBaseTable> baseTables = new ArrayList<>();
            ConglomerateDescriptor cd = null;

            if (selectNode.getFromList().size() == 1) {
                collectBaseTables((FromTable)selectNode.getFromList().elementAt(0), baseTables);
                if (baseTables.size() == 1) {
                    cd = baseTables.get(0).baseConglomerateDescriptor;
                }
            }
			/* Copy child cost to this node's cost */
            costEstimate=childResult.costEstimate;
            getCurrentAccessPath().getJoinStrategy().estimateCost(this,restrictionList,cd,outerCost,optimizer,costEstimate);

			/* Note: Prior to the fix for DERBY-781 we had calls here
			 * to set the cost estimate for BestAccessPath and
			 * BestSortAvoidancePath to equal costEstimate.  That used
			 * to be okay because prior to DERBY-781 we would only
			 * get here once (per join order) for a given SelectNode/
			 * RowResultSetNode and thus we could safely say that the
			 * costEstimate from the most recent call to "optimize()"
			 * was the best one so far (because we knew that we would
			 * only call childResult.optimize() once).  Now that we
			 * support hash joins with subqueries, though, we can get
			 * here twice per join order: once when the optimizer is
			 * considering a nested loop join with this PRN, and once
			 * when it is considering a hash join.  This means we can't
			 * just arbitrarily use the cost estimate for the most recent
			 * "optimize()" as the best cost because that may not
			 * be accurate--it's possible that the above call to
			 * childResult.optimize() was for a hash join, but that
			 * we were here once before (namely for nested loop) and
			 * the cost of the nested loop is actually less than
			 * the cost of the hash join.  In that case it would
			 * be wrong to use costEstimate as the cost of the "best"
			 * paths because it (costEstimate) holds the cost of
			 * the hash join, not of the nested loop join.  So with
			 * DERBY-781 the following calls were removed:
			 *   getBestAccessPath().setCostEstimate(costEstimate);
			 *   getBestSortAvoidancePath().setCostEstimate(costEstimate);
			 * If costEstimate *does* actually hold the estimate for
			 * the best path so far, then we will set BestAccessPath
			 * and BestSortAvoidancePath as needed in the following
			 * call to "considerCost".
			 */

            // childResultOptimized = true;

			/* RESOLVE - ARBITRARYHASHJOIN - Passing restriction list here, as above, is correct.
			 * However,  passing predList makes the following work:
			 *	select * from t1, (select * from t2) c properties joinStrategy = hash where t1.c1 = c.c1;
			 * The following works with restrictionList:
			 *	select * from t1, (select c1 + 0 from t2) c(c1) properties joinStrategy = hash where t1.c1 = c.c1;
			 */
            optimizer.considerCost(this,restrictionList,getCostEstimate(),outerCost);
        }

        return costEstimate;
    }

    private void collectBaseTables(ResultSetNode node, List<FromBaseTable> baseTables) {

        if (node == null) {
            return;
        }
        
        if (node instanceof FromBaseTable) {
            baseTables.add((FromBaseTable) node);
            return;
        }

        if (node instanceof SingleChildResultSetNode) {
            collectBaseTables(((SingleChildResultSetNode) node).childResult, baseTables);
        }
    }

    @Override
    public boolean feasibleJoinStrategy(OptimizablePredicateList predList,
                                        Optimizer optimizer,
                                        CostEstimate outerCost) throws StandardException{

		/* The child being an Optimizable is a special case.  In that
		 * case, we want to get the current access path and join strategy
		 * from the child.  Otherwise, we want to get it from this node.
		 */
        if(childResult instanceof Optimizable){
            // With DERBY-805 it's possible that, when considering a nested
            // loop join with this PRN, we pushed predicates down into the
            // child if the child is a UNION node.  At this point, though, we
            // may be considering doing a hash join with this PRN instead of a
            // nested loop join, and if that's the case we need to pull any
            // predicates back up so that they can be searched for equijoins
            // that will in turn make the hash join possible.  So that's what
            // the next call does.  Two things to note: 1) if no predicates
            // were pushed, this call is a no-op; and 2) if we get here when
            // considering a nested loop join, the predicates that we pull
            // here (if any) will be re-pushed for subsequent costing/
            // optimization as necessary (see OptimizerImpl.costPermutation(),
            // which will call this class's optimizeIt() method and that's
            // where the predicates are pushed down again).
            if(childResult instanceof UnionNode)
                ((UnionNode)childResult).pullOptPredicates(restrictionList);

            return ((Optimizable)childResult).feasibleJoinStrategy(restrictionList,optimizer,outerCost);
        }else{
            return super.feasibleJoinStrategy(restrictionList,optimizer,outerCost);
        }
    }

    @Override
    public AccessPath getCurrentAccessPath(){
        if(childResult instanceof Optimizable)
            return ((Optimizable)childResult).getCurrentAccessPath();

        return super.getCurrentAccessPath();
    }

    @Override
    public AccessPath getBestAccessPath(){
        if(childResult instanceof Optimizable)
            return ((Optimizable)childResult).getBestAccessPath();

        return super.getBestAccessPath();
    }

    @Override
    public AccessPath getBestSortAvoidancePath(){
        if(childResult instanceof Optimizable)
            return ((Optimizable)childResult).getBestSortAvoidancePath();

        return super.getBestSortAvoidancePath();
    }

    @Override
    public AccessPath getTrulyTheBestAccessPath(){
		/* The childResult will always be an Optimizable
		 * during code generation.  If the childResult was
		 * not an Optimizable during optimization, then this node
		 * will have the truly the best access path, so we want to
		 * return it from this node, rather than traversing the tree.
		 * This can happen for non-flattenable derived tables.
		 * Anyway, we note this state when modifying the access paths.
		 */
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

    @Override
    public void rememberSortAvoidancePath(){
        if(childResult instanceof Optimizable)
            ((Optimizable)childResult).rememberSortAvoidancePath();
        else
            super.rememberSortAvoidancePath();
    }

    @Override
    public boolean considerSortAvoidancePath(){
        if(childResult instanceof Optimizable)
            return ((Optimizable)childResult).considerSortAvoidancePath();

        return super.considerSortAvoidancePath();
    }

    @Override
    public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate) throws StandardException{
        assert optimizablePredicate instanceof Predicate:"optimizablePredicate expected to be instanceof Predicate";
        assert !optimizablePredicate.hasSubquery(): "optimizable predicate cannot have a subquery";
        assert !optimizablePredicate.hasMethodCall(): "optimizablePredicate cannot have a method call";

        /* Add the matching predicate to the restrictionList */
        if(restrictionList==null){
            restrictionList=(PredicateList)getNodeFactory().getNode(
                    C_NodeTypes.PREDICATE_LIST,
                    getContextManager());
        }

        restrictionList.addPredicate((Predicate)optimizablePredicate);

      /* Remap all of the ColumnReferences to point to the
       * source of the values.
       */
        Predicate pred=(Predicate)optimizablePredicate;

      /* If the predicate is scoped then the call to "remapScopedPred()"
       * will do the necessary remapping for us and will return true;
       * otherwise, we'll just do the normal remapping here.
       */
        if(!pred.remapScopedPred()){
            RemapCRsVisitor rcrv=new RemapCRsVisitor(true);
            pred.getAndNode().accept(rcrv);
        }
        return true;
    }

    @Override
    public void pullOptPredicates(OptimizablePredicateList optimizablePredicates) throws StandardException{
        // DERBY-4001: Don't pull predicates if this node is part of a NOT
        // EXISTS join. For example, in the query below, if we allowed the
        // predicate 1<>1 (always false) to be pulled, no rows would be
        // returned, whereas it should return all the rows in table T.
        // SELECT * FROM T WHERE NOT EXISTS (SELECT * FROM T WHERE 1<>1)
        if(restrictionList!=null && !isNotExists()){
            // Pull up any predicates that may have been pushed further
            // down the tree during optimization.
            if(childResult instanceof UnionNode)
                ((UnionNode)childResult).pullOptPredicates(restrictionList);

            RemapCRsVisitor rcrv=new RemapCRsVisitor(false);
            for(int i=restrictionList.size()-1;i>=0;i--){
                OptimizablePredicate optPred= restrictionList.getOptPredicate(i);
                ((Predicate)optPred).getAndNode().accept(rcrv);
                optimizablePredicates.addOptPredicate(optPred);
                restrictionList.removeOptPredicate(i);
            }
        }
    }

    public void pullRowIdPredicates(OptimizablePredicateList optimizablePredicates) throws StandardException{
        // pull up rowid predicates that are not marked as start or stop key
        if (restrictionList != null) {
            for (int i = restrictionList.size() - 1; i >= 0; i--) {
                OptimizablePredicate optPred = restrictionList.getOptPredicate(i);
                if (optPred.isRowId()) {
                    if (!optPred.isStartKey() && !optPred.isStopKey()) {
                        optimizablePredicates.addOptPredicate(optPred);
                        restrictionList.removeOptPredicate(i);
                    }
                }
            }
        }
    }
    @Override
    public Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException{
        boolean origChildOptimizable=true;

		/* It is okay to optimize most nodes multiple times.  However,
		 * modifying the access path is something that should only be done
		 * once per node.  One reason for this is that the predicate list
		 * will be empty after the 1st call, and we assert that it should
		 * be non-empty.  Multiple calls to modify the access path can
		 * occur when there is a non-flattenable FromSubquery (or view).
		 */
        if(accessPathModified){
            return this;
        }

		/*
		** Do nothing if the child result set is not optimizable, as there
		** can be nothing to modify.
		*/
        boolean alreadyPushed=false;
        if(!(childResult instanceof Optimizable)){
            // Remember that the original child was not Optimizable
            origChildOptimizable=false;

			/* When we optimized the child we passed in our restriction list
			 * so that scoped predicates could be pushed further down the
			 * tree.  We need to do the same when modifying the access
			 * paths to ensure we generate the same plans the optimizer
			 * chose.
			 */
            childResult=childResult.modifyAccessPaths(restrictionList);

			/* Mark this node as having the truly ... for
			 * the underlying tree.
			 */
            hasTrulyTheBestAccessPath=true;

			/* Replace this PRN with a HRN if we are doing a hash join */
            if(trulyTheBestAccessPath.getJoinStrategy().isHashJoin()){
                assert restrictionList!=null: "restrictionList expected to be non-null";
                assert !restrictionList.isEmpty() : "restrictionList.size() expected to be non-zero";
				/* We're doing a hash join on an arbitary result set.
				 * We need to get the table number from this node when
				 * dividing up the restriction list for a hash join.
				 * We need to explicitly remember this.
				 */
                getTableNumberHere=true;
            }else{
                if (getCompilerContext().isProjectionPruningEnabled()) {
                    /* after building the optimized tree bottom-up, we know which fields are not referenced
                     * and can be pruned
                     */
                    resultColumns.doProjection(false);
                }
				/* We consider materialization into a temp table as a last step.
				 * Currently, we only materialize VTIs that are inner tables
				 * and can't be instantiated multiple times.  In the future we
				 * will consider materialization as a cost based option.
				 */
                return (Optimizable)considerMaterialization(outerTables);
            }
        }

		/* If the child is not a FromBaseTable, then we want to
		 * keep going down the tree.  (Nothing to do at this node.)
		 */
        else if(!(childResult instanceof FromBaseTable)){
			/* Make sure that we have a join strategy */
            if(trulyTheBestAccessPath.getJoinStrategy()==null){
                trulyTheBestAccessPath=((Optimizable)childResult).getTrulyTheBestAccessPath();
            }

            // If the childResult is a SetOperatorNode (esp. a UnionNode),
            // then it's possible that predicates in our restrictionList are
            // supposed to be pushed further down the tree (as of DERBY-805).
            // We passed the restrictionList down when we optimized the child
            // so that the relevant predicates could be pushed further as part
            // of the optimization process; so now that we're finalizing the
            // paths, we need to do the same thing: i.e. pass restrictionList
            // down so that the predicates that need to be pushed further
            // _can_ be pushed further.
            if(childResult instanceof SetOperatorNode){
                childResult=(ResultSetNode)((SetOperatorNode)childResult).modifyAccessPath(outerTables,restrictionList);

                // Take note of the fact that we already pushed predicates
                // as part of the modifyAccessPaths call.  This is necessary
                // because there may still be predicates in restrictionList
                // that we intentionally decided not to push (ex. if we're
                // going to do hash join then we chose to not push the join
                // predicates).  Whatever the reason for not pushing the
                // predicates, we have to make sure we don't inadvertenly
                // push them later (esp. as part of the "pushUsefulPredicates"
                // call below).
                alreadyPushed=true;
            }else{
                childResult=(ResultSetNode)((FromTable)childResult).modifyAccessPath(outerTables);
            }
        }

        if (getCompilerContext().isProjectionPruningEnabled()) {
            /* after building the optimized tree bottom-up, we know which fields are not referenced
             * and can be pruned
             */
            resultColumns.doProjection(false);
        }

        if((restrictionList!=null) && !alreadyPushed){
            restrictionList.pushUsefulPredicates((Optimizable)childResult);
        }

		/*
		** The optimizer's decision on the access path for the child result
		** set may require the generation of extra result sets.  For
		** example, if it chooses an index, we need an IndexToBaseRowNode
		** above the FromBaseTable (and the FromBaseTable has to change
		** its column list to match that of the index.
		*/
        if(origChildOptimizable){
            childResult=childResult.changeAccessPath();
        }
        accessPathModified=true;

		/*
		** Replace this PRN with a HTN if a hash join
		** is being done at this node.  (hash join on a scan
		** is a special case and is handled at the FBT.)
		*/
        if(trulyTheBestAccessPath.getJoinStrategy()!=null &&
                trulyTheBestAccessPath.getJoinStrategy().isHashJoin()){
            return replaceWithHashTableNode();
        }

		/* We consider materialization into a temp table as a last step.
		 * Currently, we only materialize VTIs that are inner tables
		 * and can't be instantiated multiple times.  In the future we
		 * will consider materialization as a cost based option.
		 */
        return (Optimizable)considerMaterialization(outerTables);
    }

    /**
     * This method creates a HashTableNode between the PRN and
     * it's child when the optimizer chooses hash join on an
     * arbitrary (non-FBT) result set tree.
     * We divide up the restriction list into 3 parts and
     * distribute those parts as described below.
     *
     * @return The new (same) top of our result set tree.
     * @throws StandardException Thrown on error
     */
    private Optimizable replaceWithHashTableNode() throws StandardException{
        // If this PRN has TTB access path for its child, store that access
        // path in the child here, so that we can find it later when it
        // comes time to generate qualifiers for the hash predicates (we
        // need the child's access path when generating qualifiers; if we
        // don't pass the path down here, the child won't be able to find
        // it).
        if(hasTrulyTheBestAccessPath){
            ((FromTable)childResult).trulyTheBestAccessPath=(AccessPathImpl)getTrulyTheBestAccessPath();

            // If the child itself is another SingleChildResultSetNode
            // (which is also what a ProjectRestrictNode is), then tell
            // it that it is now holding TTB path for it's own child.  Again,
            // this info is needed so that child knows where to find the
            // access path at generation time.
            if(childResult instanceof SingleChildResultSetNode){
                ((SingleChildResultSetNode)childResult).hasTrulyTheBestAccessPath=true;

                // While we're at it, add the PRN's table number to the
                // child's referenced map so that we can find the equijoin
                // predicate.  We have to do this because the predicate
                // will be referencing the PRN's tableNumber, not the
                // child's--and since we use the child as the target
                // when searching for hash keys (as can be seen in
                // HashJoinStrategy.divideUpPredicateLists()), the child
                // should know what this PRN's table number is.  This
                // is somewhat bizarre since the child doesn't
                // actually "reference" this PRN, but since the child's
                // reference map is used when searching for the equijoin
                // predicate (see "buildTableNumList" in
                // BinaryRelationalOperatorNode), this is the simplest
                // way to pass this PRN's table number down.
                childResult.getReferencedTableMap().set(tableNumber);
            }
        }

		/* We want to divide the predicate list into 3 separate lists -
		 *	o predicates against the source of the hash table, which will
		 *	  be applied on the way into the hash table (searchRestrictionList)
		 *  o join clauses which are qualifiers and get applied to the
		 *	  rows in the hash table on a probe (joinRestrictionList)
		 *	o non-qualifiers involving both tables which will get
		 *	  applied after a row gets returned from the HTRS (nonQualifiers)
		 *
		 * We do some unnecessary work when doing this as we want to reuse
		 * as much existing code as possible.  The code that we are reusing
		 * was originally built for hash scans, hence the unnecessary
		 * requalification list.
		 */
        ContextManager ctxMgr=getContextManager();
        PredicateList searchRestrictionList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,ctxMgr);
        PredicateList joinQualifierList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,ctxMgr);
        PredicateList requalificationRestrictionList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,ctxMgr);
        trulyTheBestAccessPath.getJoinStrategy().divideUpPredicateLists(this,
                restrictionList,
                searchRestrictionList,
                joinQualifierList,
                requalificationRestrictionList,
                getDataDictionary());

		/* Break out the non-qualifiers from HTN's join qualifier list and make that
		 * the new restriction list for this PRN.
		 */
        restrictionList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,ctxMgr);
        /* For non-base table, we remove first 2 lists from requal list to avoid adding duplicates.
         */
        for(int i=0;i<searchRestrictionList.size();i++)
            requalificationRestrictionList.removeOptPredicate(searchRestrictionList.elementAt(i));
        for(int i=0;i<joinQualifierList.size();i++)
            requalificationRestrictionList.removeOptPredicate(joinQualifierList.elementAt(i));

        joinQualifierList.transferNonQualifiers(this,restrictionList); //purify joinQual list
        requalificationRestrictionList.copyPredicatesToOtherList(restrictionList); //any residual

        ResultColumnList htRCList;

		/* We get a shallow copy of the child's ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
        htRCList=childResult.getResultColumns();
        childResult.setResultColumns(htRCList.copyListAndObjects());

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the HTN's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 * NOTE: We don't want to mark the underlying RCs as referenced, otherwise
		 * we won't be able to project out any of them.
		 */
        htRCList.genVirtualColumnNodes(childResult,childResult.getResultColumns(),false);

		/* The CRs for this side of the join in both the searchRestrictionList
		 * the joinQualifierList now point to the HTN's RCL.  We need them
		 * to point to the RCL in the child of the HTN.  (We skip doing this for
		 * the joinQualifierList as the code to generate the Qualifiers does not
		 * care.)
		 */
        RemapCRsVisitor rcrv=new RemapCRsVisitor(true);
        searchRestrictionList.accept(rcrv);

		/* We can finally put the HTN between ourself and our old child. */
        childResult=(ResultSetNode)getNodeFactory().getNode(
                C_NodeTypes.HASH_TABLE_NODE,
                childResult,
                tableProperties,
                htRCList,
                searchRestrictionList,
                joinQualifierList,
                trulyTheBestAccessPath,
                getCostEstimate(),
                projectSubquerys,
                restrictSubquerys,
                hashKeyColumns(),
                ctxMgr);
        return this;
    }

    @Override
    public void verifyProperties(DataDictionary dDictionary) throws StandardException{
		/* Table properties can be attached to this node if
		 * its child is not an optimizable, otherwise they
		 * are attached to its child.
		 */

        if(childResult instanceof Optimizable){
            ((Optimizable)childResult).verifyProperties(dDictionary);
        }else{
            super.verifyProperties(dDictionary);
        }
    }

    @Override
    public boolean legalJoinOrder(JBitSet assignedTableMap){
        if (dependencyMap != null) {
            if (existsTable || fromSSQ)
                // the check of getFirstSetBit()!= -1 ensures that exists table or table converted from SSQ won't be the leftmost table
                return (assignedTableMap.getFirstSetBit()!= -1) && assignedTableMap.contains(dependencyMap);
            else
                return assignedTableMap.contains(dependencyMap);
        }

        return !(childResult instanceof Optimizable) || ((Optimizable)childResult).legalJoinOrder(assignedTableMap);
    }

    @Override
    public double uniqueJoin(OptimizablePredicateList predList) throws StandardException{
        if(childResult instanceof Optimizable){
            return ((Optimizable)childResult).uniqueJoin(predList);
        }else{
            return super.uniqueJoin(predList);
        }
    }

    /**
     * Return the restriction list from this node.
     *
     * @return The restriction list from this node.
     */
    PredicateList getRestrictionList(){
        return restrictionList;
    }

    /**
     * Return the user specified join strategy, if any for this table.
     *
     * @return The user specified join strategy, if any for this table.
     */
    @Override
    String getUserSpecifiedJoinStrategy(){
        if(childResult instanceof FromTable){
            return ((FromTable)childResult).getUserSpecifiedJoinStrategy();
        }else{
            return userSpecifiedJoinStrategy;
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

            if(restriction!=null){
                printLabel(depth,"restriction: ");
                restriction.treePrint(depth+1);
            }

            if(restrictionList!=null  && !restrictionList.isEmpty()){
                printLabel(depth,"restrictionList: ");
                restrictionList.treePrint(depth+1);
            }

            if(projectSubquerys!=null){
                printLabel(depth,"projectSubquerys: ");
                projectSubquerys.treePrint(depth+1);
            }

            if(restrictSubquerys!=null){
                printLabel(depth,"restrictSubquerys: ");
                restrictSubquerys.treePrint(depth+1);
            }
        }
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
     * Push expressions down to the first ResultSetNode which can do expression
     * evaluation and has the same referenced table map.
     * RESOLVE - This means only pushing down single table expressions to
     * ProjectRestrictNodes today.  Once we have a better understanding of how
     * the optimizer will work, we can push down join clauses.
     *
     * @param predicateList The PredicateList.
     * @throws StandardException Thrown on error
     */
    @Override
    public void pushExpressions(PredicateList predicateList) throws StandardException{
        PredicateList pushPList;

        assert predicateList!=null: "predicateList is expected to be non-null";

		/* Push single table predicates down to the left of an outer
		 * join, if possible.  (We need to be able to walk an entire
		 * join tree.)
		 */
        if(childResult instanceof JoinNode){
            ((FromTable)childResult).pushExpressions(predicateList);

        }

		/* Build a list of the single table predicates that we can push down */
        pushPList=predicateList.getPushablePredicates(referencedTableMap);

		/* If this is a PRN above a SelectNode, probably due to a 
		 * view or derived table which couldn't be flattened, then see
		 * if we can push any of the predicates which just got pushed
		 * down to our level into the SelectNode.
		 */
        if(pushPList!=null && (childResult instanceof SelectNode)){
            SelectNode childSelect=(SelectNode)childResult;

            // We can't push down if there is a window
            // function because that would make ROW_NUMBER give wrong
            // result:
            // E.g.
            //     SELECT * from (SELECT ROW_NUMBER() OVER (), j FROM T
            //                    ORDER BY j) WHERE j=5
            //
            // Similarly, don't push if we have OFFSET and/or FETCH FROM.
            //
            if((!childSelect.hasWindows() && childSelect.fetchFirst==null && childSelect.offset==null)){
                optimizeTrace(OptimizerFlag.JOIN_NODE_PREDICATE_MANIPULATION,0,0,0.0,
                                         "ProjectRestrictNode pushing predicates.",pushPList);
                pushPList.pushExpressionsIntoSelect((SelectNode)childResult,false);
            }
        }


		/* DERBY-649: Push simple predicates into Unions. It would be up to UnionNode
		 * to decide if these predicates can be pushed further into underlying SelectNodes
		 * or UnionNodes.  Note, we also keep the predicateList at this
		 * ProjectRestrictNode in case the predicates are not pushable or only
		 * partially pushable.
		 *
		 * It is possible to expand this optimization in UnionNode later.
		 */
        if(pushPList!=null && (childResult instanceof UnionNode))
            ((UnionNode)childResult).pushExpressions(pushPList);

        if(restrictionList==null){
            restrictionList=pushPList;
        }else if(pushPList!=null && !pushPList.isEmpty()){
			/* Concatenate the 2 PredicateLists */
            restrictionList.destructiveAppend(pushPList);
        }

		/* RESOLVE - this looks like the place to try to try to push the 
		 * predicates through the ProjectRestrict.  Seems like we should
		 * "rebind" the column references and reset the referenced table maps
		 * in restrictionList and then call childResult.pushExpressions() on
		 * restrictionList.
		 */
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
        if(restrictionList==null){
            restrictionList=(PredicateList)getNodeFactory().getNode( C_NodeTypes.PREDICATE_LIST, getContextManager());
        }
        restrictionList.addPredicate(predicate);
        return this;
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
     * Optimize this ProjectRestrictNode.
     *
     * @param dataDictionary The DataDictionary to use for optimization
     * @param predicates     The PredicateList to optimize.  This should
     *                       be a join predicate.
     * @param outerRows      The number of outer joining rows
     * @throws StandardException Thrown on error
     * @return ResultSetNode    The top of the optimized subtree
     */
    @Override
    public ResultSetNode optimize(DataDictionary dataDictionary,
                                  PredicateList predicates,
                                  double outerRows) throws StandardException{
		/* We need to implement this method since a PRN can appear above a
		 * SelectNode in a query tree.
		 */
        childResult=childResult.optimize(dataDictionary, restrictionList, outerRows);

        Optimizer optimizer=getOptimizer(
                (FromList)getNodeFactory().getNode(C_NodeTypes.FROM_LIST,
                        getNodeFactory().doJoinOrderOptimization(),
                        this,
                        getContextManager()),
                predicates,
                dataDictionary,
                null);

        // RESOLVE: SHOULD FACTOR IN THE NON-OPTIMIZABLE PREDICATES THAT
        // WERE NOT PUSHED DOWN
        costEstimate=optimizer.newCostEstimate();

        CostEstimate childCost=childResult.getCostEstimate();
        this.costEstimate.setCost(childCost);
//        this.costEstimate.setCost(childCost.getEstimatedCost(),childCost.rowCount(),childCost.singleScanRowCount());

        return this;
    }

    /**
     * Get the CostEstimate for this ProjectRestrictNode.
     *
     * @return The CostEstimate for this ProjectRestrictNode, which is
     * the cost estimate for the child node.
     */
    @Override
    public CostEstimate getCostEstimate(){
		/*
		** The cost estimate will be set here if either optimize() or
		** optimizeIt() was called on this node.  It's also possible
		** that optimization was done directly on the child node,
		** in which case the cost estimate will be null here.
		*/
        if(costEstimate==null)
            return childResult.getCostEstimate();
        else{
            return costEstimate;
        }
    }

    /**
     * Get the final CostEstimate for this ProjectRestrictNode.
     *
     * @return The final CostEstimate for this ProjectRestrictNode, which is
     * the final cost estimate for the child node.
     */
    @Override
    public CostEstimate getFinalCostEstimate(boolean useSelf) throws StandardException{
        if(finalCostEstimate!=null)
            // we already set it, so just return it.
            return finalCostEstimate;

        if (hasTrulyTheBestAccessPath) {
            finalCostEstimate = getTrulyTheBestAccessPath().getCostEstimate();
            return finalCostEstimate;
        }
        // If the child result set is an Optimizable, then this node's
        // final cost is that of the child.  Otherwise, this node must
        // hold "trulyTheBestAccessPath" for it's child so we pull
        // the final cost from there.
        if(childResult instanceof Optimizable)
            finalCostEstimate = childResult.getFinalCostEstimate(true);
        else
            finalCostEstimate=getTrulyTheBestAccessPath().getCostEstimate();

        return finalCostEstimate;
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
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException{
        assert resultColumns!=null: "Tree structure bad";

        //
        // If we are projecting and restricting the stream from a table
        // function, then give the table function all of the information that
        // it needs in order to push the projection and qualifiers into
        // the table function. See DERBY-4357.
        //
        if(childResult instanceof FromVTI){
            ((FromVTI)childResult).computeProjectionAndRestriction(restrictionList);
        }

        generateMinion(acb,mb,false);
    }

    /**
     * General logic shared by Core compilation.
     *
     * @param acb The ExpressionClassBuilder for the class being built
     * @param mb  The method the expression will go into
     * @throws StandardException Thrown on error
     */
    @Override
    public void generateResultSet(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException{
        generateMinion(acb,mb,true);
    }

    private void generateChild(ExpressionClassBuilder acb,
                               MethodBuilder mb,
                               boolean genChildResultSet) throws StandardException {

        if(genChildResultSet)
            childResult.generateResultSet(acb,mb);
        else
            childResult.generate((ActivationClassBuilder)acb,mb);
    }
    /**
     * Generate a new array with "numberOfValues" Strings and place it on the stack.
     *
     * @param acb            The ActivationClassBuilder for the class we're building
     * @param mb             The MethodBuilder on whose stack to place the String[].
     * @param resultColumns  The expressions to add to the list.
     * @return The field that holds the list data
     */
    public static void
    generateExpressionsArrayOnStack(ExpressionClassBuilder acb,
                                    MethodBuilder          mb,
                                    ResultColumnList       resultColumns)
    throws StandardException {

        boolean genExpressions = resultColumns != null;
        int numberOfValues = genExpressions ? resultColumns.size() : 0;

        String [] expressions = new String[numberOfValues];

        int i = 0;
        String tempString;
        StringBuilder sb = new StringBuilder();
        if (genExpressions) {
            for (ResultColumn rc : resultColumns) {
                tempString =
                expressions[i] = OperatorToString.opToSparkString(rc.getExpression());
                if (expressions[i].isEmpty()) {
                    genExpressions = false;
                    numberOfValues = 0;
                    break;
                }
                sb.append(tempString);
                sb.append(format(" as %s", ValueRow.getNamedColumn(i)));
                expressions[i] = sb.toString();
                sb.setLength(0);
                i++;
            }
        }

        String stringClassName = "java.lang.String";
        LocalField arrayField = acb.newFieldDeclaration(
		                            Modifier.PRIVATE, stringClassName + "[]");
        mb.pushNewArray(stringClassName, numberOfValues);
        mb.setField(arrayField);

        i = 0;
        if (genExpressions) {
            // Now copy the strings we built previously into the String[].
            for (ResultColumn rc : resultColumns) {
                mb.getField(arrayField);
                mb.push(expressions[i]);
                mb.setArrayElement(i++);
            }
        }

        // Now push a reference to the String array we just built onto the stack.
        mb.getField(arrayField);
    }

    /**
     * Logic shared by generate() and generateResultSet().
     *
     * @param acb The ExpressionClassBuilder for the class being built
     * @param mb  The method the expression will go into
     * @throws StandardException Thrown on error
     */

    private void generateMinion(ExpressionClassBuilder acb,
                                MethodBuilder mb,
                                boolean genChildResultSet) throws StandardException{
		/* If this ProjectRestrict doesn't do anything, bypass its generation.
		 * (Remove any true and true predicates first, as they could be left
		 * by the like transformation.)
		 */
        if(restrictionList!=null && !restrictionList.isEmpty()){
            restrictionList.eliminateBooleanTrueAndBooleanTrue();
        }

        if(nopProjectRestrict()){
            generateNOPProjectRestrict();
            generateChild(acb, mb, genChildResultSet);
            costEstimate=childResult.getFinalCostEstimate(true);
            return;
        }

        // build up the tree.

		/* Put the predicates back into the tree */
        if(restrictionList!=null){
            constantRestriction=restrictionList.restoreConstantPredicates();
            // Remove any redundant predicates before restoring
            restrictionList.removeRedundantPredicates();
            restriction=restrictionList.restorePredicates();
			/* Allow the restrictionList to get garbage collected now
			 * that we're done with it.
			 */
             // restrictionList=null; DO NOT CLEANUP :JL Explain Plan Still needs this possibly.
        }

        // for the restriction, we generate an exprFun
        // that evaluates the expression of the clause
        // against the current row of the child's result.
        // if the restriction is empty, simply pass null
        // to optimize for run time performance.

        // generate the function and initializer:
        // Note: Boolean lets us return nulls (boolean would not)
        // private Boolean exprN()
        // {
        //   return <<restriction.generate(ps)>>;
        // }
        // static Method exprN = method pointer to exprN;


        // Map the result columns to the source columns

        ResultColumnList.ColumnMapping mappingArrays= resultColumns.mapSourceColumns();

        int[] mapArray=mappingArrays.mapArray;
        boolean[] cloneMap=mappingArrays.cloneMap;

        int mapArrayItem=acb.addItem(new ReferencedColumnsDescriptorImpl(mapArray));
        int cloneMapItem=acb.addItem(cloneMap);

		/* Will this node do a projection? */
        boolean doesProjection=true;

		/* Does a projection unless same # of columns in same order
		 * as child.
		 */
        if((!reflectionNeededForProjection())&&mapArray!=null&&mapArray.length==childResult.getResultColumns().size()){
			/* mapArray entries are 1-based */
            int index=0;
            for(;index<mapArray.length;index++){
                if(mapArray[index]!=index+1){
                    break;
                }
            }
            if(index==mapArray.length){
                doesProjection=false;
            }
        }


		/* Generate the ProjectRestrictSet:
		 *	arg1: childExpress - Expression for childResultSet
		 *  arg2: Activation
		 *  arg3: restrictExpress - Expression for restriction
		 *  arg4: projectExpress - Expression for projection
		 *  arg5: resultSetNumber
		 *  arg6: constantExpress - Expression for constant restriction
		 *			(for example, where 1 = 2)
		 *  arg7: mapArrayItem - item # for mapping of source columns
         *  arg8: cloneMapItem - item # for mapping of columns that need cloning
         *  arg9: reuseResult - whether or not the result row can be reused
         *                      (ie, will it always be the same)
         *  arg10: doesProjection - does this node do a projection
         *  arg11: estimated row count
         *  arg12: estimated cost
         *  arg13: close method
         */

        acb.pushGetResultSetFactoryExpression(mb);
        generateChild(acb, mb, genChildResultSet);

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
        assignResultSetNumber();

        // Load our final cost estimate.
        costEstimate=getFinalCostEstimate(false);

        // if there is no restriction, we just want to pass null.
        if(restriction==null){
            mb.pushNull(ClassName.GeneratedMethod);
        }
        else {
            // this sets up the method and the static field.
            // generates:
            // 	Object userExprFun { }
            MethodBuilder userExprFun=acb.newUserExprFun();

            // restriction knows it is returning its value;

			/* generates:
			 *    return  <restriction.generate(acb)>;
			 * and adds it to userExprFun
			 * NOTE: The explicit cast to DataValueDescriptor is required
			 * since the restriction may simply be a boolean column or subquery
			 * which returns a boolean.  For example:
			 *		where booleanColumn
			 */
            restriction.generateExpression(acb,userExprFun);
            userExprFun.methodReturn();

            // we are done modifying userExprFun, complete it.
            userExprFun.complete();

            // restriction is used in the final result set as an access of the new static
            // field holding a reference to this new method.
            // generates:
            //	ActivationClass.userExprFun
            // which is the static field that "points" to the userExprFun
            // that evaluates the where clause.
            acb.pushMethodReference(mb,userExprFun);
        }

		/* Determine whether or not reflection is needed for the projection.
		 * Reflection is not needed if all of the columns map directly to source
		 * columns.
		 */
        boolean canUseSparkSQLExpressions = false;
        boolean hasGroupingFunction = false;
        if(reflectionNeededForProjection()){
            // for the resultColumns, we generate a userExprFun
            // that creates a new row from expressions against
            // the current row of the child's result.
            // (Generate optimization: see if we can simply
            // return the current row -- we could, but don't, optimize
            // the function call out and have execution understand
            // that a null function pointer means take the current row
            // as-is, with the performance trade-off as discussed above.)

            /* Generate the Row function for the projection */
            canUseSparkSQLExpressions = resultColumns.generateCore(acb,mb,false);
            hasGroupingFunction = resultColumns.hasGroupingFunction();
        }else{
            mb.pushNull(ClassName.GeneratedMethod);
        }

        mb.push(resultSetNumber);

        // if there is no constant restriction, we just want to pass null.
        if(constantRestriction==null){
            mb.pushNull(ClassName.GeneratedMethod);
        }else{
            // this sets up the method and the static field.
            // generates:
            // 	userExprFun { }
            MethodBuilder userExprFun=acb.newUserExprFun();

            // restriction knows it is returning its value;

			/* generates:
			 *    return <restriction.generate(acb)>;
			 * and adds it to userExprFun
			 * NOTE: The explicit cast to DataValueDescriptor is required
			 * since the restriction may simply be a boolean column or subquery
			 * which returns a boolean.  For example:
			 *		where booleanColumn
			 */
            constantRestriction.generateExpression(acb,userExprFun);

            userExprFun.methodReturn();

            // we are done modifying userExprFun, complete it.
            userExprFun.complete();

            // restriction is used in the final result set as an access
            // of the new static field holding a reference to this new method.
            // generates:
            //	ActivationClass.userExprFun
            // which is the static field that "points" to the userExprFun
            // that evaluates the where clause.
            acb.pushMethodReference(mb,userExprFun);
        }

        mb.push(mapArrayItem);
        mb.push(cloneMapItem);
        mb.push(resultColumns.reusableResult());
        mb.push(doesProjection);
        mb.push(costEstimate.rowCount());
        mb.push(costEstimate.getEstimatedCost());
        mb.push(printExplainInformationForActivation());

        String filterPred = OperatorToString.opToSparkString(restriction);
        mb.push(filterPred);

        ProjectRestrictNode.generateExpressionsArrayOnStack(acb, mb, canUseSparkSQLExpressions ? resultColumns : null);
        mb.push(hasGroupingFunction);
        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,"getProjectRestrictResultSet", ClassName.NoPutResultSet,15);
    }

    /**
     * Determine whether this ProjectRestrict does anything.  If it doesn't
     * filter out any rows or columns, it's a No-Op.
     *
     * @return true if this ProjectRestrict is a No-Op.
     */
    public boolean nopProjectRestrict(){
		/*
		** This ProjectRestrictNode is not a No-Op if it does any
		** restriction.
		*/
        if((restriction!=null) || (constantRestriction!=null) || (restrictionList!=null && !restrictionList.isEmpty())){
            return false;
        }

        ResultColumnList childColumns=childResult.getResultColumns();
        ResultColumnList PRNColumns=this.getResultColumns();

		/*
		** The two lists have the same numbers of elements.  Are the lists
		** identical?  In other words, is the expression in every ResultColumn
		** in the PRN's RCL a ColumnReference that points to the same-numbered
		** column?
		*/
        return PRNColumns.nopProjection(childColumns);

    }

    /**
     * Bypass the generation of this No-Op ProjectRestrict, and just generate
     * its child result set.
     *
     * @throws StandardException Thrown on error
     */
    public void generateNOPProjectRestrict() throws StandardException{
        this.getResultColumns().setRedundant();
    }

    /**
     * Consider materialization for this ResultSet tree if it is valid and cost effective
     * (It is not valid if incorrect results would be returned.)
     *
     * @return Top of the new/same ResultSet tree.
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultSetNode considerMaterialization(JBitSet outerTables) throws StandardException{
        childResult=childResult.considerMaterialization(outerTables);
        if(childResult.performMaterialization(outerTables)){
            MaterializeResultSetNode mrsn;
            ResultColumnList prRCList;

			/* If the restriction contians a ColumnReference from another
			 * table then the MRSN must go above the childResult.  Otherwise we can put
			 * it above ourselves. (The later is optimal since projection and restriction 
			 * will only happen once.)
			 * Put MRSN above PRN if any of the following are true:
			 *	o  PRN doesn't have a restriction list
			 *	o  PRN's restriction list is empty 
			 *  o  Table's referenced in PRN's restriction list are a subset of
			 *	   table's referenced in PRN's childResult.  (NOTE: Rather than construct
			 *     a new, empty JBitSet before checking, we simply clone the childResult's
			 *	   referencedTableMap.  This is done for code simplicity and will not 
			 *	   affect the result.)
			 */
            ReferencedTablesVisitor rtv=new ReferencedTablesVisitor((JBitSet)childResult.getReferencedTableMap().clone());
            boolean emptyRestrictionList=(restrictionList==null || restrictionList.isEmpty());
            if(!emptyRestrictionList){
                restrictionList.accept(rtv);
            }
            if(emptyRestrictionList ||
                    childResult.getReferencedTableMap().contains(rtv.getTableMap())){
				/* We get a shallow copy of the ResultColumnList and its 
				 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
				 */
                prRCList=resultColumns;
                setResultColumns(resultColumns.copyListAndObjects());

				/* Replace ResultColumn.expression with new VirtualColumnNodes
				 * in the NormalizeResultSetNode's ResultColumnList.  (VirtualColumnNodes include
				 * pointers to source ResultSetNode, this, and source ResultColumn.)
				 */
                prRCList.genVirtualColumnNodes(this,resultColumns);

				/* Finally, we create the new MaterializeResultSetNode */
                mrsn=(MaterializeResultSetNode)getNodeFactory().getNode(
                        C_NodeTypes.MATERIALIZE_RESULT_SET_NODE,
                        this,
                        prRCList,
                        tableProperties,
                        getContextManager());
                // Propagate the referenced table map if it's already been created
                if(referencedTableMap!=null){
                    mrsn.setReferencedTableMap((JBitSet)referencedTableMap.clone());
                }
                return mrsn;
            }else{
				/* We get a shallow copy of the ResultColumnList and its 
				 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
				 */
                prRCList=childResult.getResultColumns();
                childResult.setResultColumns(prRCList.copyListAndObjects());

				/* Replace ResultColumn.expression with new VirtualColumnNodes
				 * in the MaterializeResultSetNode's ResultColumnList.  (VirtualColumnNodes include
				 * pointers to source ResultSetNode, this, and source ResultColumn.)
				 */
                prRCList.genVirtualColumnNodes(childResult,childResult.getResultColumns());

				/* RESOLVE - we need to push single table predicates down so that
				 * they get applied while building the MaterializeResultSet.
				 */

				/* Finally, we create the new MaterializeResultSetNode */
                mrsn=(MaterializeResultSetNode)getNodeFactory().getNode(
                        C_NodeTypes.MATERIALIZE_RESULT_SET_NODE,
                        childResult,
                        prRCList,
                        tableProperties,
                        getContextManager());
                // Propagate the referenced table map if it's already been created
                if(childResult.getReferencedTableMap()!=null){
                    mrsn.setReferencedTableMap((JBitSet)childResult.getReferencedTableMap().clone());
                }
                childResult=mrsn;
            }
        }

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
     * Get the lock mode for the target of an update statement
     * (a delete or update).  The update mode will always be row for
     * CurrentOfNodes.  It will be table if there is no where clause.
     *
     * @return The lock mode
     */
    @Override
    public int updateTargetLockMode(){
        if(restriction!=null || constantRestriction!=null){
            return TransactionController.MODE_RECORD;
        }else{
            return childResult.updateTargetLockMode();
        }
    }

    /**
     * Is it possible to do a distinct scan on this ResultSet tree.
     * (See SelectNode for the criteria.)
     *
     * @param distinctColumns the set of distinct columns
     * @return Whether or not it is possible to do a distinct scan on this ResultSet tree.
     */
    @Override
    boolean isPossibleDistinctScan(Set<BaseColumnNode> distinctColumns){
        if(restriction!=null || (restrictionList!=null && !restrictionList.isEmpty())){
            return false;
        }

        Set<BaseColumnNode> columns=new HashSet<>();
        for(int i=0;i<resultColumns.size();i++){
            ResultColumn rc=resultColumns.elementAt(i);
            BaseColumnNode bc=rc.getBaseColumnNode();
            if(bc==null) return false;
            columns.add(bc);
        }

        return columns.equals(distinctColumns) && childResult.isPossibleDistinctScan(distinctColumns);
    }

    /**
     * Mark the underlying scan as a distinct scan.
     */
    @Override
    void markForDistinctScan() throws StandardException {
        childResult.markForDistinctScan();
    }


    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException{
        super.acceptChildren(v);
        if(restriction!=null){
            restriction=(ValueNode)restriction.accept(v, this);
        }
        if(restrictionList!=null){
            restrictionList=(PredicateList)restrictionList.accept(v, this);
        }
    }


    /**
     * set the Information gathered from the parent table that is
     * required to peform a referential action on dependent table.
     */
    @Override
    public void setRefActionInfo(long fkIndexConglomId,int[] fkColArray,String parentResultSetId,boolean dependentScan){
        childResult.setRefActionInfo(fkIndexConglomId,fkColArray,parentResultSetId,dependentScan);
    }

    public void setRestriction(ValueNode restriction){
        this.restriction=restriction;
    }

    /**
     * Push the order by list down from InsertNode into its child result set so
     * that the optimizer has all of the information that it needs to consider
     * sort avoidance.
     *
     * @param orderByList The order by list
     */
    @Override
    void pushOrderByList(OrderByList orderByList){
        childResult.pushOrderByList(orderByList);
    }

    /**
     * Push down the offset and fetch first parameters, if any, to the
     * underlying child result set.
     *
     * @param offset             the OFFSET, if any
     * @param fetchFirst         the OFFSET FIRST, if any
     * @param hasJDBClimitClause true if the clauses were added by (and have the semantics of) a JDBC limit clause
     */
    @Override
    void pushOffsetFetchFirst(ValueNode offset,ValueNode fetchFirst,boolean hasJDBClimitClause){
        childResult.pushOffsetFetchFirst(offset,fetchFirst,hasJDBClimitClause);
    }

    @Override
    public ConstantAction makeConstantAction() throws StandardException{
        return childResult.makeConstantAction();
    }

    @Override
    public void assignResultSetNumber() throws StandardException{
        super.assignResultSetNumber();

		/* Set the point of attachment in all subqueries attached
		 * to this node.
		 */
        if(projectSubquerys!=null && !projectSubquerys.isEmpty()){
            projectSubquerys.setPointOfAttachment(resultSetNumber);
        }
        if(restrictSubquerys!=null && !restrictSubquerys.isEmpty()){
            restrictSubquerys.setPointOfAttachment(resultSetNumber);
        }
    }

    @Override
    public String printExplainInformation(String attrDelim, int order) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb.append(spaceToLevel())
                .append("ProjectRestrict").append("(")
                .append("n=").append(order)
                .append(attrDelim);

        if (childResult instanceof FromBaseTable || childResult instanceof FromVTI || childResult instanceof IndexToBaseRowNode) {
            sb.append(getFinalCostEstimate(false).prettyProjectionString(attrDelim));
        } else
            sb.append(getFinalCostEstimate(false).prettyProcessingString(attrDelim));

        List<String> qualifiers =  Lists.transform(PredicateUtils.PLtoList(RSUtils.getPreds(this)), PredicateUtils.predToString);
        if(qualifiers!=null && !qualifiers.isEmpty()) //add
            sb.append(attrDelim).append("preds=[").append(Joiner.on(",").skipNulls().join(qualifiers)).append("]");
        sb.append(")");
        return sb.toString();
    }
    @Override
    public void buildTree(Collection<QueryTreeNode> tree, int depth) throws StandardException {
        if (!nopProjectRestrict()) {
            setDepth(depth);
            tree.add(this);
            // look for subqueries in projection and restrictions, print if any
            for (SubqueryNode sub: RSUtils.collectExpressionNodes(this, SubqueryNode.class))
                sub.buildTree(tree,depth+1);

            childResult.buildTree(tree, depth+1);
        }
        else {
            childResult.buildTree(tree, depth);
        }
    }

    public boolean isUnsatisfiable() {
        if (sat == Satisfiability.UNSAT)
            return true;

        if (sat != Satisfiability.UNKNOWN)
            return false;

        if (childResult.isNotExists())
            return false;

        if (restrictionList != null && restrictionList.isUnsatisfiable()) {
            sat = Satisfiability.UNSAT;
            return true;
        }

        if (childResult.isUnsatisfiable()) {
            sat = Satisfiability.UNSAT;
            return true;
        }

        sat = Satisfiability.NEITHER;
        return false;
    }

    public boolean hasSubqueries() {
        return (projectSubquerys != null && !projectSubquerys.isEmpty()) ||
                (restrictSubquerys != null && !restrictSubquerys.isEmpty());
    }

    @Override
    public String toHTMLString() {
        return "" +
                "resultSetNumber: " + getResultSetNumber() + "<br/>" +
                "level: " + getLevel() + "<br/>" +
                "correlationName: " + getCorrelationName() + "<br/>" +
                "corrTableName: " + Objects.toString(corrTableName) + "<br/>" +
                "tableNumber: " + getTableNumber() + "<br/>" +
                "existsTable: " + existsTable + "<br/>" +
                "dependencyMap: " + Objects.toString(dependencyMap) +
                super.toHTMLString();
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
    public boolean isOneRowResultSet() throws StandardException{
        if (matchRowId) {
            return false;
        }
        if(existsTable ){
            return true;
        }
        return childResult.isOneRowResultSet();
    }

}
