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

import com.splicemachine.db.catalog.types.DefaultInfoImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.util.StringUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.*;

/**
 * A FromTable represents a table in the FROM clause of a DML statement.
 * It can be either a base table, a subquery or a project restrict.
 *
 * @see FromBaseTable
 * @see FromSubquery
 * @see ProjectRestrictNode
 */
public abstract class FromTable extends ResultSetNode implements Optimizable{
    Properties tableProperties;
    String correlationName;
    TableName corrTableName;
    int tableNumber;
    /* (Query block) level is 0-based. */
    /* RESOLVE - View resolution will have to update the level within
     * the view tree.
     */
    int level;
    // hashKeyColumns are 0-based column #s within the row returned by the store for hash scans
    int[] hashKeyColumns;

    AccessPath currentAccessPath;
    AccessPath bestAccessPath;
    AccessPath bestSortAvoidancePath;
    AccessPath trulyTheBestAccessPath;

    private int joinStrategyNumber;

    protected String userSpecifiedJoinStrategy;

    protected DataSetProcessorType dataSetProcessorType = DataSetProcessorType.DEFAULT_CONTROL;

    protected CostEstimate bestCostEstimate;

    // running sum of best cost for the current join path
    protected CostEstimate accumulatedCost;
    protected CostEstimate accumulatedCostForSortAvoidancePlan;


    private boolean considerSortAvoidancePath;

    /**
     * Set of object->trulyTheBestAccessPath mappings used to keep track
     * of which of this Optimizable's "trulyTheBestAccessPath" was the best
     * with respect to a specific outer query or ancestor node.  In the case
     * of an outer query, the object key will be an instance of OptimizerImpl.
     * In the case of an ancestor node, the object key will be that node itself.
     * Each ancestor node or outer query could potentially have a different
     * idea of what this Optimizable's "best access path" is, so we have to
     * keep track of them all.
     */
    private Map<Object,AccessPath> bestPlanMap;

    /**
     * Operations that can be performed on bestPlanMap.
     */
    public static final short REMOVE_PLAN=0;
    public static final short ADD_PLAN=1;
    public static final short LOAD_PLAN=2;

    /**
     * the original unbound table name
     */
    protected TableName origTableName;

    /* semi-join related variables */
    public boolean existsTable;
    public boolean isNotExists;
    public boolean matchRowId;
    public JBitSet dependencyMap;

    /**
     * SSQ join related variables
     */
    public boolean fromSSQ;

    /* variable tracking the info of a FromTable node flattened from a HalfJoinNode.
     * if outerJoinLevel = 0, the table is free to join with other tables in the FromList, if outerJoinLevel > 0, it can only
     * join with its left table indicated in the dependencyMap */
    protected int outerJoinLevel;
    PredicateList postJoinPredicates;

    protected boolean hasJoinPredicatePushedDownFromOuter = false;
    /**
     * Initializer for a table in a FROM list.
     *
     * @param correlationName The correlation name
     * @param tableProperties Properties list associated with the table
     */
    @Override
    public void init(Object correlationName,Object tableProperties){
        this.correlationName=(String)correlationName;
        this.tableProperties=(Properties)tableProperties;
        tableNumber=-1;
        bestPlanMap=null;
        outerJoinLevel = 0;
    }

    /**
     * Get this table's correlation name, if any.
     */
    public String getCorrelationName(){
        return correlationName;
    }

    /*
     *  Optimizable interface
     */

    @Override
    public CostEstimate optimizeIt(Optimizer optimizer,
                                   OptimizablePredicateList predList,
                                   CostEstimate outerCost,
                                   RowOrdering rowOrdering) throws StandardException{
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

        CostEstimate singleScanCost=estimateCost(predList, null, outerCost, optimizer, rowOrdering);

        /* Make sure there is a cost estimate to set */
        getCostEstimate(optimizer);

        setCostEstimate(singleScanCost);

        /* Optimize any subqueries that need to get optimized and
         * are not optimized any where else.  (Like those
         * in a RowResultSetNode.)
         */
        optimizeSubqueries(getDataDictionary(),costEstimate.rowCount(), optimizer.isForSpark());

        /*
        ** Get the cost of this result set in the context of the whole plan.
        */
        getCurrentAccessPath().getJoinStrategy().estimateCost(
                this, predList,null, outerCost, optimizer, getCostEstimate());

        optimizer.considerCost(this,predList,getCostEstimate(),outerCost);

        return getCostEstimate();
    }

    @Override
    public boolean nextAccessPath(Optimizer optimizer,
                                  OptimizablePredicateList predList,
                                  RowOrdering rowOrdering) throws StandardException{
        int numStrat=optimizer.getNumberOfJoinStrategies();
        boolean found=false;
        AccessPath ap=getCurrentAccessPath();

        /*
        ** Most Optimizables have no ordering, so tell the rowOrdering that
        ** this Optimizable is unordered, if appropriate.
        */
        if(userSpecifiedJoinStrategy!=null){
            /*
            ** User specified a join strategy, so we should look at only one
            ** strategy.  If there is a current strategy, we have already
            ** looked at the strategy, so go back to null.
            */
            if(ap.getJoinStrategy()!=null){
                ap.setJoinStrategy(null);

                found=false;
            }else{
                ap.setJoinStrategy(optimizer.getJoinStrategy(userSpecifiedJoinStrategy));
                ap.setHintedJoinStrategy(true);

                if(ap.getJoinStrategy()==null){
                    throw StandardException.newException(SQLState.LANG_INVALID_JOIN_STRATEGY,
                            userSpecifiedJoinStrategy,getBaseTableName(),
                            StringUtil.stringJoin(optimizer.getJoinStrategyNames(),", "));
                }

                found=true;
            }
        }else if(joinStrategyNumber<numStrat){
            ap.setHintedJoinStrategy(false);
            /* Step through the join strategies. */
            ap.setJoinStrategy(optimizer.getJoinStrategy(joinStrategyNumber));

            joinStrategyNumber++;

            found=true;

            optimizer.tracer().trace(OptimizerFlag.CONSIDERING_JOIN_STRATEGY,tableNumber,0,0.0,ap.getJoinStrategy(),
                                     correlationName);
        }
        ap.setMissingHashKeyOK(false);

        // Tell the RowOrdering about columns that are equal to constant
        // expressions.
        tellRowOrderingAboutConstantColumns(rowOrdering, predList);

        return found;
    }

    /**
     * Most Optimizables cannot be ordered
     */
    protected boolean canBeOrdered(){
        return false;
    }

    @Override
    public AccessPath getCurrentAccessPath(){
        return currentAccessPath;
    }

    @Override
    public AccessPath getBestAccessPath(){
        return bestAccessPath;
    }

    @Override
    public AccessPath getBestSortAvoidancePath(){
        return bestSortAvoidancePath;
    }

    @Override
    public AccessPath getTrulyTheBestAccessPath(){
        return trulyTheBestAccessPath;
    }

    @Override
    public void rememberSortAvoidancePath(){
        considerSortAvoidancePath=true;
    }

    @Override
    public boolean considerSortAvoidancePath(){
        return considerSortAvoidancePath;
    }

    public static boolean isNonCoveringIndex(Optimizable innerTable) {
        try {
            AccessPath path = innerTable.getCurrentAccessPath();
            if (path != null) {
                ConglomerateDescriptor cd = path.getConglomerateDescriptor();
                return (cd != null && cd.isIndex() && !innerTable.isCoveringIndex(cd));
            }
        } catch (Exception e) {
            throw new IllegalStateException("could not determine if index is covering", e);
        }
        return false;
    }

    @Override
    public void rememberJoinStrategyAsBest(AccessPath ap){
        Optimizer optimizer=ap.getOptimizer();

        AccessPath currentAccessPath=getCurrentAccessPath();
        JoinStrategy joinStrategy=currentAccessPath.getJoinStrategy();
        ap.setJoinStrategy(joinStrategy);
        ap.setHintedJoinStrategy(currentAccessPath.isHintedJoinStrategy());
        if (joinStrategy.getJoinStrategyType() == JoinStrategy.JoinStrategyType.BROADCAST)
            ap.setMissingHashKeyOK(currentAccessPath.isMissingHashKeyOK());
        else
            ap.setMissingHashKeyOK(false);

        OptimizerTrace tracer=optimizer.tracer();
        tracer.trace(OptimizerFlag.REMEMBERING_JOIN_STRATEGY,tableNumber,0,0.0,joinStrategy);

        if(ap==bestAccessPath){
            tracer.trace(OptimizerFlag.REMEMBERING_BEST_ACCESS_PATH_SUBSTRING,tableNumber,0,0.0,ap);
        }else if(ap==bestSortAvoidancePath){
            tracer.trace(OptimizerFlag.REMEMBERING_BEST_SORT_AVOIDANCE_ACCESS_PATH_SUBSTRING,tableNumber,0,0.0,ap);
        }else{
            tracer.trace(OptimizerFlag.REMEMBERING_BEST_UNKNOWN_ACCESS_PATH_SUBSTRING,tableNumber,0,0.0,ap);
        }
    }

    @Override
    public TableDescriptor getTableDescriptor(){
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT( "getTableDescriptor() not expected to be called for " +getClass().toString());
        }

        return null;
    }

    @Override
    public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate) throws StandardException{
        return false;
    }

    @Override
    public void pullOptPredicates(OptimizablePredicateList optimizablePredicates) throws StandardException{
        /* For most types of Optimizable, do nothing */
    }

    @Override
    public void pullOptPostJoinPredicates(OptimizablePredicateList optimizablePredicates) throws StandardException{
		if (postJoinPredicates != null) {
            for(int i=postJoinPredicates.size()-1;i>=0;i--){
                OptimizablePredicate optPred= postJoinPredicates.getOptPredicate(i);
                optimizablePredicates.addOptPredicate(optPred);
                postJoinPredicates.removeOptPredicate(i);
            }
        }
    }

    @Override
    public Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException{
        /* For most types of Optimizable, do nothing */
        return this;
    }

    @Override
    public boolean isCoveringIndex(ConglomerateDescriptor cd) throws StandardException{
        return false;
    }

    @Override
    public Properties getProperties(){
        return tableProperties;
    }

    @Override
    public void setProperties(Properties tableProperties){
        this.tableProperties=tableProperties;
    }

    @Override
    public void verifyProperties(DataDictionary dDictionary) throws StandardException{
        if(tableProperties==null){
            return;
        }
        /* Check here for:
         *        invalid properties key
         *        invalid joinStrategy
         *        invalid value for hashInitialCapacity
         *        invalid value for hashLoadFactor
         *        invalid value for hashMaxCapacity
         */
        Enumeration e=tableProperties.keys();
        while(e.hasMoreElements()){
            String key=(String)e.nextElement();
            String value=(String)tableProperties.get(key);

            switch(key){
                case "joinStrategy":
                    userSpecifiedJoinStrategy=StringUtil.SQLToUpperCase(value);
                    if (userSpecifiedJoinStrategy.equals("CROSS")) {
                        dataSetProcessorType = dataSetProcessorType.combine(DataSetProcessorType.FORCED_SPARK);
                    }
                    break;
                case "broadcastCrossRight": {
                    // no op since parseBoolean never throw
                    break;
                }
                case "useSpark":
                case "useOLAP":
                    dataSetProcessorType = dataSetProcessorType.combine(
                            Boolean.parseBoolean(StringUtil.SQLToUpperCase(value))?
                                    DataSetProcessorType.QUERY_HINTED_SPARK:
                                    DataSetProcessorType.QUERY_HINTED_CONTROL);
                    break;
                default:
                    // No other "legal" values at this time
                    throw StandardException.newException(SQLState.LANG_INVALID_FROM_TABLE_PROPERTY,key, "joinStrategy");
            }
        }
    }

    @Override
    public String getName() throws StandardException{
        return getExposedName();
    }

    @Override
    public String getBaseTableName(){
        return "";
    }

    @Override
    public int convertAbsoluteToRelativeColumnPosition(int absolutePosition){
        return absolutePosition;
    }

    @Override
    public void updateBestPlanMap(short action, Object planKey) throws StandardException{
        if(action==REMOVE_PLAN){
            if(bestPlanMap!=null){
                bestPlanMap.remove(planKey);
                if(bestPlanMap.isEmpty())
                    bestPlanMap=null;
            }

            return;
        }

        AccessPath bestPath=getTrulyTheBestAccessPath();
        AccessPath ap=null;
        if(action==ADD_PLAN){
            // If we get to this method before ever optimizing this node, then
            // there will be no best path--so there's nothing to do.
            if(bestPath==null)
                return;

            // If the bestPlanMap already exists, search for an
            // AccessPath for the received key and use that if we can.
            if(bestPlanMap==null)
                bestPlanMap=new IdentityHashMap<>();
            else
                ap=bestPlanMap.get(planKey);

            // If we don't already have an AccessPath for the key,
            // create a new one.  If the key is an OptimizerImpl then
            // we might as well pass it in to the AccessPath constructor;
            // otherwise just pass null.
            if(ap==null){
                if(planKey instanceof Optimizer)
                    ap=new AccessPathImpl((Optimizer)planKey);
                else
                    ap=new AccessPathImpl(null);
            }

            ap.copy(bestPath);
            //noinspection unchecked
            bestPlanMap.put(planKey,ap);
            return;
        }

        // If we get here, we want to load the best plan from our map
        // into this Optimizable's trulyTheBestAccessPath field.

        // If we don't have any plans saved, then there's nothing to load.
        // This can happen if the key is an OptimizerImpl that tried some
        // join order for which there was no valid plan.
        if(bestPlanMap==null)
            return;

        ap=bestPlanMap.get(planKey);

        // It might be the case that there is no plan stored for
        // the key, in which case there's nothing to load.
        if((ap==null) || (ap.getCostEstimate()==null))
            return;

        // We found a best plan in our map, so load it into this Optimizable's
        // trulyTheBestAccessPath field.
        bestPath.copy(ap);
    }

    @Override
    public boolean bestPathPicksSortMergeJoin(int planType) {
        AccessPath bestPath = null;
        switch(planType){
            case Optimizer.NORMAL_PLAN:
                bestPath=getBestAccessPath();
                break;
            case Optimizer.SORT_AVOIDANCE_PLAN:
                bestPath=getBestSortAvoidancePath();
                break;
            default:
                if(SanityManager.DEBUG){
                    SanityManager.THROWASSERT( "Invalid plan type "+planType);
                }
        }
        if (bestPath != null &&
                bestPath.getJoinStrategy() != null &&
                bestPath.getJoinStrategy().getJoinStrategyType().equals(JoinStrategy.JoinStrategyType.MERGE_SORT))
            return true;

        return false;
    }

    @Override
    public void rememberAsBest(int planType,Optimizer optimizer) throws StandardException{
        AccessPath bestPath=null;

        switch(planType){
            case Optimizer.NORMAL_PLAN:
                bestPath=getBestAccessPath();
                break;
            case Optimizer.SORT_AVOIDANCE_PLAN:
                bestPath=getBestSortAvoidancePath();
                break;
            default:
                if(SanityManager.DEBUG){
                    SanityManager.THROWASSERT( "Invalid plan type "+planType);
                }
        }

        getTrulyTheBestAccessPath().copy(bestPath);

        // Since we just set trulyTheBestAccessPath for the current
        // join order of the received optimizer, take note of what
        // that path is, in case we need to "revert" back to this
        // path later.  See Optimizable.updateBestPlanMap().
        // Note: Since this call descends all the way down to base
        // tables, it can be relatively expensive when we have deeply
        // nested subqueries.  So in an attempt to save some work, we
        // skip the call if this node is a ProjectRestrictNode whose
        // child is an Optimizable--in that case the ProjectRestrictNode
        // will in turn call "rememberAsBest" on its child and so
        // the required call to updateBestPlanMap() will be
        // made at that time.  If we did it here, too, then we would
        // just end up duplicating the work.
        if(!(this instanceof ProjectRestrictNode))
            updateBestPlanMap(ADD_PLAN,optimizer);
        else{
            ProjectRestrictNode prn=(ProjectRestrictNode)this;
            if(!(prn.getChildResult() instanceof Optimizable))
                updateBestPlanMap(ADD_PLAN,optimizer);
        }

        /* also store the name of the access path; i.e index name/constraint
         * name if we're using an index to access the base table.
         */

        if(isBaseTable()){
            DataDictionary dd=getDataDictionary();
            TableDescriptor td=getTableDescriptor();
            getTrulyTheBestAccessPath().initializeAccessPathName(dd,td);
        }

        assert bestPath!=null;
        setCostEstimate(bestPath.getCostEstimate());

        bestPath.getOptimizer().tracer().trace(OptimizerFlag.REMEMBERING_BEST_ACCESS_PATH, tableNumber, planType, 0.0,
                                               bestPath, correlationName);
    }

    @Override
    public void startOptimizing(Optimizer optimizer,RowOrdering rowOrdering){
        resetJoinStrategies();

        considerSortAvoidancePath=false;

        /*
        ** If there are costs associated with the best and sort access
        ** paths, set them to their maximum values, so that any legitimate
        ** access path will look cheaper.
        */
        CostEstimate ce=getBestAccessPath().getCostEstimate();

        if(ce!=null)
            ce.setCost(Double.MAX_VALUE,Double.MAX_VALUE,Double.MAX_VALUE);

        ce=getBestSortAvoidancePath().getCostEstimate();

        if(ce!=null)
            ce.setCost(Double.MAX_VALUE,Double.MAX_VALUE,Double.MAX_VALUE);

        if(!canBeOrdered())
            rowOrdering.addUnorderedOptimizable(this);
    }

    /**
     * This method is called when this table is placed in a potential
     * join order, or when a new conglomerate is being considered.
     * Set this join strategy number to 0 to indicate that
     * no join strategy has been considered for this table yet.
     */
    protected void resetJoinStrategies(){
        joinStrategyNumber=0;
        AccessPath currentAccessPath=getCurrentAccessPath();
        currentAccessPath.setJoinStrategy(null);
        currentAccessPath.setHintedJoinStrategy(false);
        currentAccessPath.setMissingHashKeyOK(false);
    }

    @Override
    public CostEstimate estimateCost(OptimizablePredicateList predList,
                                     ConglomerateDescriptor cd,
                                     CostEstimate outerCost,
                                     Optimizer optimizer,
                                     RowOrdering rowOrdering) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("estimateCost() not expected to be called for "+ getClass().toString());
        }

        return null;
    }

    /**
     * Get the final CostEstimate for this FromTable.
     *
     * @return The final CostEstimate for this FromTable, which is
     * the costEstimate of trulyTheBestAccessPath if there is one.
     * If there's no trulyTheBestAccessPath for this node, then
     * we just return the value stored in costEstimate as a default.
     */
    @Override
    public CostEstimate getFinalCostEstimate(boolean useSelf) throws StandardException{
        // If we already found it, just return it.
        if(finalCostEstimate!=null)
            return finalCostEstimate;

        if(getTrulyTheBestAccessPath()==null)
            finalCostEstimate=costEstimate;
        else
            finalCostEstimate=getTrulyTheBestAccessPath().getCostEstimate();

        return finalCostEstimate;
    }

    @Override
    public boolean isBaseTable(){
        return false;
    }

    /**
     * Check if any columns containing large objects (BLOBs or CLOBs) are
     * referenced in this table.
     *
     * @return {@code true} if at least one large object column is referenced,
     * {@code false} otherwise
     */
    @Override
    public boolean hasLargeObjectColumns(){
        for(int i=0;i<resultColumns.size();i++){
            ResultColumn rc=resultColumns.elementAt(i);
            if(rc.isReferenced()){
                DataTypeDescriptor type=rc.getType();
                if(type!=null && type.getTypeId().isLOBTypeId()){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     *
     * Always meterializable
     *
     * @return
     * @throws StandardException
     */
    @Override

    public boolean isMaterializable() throws StandardException{
        return true;
    }

    @Override
    public boolean supportsMultipleInstantiations(){
        return true;
    }

    @Override
    public int getTableNumber(){
        return tableNumber;
    }

    @Override
    public boolean hasTableNumber(){
        return tableNumber>=0;
    }

    @Override
    public boolean forUpdate(){
        return false;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    @Override
    public int[] hashKeyColumns(){
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(hashKeyColumns!=null,
                    "hashKeyColumns expected to be non-null");
        }

        return hashKeyColumns;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    @Override
    public void setHashKeyColumns(int[] columnNumbers){
        hashKeyColumns=columnNumbers;
    }

    @Override
    public boolean feasibleJoinStrategy(OptimizablePredicateList predList,Optimizer optimizer,CostEstimate outerCost) throws StandardException{
        AccessPath currentAccessPath=getCurrentAccessPath();
        return currentAccessPath.getJoinStrategy().feasible(this,predList,optimizer,outerCost,currentAccessPath.isHintedJoinStrategy(), false);
    }

    /**
     * No-op in FromTable.
     *
     * @see HalfOuterJoinNode#isJoinColumnForRightOuterJoin
     */
    public void isJoinColumnForRightOuterJoin(ResultColumn rc){ }

    @Override
    public boolean legalJoinOrder(JBitSet assignedTableMap){
        // Only those subclasses with dependencies need to override this.
        return true;
    }

    @Override
    public int getNumColumnsReturned(){
        return resultColumns.size();
    }

    @Override
    public boolean isTargetTable(){
        return false;
    }

    @Override
    public boolean isOneRowScan() throws StandardException{
        /* We simply return isOneRowResultSet() for all
         * subclasses except for EXISTS FBT where
         * the semantics differ between 1 row per probe
         * and whether or not there can be more than 1
         * rows that qualify on a scan.
         */
        return isOneRowResultSet();
    }

    @Override
    public void initAccessPaths(Optimizer optimizer){
        if(currentAccessPath==null){
            currentAccessPath=optimizer.newAccessPath();
        }
        if(bestAccessPath==null){
            bestAccessPath=optimizer.newAccessPath();
        }
        if(bestSortAvoidancePath==null){
            bestSortAvoidancePath=optimizer.newAccessPath();
        }
        if(trulyTheBestAccessPath==null){
            trulyTheBestAccessPath=optimizer.newAccessPath();
        }
    }

    @Override
    public void resetAccessPaths() {
        currentAccessPath = null;
        bestAccessPath = null;
        bestSortAvoidancePath = null;
        trulyTheBestAccessPath = null;
    }

    @Override
    public double uniqueJoin(OptimizablePredicateList predList) throws StandardException{
        return -1d;
    }

    /**
     * Return the user specified join strategy, if any for this table.
     *
     * @return The user specified join strategy, if any for this table.
     */
    String getUserSpecifiedJoinStrategy(){
        if(tableProperties==null){
            return null;
        }

        return tableProperties.getProperty("joinStrategy");
    }

    /**
     * Is this a table that has a FOR UPDATE
     * clause.  Overridden by FromBaseTable.
     *
     * @return true/false
     */
    protected boolean cursorTargetTable(){
        return false;
    }

    protected CostEstimate getCostEstimate(Optimizer optimizer){
        if(costEstimate==null){
            costEstimate=optimizer.newCostEstimate();
        }
        return costEstimate;
    }

    /*
    ** This gets a cost estimate for doing scratch calculations.  Typically,
    ** it will hold the estimated cost of a conglomerate.  If the optimizer
    ** decides the scratch cost is lower than the best cost estimate so far,
    ** it will copy the scratch cost to the non-scratch cost estimate,
    ** which is allocated above.
    */
    protected CostEstimate getScratchCostEstimate(Optimizer optimizer){
        if(scratchCostEstimate==null){
            scratchCostEstimate=optimizer.newCostEstimate();
        }
        scratchCostEstimate.setRowOrdering(null);
        scratchCostEstimate.setBase(null);

        return scratchCostEstimate;
    }

    /**
     * Set the cost estimate in this node to the given cost estimate.
     */
    protected void setCostEstimate(CostEstimate newCostEstimate){
        costEstimate=getCostEstimate();

        costEstimate.setCost(newCostEstimate);
    }

    /**
     * Assign the cost estimate in this node to the given cost estimate.
     */
    protected void assignCostEstimate(CostEstimate newCostEstimate) throws StandardException{
        costEstimate=newCostEstimate;
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
            return "correlation Name: "+correlationName+"\n"+
                    (corrTableName!=null? corrTableName.toString():"null")+"\n"+
                    "tableNumber "+tableNumber+"\n"+
                    "level "+level+"\n"+
                    super.toString();
        }else{
            return "";
        }
    }

    @Override
    public String toHTMLString() {
        return "" +
                "tableNumber: " + tableNumber + "<br/>" +
                "level: " + level + "<br/>" +
                "correlationName: " + correlationName + "<br/>" +
                "corrTableName: " + Objects.toString(corrTableName) + "<br/>" +
                super.toHTMLString();
    }

    /**
     * Return a ResultColumnList with all of the columns in this table.
     * (Used in expanding '*'s.)
     * NOTE: Since this method is for expanding a "*" in the SELECT list,
     * ResultColumn.expression will be a ColumnReference.
     *
     * @param allTableName The qualifier on the "*"
     * @return ResultColumnList    List of result columns from this table.
     * @throws StandardException Thrown on error
     */
    public ResultColumnList getResultColumnsForList(TableName allTableName,
                                                    ResultColumnList inputRcl,
                                                    TableName tableName) throws StandardException{
        ResultColumnList rcList;
        ResultColumn resultColumn;
        ValueNode valueNode;
        String columnName;
        TableName exposedName;
        TableName toCompare;

        /* If allTableName is non-null, then we must check to see if it matches
         * our exposed name.
         */

        if(correlationName==null)
            toCompare=tableName;
        else{
            if(allTableName!=null)
                toCompare=makeTableName(allTableName.getSchemaName(),correlationName);
            else
                toCompare=makeTableName(null,correlationName);
        }

        if(allTableName!=null && !allTableName.equals(toCompare)){
            return null;
        }

        /* Cache exposed name for this table.
         * The exposed name becomes the qualifier for each column
         * in the expanded list.
         */
        if(correlationName==null){
            exposedName=tableName;
        }else{
            exposedName=makeTableName(null,correlationName);
        }

        rcList=(ResultColumnList)getNodeFactory().getNode( C_NodeTypes.RESULT_COLUMN_LIST, getContextManager());

        /* Build a new result column list based off of resultColumns.
         * NOTE: This method will capture any column renaming due to
         * a derived column list.
         */
        int inputSize=inputRcl.size();
        for(int index=0;index<inputSize;index++){
            // Build a ResultColumn/ColumnReference pair for the column //
            columnName=inputRcl.elementAt(index).getName();
            valueNode=(ValueNode)getNodeFactory().getNode(
                    C_NodeTypes.COLUMN_REFERENCE,
                    columnName,
                    exposedName,
                    getContextManager());
            resultColumn=(ResultColumn)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    columnName,
                    valueNode,
                    getContextManager());

            // Build the ResultColumnList to return //
            rcList.addResultColumn(resultColumn);
        }
        return rcList;
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
    void pushExpressions(PredicateList predicateList) throws StandardException{
        assert predicateList!=null: "predicateList is expected to be non-null";
    }

    /**
     * Get the exposed name for this table, which is the name that can
     * be used to refer to it in the rest of the query.
     *
     * @throws StandardException Thrown on error
     * @return The exposed name of this table.
     */
    public String getExposedName() throws StandardException{
        if(SanityManager.DEBUG)
            SanityManager.THROWASSERT(
                    "getExposedName() not expected to be called for "+this.getClass().getName());
        return null;
    }

    /**
     * Set the table # for this table.
     *
     * @param tableNumber The table # for this table.
     */
    public void setTableNumber(int tableNumber){
        /* This should only be called if the tableNumber has not been set yet */
        assert this.tableNumber==-1: "tableNumber is not expected to be already set";
        this.tableNumber=tableNumber;
    }

    /**
     * Return a TableName node representing this FromTable.
     * Expect this to be overridden (and used) by subclasses
     * that may set correlationName to null.
     *
     * @return a TableName node representing this FromTable.
     * @throws StandardException Thrown on error
     */
    public TableName getTableName() throws StandardException{
        if(correlationName==null) return null;

        if(corrTableName==null){
            corrTableName=makeTableName(null,correlationName);
        }

        return corrTableName;
    }

    /**
     * Set the (query block) level (0-based) for this FromTable.
     *
     * @param level The query block level for this FromTable.
     */
    public void setLevel(int level){
        this.level=level;
    }

    /**
     * Get the (query block) level (0-based) for this FromTable.
     *
     * @return int    The query block level for this FromTable.
     */
    public int getLevel(){
        return level;
    }

    /**
     * Decrement (query block) level (0-based) for this FromTable.
     * This is useful when flattening a subquery.
     *
     * @param decrement The amount to decrement by.
     */
    @Override
    void decrementLevel(int decrement){
        assert level==0 || level>=decrement: "level ("+level+") expects to be >= decrement ("+decrement+")";
        /* NOTE: level doesn't get propagated
         * to nodes generated after binding.
         */
        if(level>0){
            level-=decrement;
        }
    }

    /**
     * Get a schema descriptor for the given table.
     * Uses this.corrTableName.
     *
     * @return Schema Descriptor
     * @exception StandardException    throws on schema name
     * that doesn't exist
     */
    public SchemaDescriptor getSchemaDescriptor() throws StandardException{
        return getSchemaDescriptor(corrTableName);
    }

    /**
     * Get a schema descriptor for the given table.
     *
     * @return Schema Descriptor
     * @param    tableName the table name
     * @exception StandardException    throws on schema name
     * that doesn't exist
     */
    public SchemaDescriptor getSchemaDescriptor(TableName tableName) throws StandardException{
        SchemaDescriptor sd;

        sd=getSchemaDescriptor(tableName.getSchemaName());

        return sd;
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
        // Only FromBaseTables have schema names
        if(schemaName!=null){
            return null;
        }

        if(getExposedName().equals(name)){
            return this;
        }
        return null;
    }

    /**
     * Is this FromTable a JoinNode which can be flattened into
     * the parents FromList.
     *
     * @return boolean        Whether or not this FromTable can be flattened.
     */
    public boolean isFlattenableJoinNode(){
        return false;
    }

    /**
     * no LOJ reordering for this FromTable.
     */
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
    public FromTable transformOuterJoins(ValueNode predicateTree,int numTables) throws StandardException{
        return this;
    }

    /**
     * Fill the referencedTableMap with this ResultSetNode.
     *
     * @param passedMap The table map to fill in.
     */
    @Override
    public void fillInReferencedTableMap(JBitSet passedMap){
        if(tableNumber!=-1){
            passedMap.set(tableNumber);
        }
    }

    /**
     * Mark as updatable all the columns in the result column list of this
     * FromBaseTable that match the columns in the given update column list.
     * If the list is null, it means all the columns are updatable.
     *
     * @param updateColumns A Vector representing the columns
     *                      that can be updated.
     */
    protected void markUpdatableByCursor(List<String> updateColumns){
        resultColumns.markUpdatableByCursor(updateColumns);
    }

    /**
     * Flatten this FromTable into the outer query block. The steps in
     * flattening are:
     * o  Mark all ResultColumns as redundant, so that they are "skipped over"
     * at generate().
     * o  Append the wherePredicates to the outer list.
     * o  Return the fromList so that the caller will merge the 2 lists
     *
     * @param rcl          The RCL from the outer query
     * @param outerPList   PredicateList to append wherePredicates to.
     * @param sql          The SubqueryList from the outer query
     * @param gbl          The group by list, if any
     * @param havingClause The HAVING clause, if any
     * @return FromList        The fromList from the underlying SelectNode.
     * @param numTables     maximum number of tables in the query
     * @throws StandardException Thrown on error
     */
    public FromList flatten(ResultColumnList rcl,
                            PredicateList outerPList,
                            SubqueryList sql,
                            GroupByList gbl,
                            ValueNode havingClause,
                            int numTables)  throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT( "flatten() not expected to be called for "+this);
        }
        return null;
    }

    /**
     * Optimize any subqueries that haven't been optimized any where
     * else.  This is useful for a RowResultSetNode as a derived table
     * because it doesn't get optimized otherwise.
     *
     * @throws StandardException Thrown on error
     */
    void optimizeSubqueries(DataDictionary dd,double rowCount, Boolean forSpark) throws StandardException{ }

    /**
     * Tell the given RowOrdering about any columns that are constant
     * due to their being equality comparisons with constant expressions.
     */
    protected void tellRowOrderingAboutConstantColumns(RowOrdering rowOrdering, OptimizablePredicateList predList){
    }

    public boolean needsSpecialRCLBinding(){
        return false;
    }

    /**
     * Sets the original or unbound table name for this FromTable.
     *
     * @param tableName the unbound table name
     */
    public void setOrigTableName(TableName tableName){
        this.origTableName=tableName;
    }

    /**
     * Gets the original or unbound table name for this FromTable.
     * The tableName field can be changed due to synonym resolution.
     * Use this method to retrieve the actual unbound tablename.
     *
     * @return TableName the original or unbound tablename
     */
    public TableName getOrigTableName(){
        return this.origTableName;
    }

    public ResultColumn getRowIdColumn(){
        return null;
    }

    public void setRowIdColumn(ResultColumn rc) {
    }

    public DataSetProcessorType getDataSetProcessorType() {
        return dataSetProcessorType;
    }

    public void setDataSetProcessorType(DataSetProcessorType dataSetProcessorType) {
        this.dataSetProcessorType = dataSetProcessorType;
    }

    @Override
    public double getMemoryUsage4BroadcastJoin(){
        if (trulyTheBestAccessPath == null || trulyTheBestAccessPath.getCostEstimate() == null)
            return 0.0d;

        if (trulyTheBestAccessPath.getJoinStrategy().getJoinStrategyType() == JoinStrategy.JoinStrategyType.BROADCAST)
            return trulyTheBestAccessPath.getCostEstimate().getBase().getEstimatedHeapSize();

        return 0.0d;
    }

    /**
     * Does this node represent an EXISTS table that requires semi-join
     * @return Whether or not this node represents
     * an EXISTS table
     */
    public boolean getExistsTable() {
        return existsTable;
    }

    public void setExistsTable(boolean existsTable, boolean isNotExists,boolean matchRowId) {
        this.existsTable=existsTable;
        this.isNotExists=isNotExists;
        this.matchRowId = matchRowId;
    }

    /**
     * Return whether the current table should be joined as the right of an inclusion join or exclusion join or regular inner join
     * @return 1: exclusion join
     *         -1: inclusion join
     *         0: regular inner join
     */
    public byte getSemiJoinType() {
        if (isNotExists)
            return 1;
        else if (!matchRowId && existsTable)
            return -1;
        else
            return 0;
    }

    @Override
    public boolean getFromSSQ() {
        return fromSSQ;
    }

    public void setFromSSQ(boolean set) {
        fromSSQ = set;
    }

    public void setDependencyMap(JBitSet set) {
        dependencyMap = set;
    }

    /* tableNumber is 0-based */
    public void addToDependencyMap(int tableNumber) {
        if (tableNumber < 0 || tableNumber >= getCompilerContext().getMaximalPossibleTableCount())
            return;

        if (dependencyMap == null) {
            dependencyMap = new JBitSet(getCompilerContext().getMaximalPossibleTableCount());
        }
        dependencyMap.set(tableNumber);
    }

    public void addToDependencyMap(JBitSet newSet) {
        if (newSet == null)
            return;

        if (dependencyMap == null) {
            dependencyMap = new JBitSet(getCompilerContext().getMaximalPossibleTableCount());
        }

        dependencyMap.or(newSet);
    }

    private FormatableBitSet buildDefaultRow(ResultColumnList defaultRow) throws StandardException {
        FormatableBitSet defaultValueMap = new FormatableBitSet(resultColumns.size());

        for (int i=0; i < resultColumns.size(); i++) {
            ResultColumn rc = resultColumns.elementAt(i);
            ValueNode defaultTree;
            ResultColumn newResultColumn;
            if (rc.getExpression() instanceof CurrentRowLocationNode) {
                //add a dummy value for the rowlocation reference, this value won't be used
                newResultColumn = (ResultColumn) getNodeFactory().getNode
                        (C_NodeTypes.RESULT_COLUMN, DataTypeDescriptor.INTEGER, getNullNode(DataTypeDescriptor.INTEGER), getContextManager());
            } else {
                ColumnDescriptor colDesc = rc.columnDescriptor;
                DataTypeDescriptor colType = colDesc.getType();


                // Check for defaults
                DefaultInfoImpl defaultInfo = (DefaultInfoImpl) colDesc.getDefaultInfo();

                /* When a column has a system variable like USER as the default value,
                   defaultInfo.defaultValue will be null. For such cases, we also need to physically store
                   the value as USER could vary depending on the USER used to log in. We don't need to
                   fetch default value for such cases.
                 */
                if (defaultInfo != null && defaultInfo.getDefaultValue() != null &&
                        !colDesc.isAutoincrement() && !colDesc.hasGenerationClause() && !colDesc.getType().isNullable()) {
                    DataValueDescriptor defaultValue = defaultInfo.getDefaultValue();
                    DataValueDescriptor newDefault = defaultValue.cloneValue(false);
                    // For Char type, the default value may not match the column type exactly.
                    // For example, column char(4) could have a default value 'a'. For such cases, what's
                    // stored in the table is really 'a   ', so we need a conversion here
                    if (newDefault instanceof SQLChar) {
                        newDefault.normalize(colDesc.getType(), newDefault);
                    }
                    defaultTree = getConstantNode(colType, newDefault);
                    defaultValueMap.set(i);
                } else {
                    defaultTree = getNullNode(colType);
                }

                newResultColumn = (ResultColumn) getNodeFactory().getNode
                        (C_NodeTypes.RESULT_COLUMN, defaultTree.getTypeServices(), defaultTree, getContextManager());
            }

            defaultRow.addResultColumn(newResultColumn);
        }

        return defaultValueMap;

    }

    public int generateDefaultRow(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException{
        if (!(this instanceof FromBaseTable) && !(this instanceof IndexToBaseRowNode)) {
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT("generateDefaultRow() not expected to be called for " + getClass().toString());
            }
            return 0;
        }

        if (resultColumns.indexRow) {
            mb.pushNull(ClassName.GeneratedMethod);
            mb.push(-1);
            return 2;
        }

        ResultColumnList defaultRow = (ResultColumnList)getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN_LIST,
                getContextManager());
        FormatableBitSet defaultValueMap = buildDefaultRow(defaultRow);
        defaultRow.generate(acb, mb);
        int defaultValueItem=acb.addItem(defaultValueMap);
        mb.push(defaultValueItem);
        return 2;
    }

    public void setAccumulatedCost(CostEstimate cost) {
        if (accumulatedCost == null)
            accumulatedCost = cost.cloneMe();
        else
            accumulatedCost.setCost(cost);
    }
    public void setAccumulatedCostForSortAvoidancePlan(CostEstimate cost) {
        if (accumulatedCostForSortAvoidancePlan == null)
            accumulatedCostForSortAvoidancePlan = cost.cloneMe();
        else
            accumulatedCostForSortAvoidancePlan.setCost(cost);
    }
    public CostEstimate getAccumulatedCost() {
        return accumulatedCost;
    }

    public CostEstimate getAccumulatedCostforSortAvoidancePlan() {
        return accumulatedCostForSortAvoidancePlan;
    }

    public int getOuterJoinLevel() {
        return outerJoinLevel;
    }

    public void setOuterJoinLevel(int level) {
        outerJoinLevel = level;
    }

    /**
     * Clear the bits from the dependency map when join nodes are flattened
     *
     * @param locations vector of bit numbers to be cleared
     */
    public void clearDependency(List<Integer> locations){
        if(this.dependencyMap!=null){
            for(Integer location : locations)
                this.dependencyMap.clear(location);
        }
    }

    public PredicateList getPostJoinPredicates() {
        return postJoinPredicates;
    }

    public void setPostJoinPredicates(PredicateList pList) {
        postJoinPredicates = pList;
    }

    @Override
    public boolean hasJoinPredicatePushedDownFromOuter() {
        return hasJoinPredicatePushedDownFromOuter;
    }

    @Override
    public void setHasJoinPredicatePushedDownFromOuter(boolean value) {
        hasJoinPredicatePushedDownFromOuter = value;
    }
}
