/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.compile;

import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.iapi.sql.compile.OptimizableList;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.OptimizerFlag;
import com.splicemachine.db.iapi.sql.compile.OptimizerTrace;
import com.splicemachine.db.iapi.sql.compile.RequiredRowOrdering;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.AggregateCostController;
import com.splicemachine.db.iapi.store.access.SortCostController;
import com.splicemachine.db.impl.sql.compile.AggregateNode;
import com.splicemachine.db.impl.sql.compile.GroupByList;
import com.splicemachine.db.impl.sql.compile.Level2OptimizerImpl;
import com.splicemachine.db.impl.sql.compile.Level2OptimizerTrace;
import com.splicemachine.db.impl.sql.compile.OrderByList;
import com.splicemachine.derby.impl.store.access.TempGroupedAggregateCostController;
import com.splicemachine.derby.impl.store.access.TempScalarAggregateCostController;
import com.splicemachine.derby.impl.store.access.TempSortController;

/**
 * This is the Level 2 Optimizer.
 *
 * {@link com.splicemachine.db.impl.sql.compile.OptimizerImpl} has a goofy
 * way of considering optimizer permutations. What it does is consider all possible
 * join orderings, and <em>independently for each table</em> considers the
 * cost of index selection. As a result, we can get weird behavior, like in the following
 * example:
 *
 * Let table {@code A} have 1M rows, and have a covering index called {@code AI},
 * and let {@code B} have 1M rows with a covering index called {@code BI}. Note that
 *  the index order is <em>on the join keys</em> for the query {@code A join B} (
 *  this is important because it allows the MERGE join strategy). Also add an additional
 *  limiting predicate on {@code A} which is on {@code A}'s join keys and which will
 *  constraint the search result. There are then four possible index choice permutations:
 *
 *  {@code AjB}
 *  {@code AjBI}
 *  {@code AIjB}
 *  {@code AIjBI}
 *
 *  and then separately there are all the possible join strategy choices that are available
 *  for each permutation choice. Because Merge join is possible, we get the list
 *
 *  <ul>
 *      <li>{@code A nlj B}</li>
 *      <li>{@code A br B}</li>
 *      <li>{@code A sm B}</li>
 *      <li>{@code AI nlj B}</li>
 *      <li>{@code AI br B}</li>
 *      <li>{@code AI sm B}</li>
 *      <li>{@code A nlj BI}</li>
 *      <li>{@code A br BI}</li>
 *      <li>{@code A sm BI}</li>
 *      <li>{@code AI nlj BI}</li>
 *      <li>{@code AI br BI}</li>
 *      <li>{@code AI sm BI}</li>
 *      <li>{@code AI m BI}</li>
 *      <li>{@code B nlj A}</li>
 *      <li>{@code B br A}</li>
 *      <li>{@code B sm A}</li>
 *      <li>{@code BI nlj A}</li>
 *      <li>{@code BI br A}</li>
 *      <li>{@code BI sm A}</li>
 *      <li>{@code B nlj AI}</li>
 *      <li>{@code B br AI}</li>
 *      <li>{@code B sm AI}</li>
 *      <li>{@code BI nlj AI}</li>
 *      <li>{@code BI br AI}</li>
 *      <li>{@code BI sm AI}</li>
 *      <li>{@code BI m AI}</li>
 *  </ul>
 *
 *  where {@code nlj=}<em>NestedLoopJoin</em>,{@code br=}<em>BroadcastJoin</em>,{@code sm=}<em>MergeSortJoin</em>,
 *  and {@code m=}<em>MergeJoin</em>. Because there is a join-order permutation, we end up with twice these combinations(
 *  we can reverse the order of {@code A} and {@code B} in the join--e.g. {@code B nlj A} is possible as well
 *  as {@code A nlj B}). This describes the total space of possible query plans for a simple two-table join.
 *
 *  So what does {@code OptimizerImpl} consider? First, it chooses a join order for the first table {@code A},
 *  then it chooses whether or not to use an index. Then, it chooses <em>both</em> a join <em>and</em> an index
 *  to use for {@code B}. As it turns out, the most optimal choice for scanning {@code A} <em>in isolation</em>
 *  is to use a full table scan. Because {@code A} is chosen in an unordered way, {@code B} is constrained to choose
 *  only un-ordered joins, making the cheapest cost {@code A br B}. However, because {@code A}'s index access was
 *  chosen <em>without considering {@code B} at all</em>, the total space explored by {@code OptimizerImpl} is actually
 *  just
 *
 *  <ul>
 *      <li>{@code A nlj B}</li>
 *      <li>{@code A br B}</li>
 *      <li>{@code A sm B}</li>
 *      <li>{@code B nlj A}</li>
 *      <li>{@code B br A}</li>
 *      <li>{@code B sm A}</li>
 *  </ul>
 *
 *  which has just 6 permutations instead of the total of 26 possible permutations. As a result, {@code OptimizerImpl}
 *  never even considers the globally optimal choice of {@code AI m BI}, even though that choice scores lower
 *  <em>and</em> performs better.
 *
 */
public class SpliceLevel2OptimizerImpl extends Level2OptimizerImpl{
    private static final Logger TRACE_LOGGER=Logger.getLogger("optimizer.trace");
    private final OptimizerTrace tracer = new Level2OptimizerTrace(null,this){
        @Override
        @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT",justification = "Intentional")
        public void trace(TraceLevel level, String traceString){

            Priority prio = Level.INFO;
            switch(level){
                case TRACE:
                    prio =  Level.TRACE;
                    break;
                case DEBUG:
                    prio =  Level.DEBUG;
                    break;
                case WARN:
                    prio =  Level.WARN;
                    break;
                case ERROR:
                    prio =  Level.ERROR;
                    break;
            }

            if (TRACE_LOGGER.isTraceEnabled()) {
                TRACE_LOGGER.log(prio,traceString);
            }
        }
    };

    private final long minTimeout;
    private final long maxTimeout;
    public SpliceLevel2OptimizerImpl(OptimizableList optimizableList,
                                     OptimizablePredicateList predicateList,
                                     DataDictionary dDictionary,
                                     boolean ruleBasedOptimization,
                                     boolean noTimeout,
                                     boolean useStatistics,
                                     int maxMemoryPerTable,
                                     JoinStrategy[] joinStrategies,
                                     int tableLockThreshold,
                                     RequiredRowOrdering requiredRowOrdering,
                                     int numTablesInQuery,
                                     LanguageConnectionContext lcc) throws StandardException{
        super(optimizableList,
                predicateList,
                dDictionary,
                ruleBasedOptimization,
                noTimeout,
                useStatistics,
                maxMemoryPerTable,
                joinStrategies,
                tableLockThreshold,
                requiredRowOrdering,
                numTablesInQuery,
                lcc);
        SConfiguration configuration=EngineDriver.driver().getConfiguration();
        this.minTimeout=configuration.getOptimizerPlanMinimumTimeout();
        this.maxTimeout=configuration.getOptimizerPlanMaximumTimeout();
        tracer().trace(OptimizerFlag.STARTED,0,0,0.0,null);
    }

    @Override
    public int getLevel(){
        return 2;
    }

    @Override
    public CostEstimate newCostEstimate(){
        return new SimpleCostEstimate();
    }

    @Override
    public CostEstimate getNewCostEstimate(double theCost,double theRowCount,double theSingleScanRowCount){
        return new SimpleCostEstimate(theCost,theRowCount,theSingleScanRowCount);
    }

    @Override
    public AggregateCostController newAggregateCostController(GroupByList groupingList,List<AggregateNode> aggregateVector){
        if(groupingList==null||groupingList.size()<=0) //we are a scalar aggregate
            return new TempScalarAggregateCostController(aggregateVector);
        else //we are a grouped aggregate
        return new TempGroupedAggregateCostController(groupingList);
    }

    @Override
    public SortCostController newSortCostController(OrderByList orderByList){
        return new TempSortController();
    }

    @Override
    @SuppressFBWarnings(value = "UR_UNINIT_READ_CALLED_FROM_SUPER_CONSTRUCTOR",justification = "Tracer actually is already initialized")
    public OptimizerTrace tracer(){
        if (TRACE_LOGGER.isTraceEnabled()) {
            optimizerTrace = true;
            return tracer;
        } else {
            optimizerTrace = false;
        }
        return super.tracer();
    }
    
    /**
     * Overridden to check splice configuration.
     */
    protected long getMinTimeout() {
        return minTimeout;
    }

    /**
     * Overridden to check splice configuration.
     */
    protected long getMaxTimeout() {
        return maxTimeout;
    }
}
