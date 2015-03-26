package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.AggregateCostController;
import com.splicemachine.db.impl.sql.compile.AggregateNode;
import com.splicemachine.db.impl.sql.compile.GroupByList;
import com.splicemachine.db.impl.sql.compile.Level2OptimizerImpl;
import com.splicemachine.derby.impl.stats.StatisticsStorage;
import com.splicemachine.derby.impl.store.access.TempGroupedAggregateCostController;
import com.splicemachine.derby.impl.store.access.TempScalarAggregateCostController;

import java.util.List;

/**
 * This is the Level 2 Optimizer.
 */
public class SpliceLevel2OptimizerImpl extends Level2OptimizerImpl{


    //    public SpliceLevel2OptimizerImpl(){
//
//    }

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
        //ensure that table statistics are properly running
        StatisticsStorage.ensureRunning(dDictionary);
    }

    /**
     * @see Optimizer#getLevel
     */
    @Override
    public int getLevel(){
        return 2;
    }

    /**
     * @see Optimizer#newCostEstimate
     */
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
            return new TempScalarAggregateCostController();
        else //we are a grouped aggregate
        return new TempGroupedAggregateCostController(groupingList);
    }
}