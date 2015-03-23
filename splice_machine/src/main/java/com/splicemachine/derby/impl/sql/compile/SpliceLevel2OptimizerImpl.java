package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.impl.sql.compile.Level2OptimizerImpl;
import com.splicemachine.derby.impl.stats.StatisticsStorage;

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

}