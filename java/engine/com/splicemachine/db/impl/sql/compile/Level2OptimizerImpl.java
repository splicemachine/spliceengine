/*

   Derby - Class org.apache.derby.impl.sql.compile.Level2OptimizerImpl

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;

/**
 * This is the Level 2 Optimizer.
 */

public class Level2OptimizerImpl extends OptimizerImpl{
    private LanguageConnectionContext lcc;

    private OptimizerTrace tracer;

    public Level2OptimizerImpl(OptimizableList optimizableList,
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
                        LanguageConnectionContext lcc)
            throws StandardException{
        super(optimizableList,predicateList,dDictionary,
                ruleBasedOptimization,noTimeout,useStatistics,maxMemoryPerTable,
                joinStrategies,tableLockThreshold,requiredRowOrdering,
                numTablesInQuery);

        // Remember whether or not optimizer trace is on;
        optimizerTrace=lcc.getOptimizerTrace();
        optimizerTraceHtml=lcc.getOptimizerTraceHtml();
        this.lcc=lcc;

        // Optimization started
        if(optimizerTrace){
            tracer().trace(OptimizerFlag.STARTED,0,0,0.0,null);
        }
    }

    @Override
    public int getLevel(){
        return 2;
    }

    @Override
    public CostEstimate newCostEstimate(){
        return new Level2CostEstimateImpl();
    }

    @Override
    public CostEstimate getNewCostEstimate(double theCost,
                                               double theRowCount,
                                               double theSingleScanRowCount){
        return new Level2CostEstimateImpl(theCost,theRowCount,theSingleScanRowCount);
    }

    @Override
    public OptimizerTrace tracer(){
        if(tracer==null){
            if(optimizerTrace){
                tracer = new Level2OptimizerTrace(lcc,this);
            }else
                tracer = NoOpOptimizerTrace.INSTANCE;
        }
        return tracer;
    }

}
