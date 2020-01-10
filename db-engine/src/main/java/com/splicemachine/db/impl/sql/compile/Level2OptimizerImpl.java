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
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.iapi.sql.compile.OptimizableList;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.OptimizerFlag;
import com.splicemachine.db.iapi.sql.compile.OptimizerTrace;
import com.splicemachine.db.iapi.sql.compile.RequiredRowOrdering;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;

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
        super(optimizableList, predicateList, dDictionary,
              ruleBasedOptimization, noTimeout, useStatistics, maxMemoryPerTable,
              joinStrategies, tableLockThreshold, requiredRowOrdering,
              numTablesInQuery);

        // Remember whether or not optimizer trace is on;
        optimizerTrace=lcc.getOptimizerTrace();
        optimizerTraceHtml=lcc.getOptimizerTraceHtml();
        this.lcc=lcc;

        // Optimization started
        if(optimizerTrace){
            // JC - lcc.getOptimizerTrace() does not recognize trace is set. This will never be called.
            // Even if it were called, the overridden tracer() method in subclass will NPE since it's ctor
            // has not finished and it's not fully init'd.
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
