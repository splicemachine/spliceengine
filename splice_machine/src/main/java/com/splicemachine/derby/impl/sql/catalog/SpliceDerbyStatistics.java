/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.db.catalog.Statistics;
import com.splicemachine.stats.TableStatistics;
import com.splicemachine.stats.estimate.Distribution;

/**
 * A Splice implementation of the Derby Statistics interface. This is unlikely
 * to be used, but we provide a basic implementation for completeness (as sort of a
 * just-in-case type situation).
 *
 * @author Scott Fines
 *         Date: 4/7/15
 */
public class SpliceDerbyStatistics implements Statistics{
    private final TableStatistics tableStats;

    public SpliceDerbyStatistics(TableStatistics tableStats){
        this.tableStats=tableStats;
    }

    @Override
    public long getConglomerateId(){
        return Long.parseLong(tableStats.tableId());
    }

    @Override public long getRowEstimate(){ return tableStats.rowCount(); }

    @Override
    public double selectivity(Object[] predicates){
        if(tableStats.rowCount()==0) return 0.1d;

        double selectivity = 1.0d;
        if(predicates!=null && predicates.length>0){
            for(int i=0;i<predicates.length;i++){
                //we assume that the predicates are = predicates
                Distribution colDist=tableStats.columnDistribution(i+1);
                selectivity*=colDist.selectivity((Comparable)predicates[i]);
            }
        }
        return selectivity;
    }

    @Override
    public int getColumnCount(){
        return tableStats.columnStatistics().size();
    }
}
