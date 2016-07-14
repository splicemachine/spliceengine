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

package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import com.splicemachine.utils.StringUtils;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public abstract class StringStatsCollector extends DvdStatsCollector{
    private ColumnStatsCollector<String> baseCollector;

    protected StringStatsCollector(ColumnStatsCollector<String> collector) {
        super(collector);
        this.baseCollector = collector;
    }

    @Override
    protected void doUpdate(DataValueDescriptor dataValueDescriptor, long count) throws StandardException {
        String string=getString(dataValueDescriptor);
        baseCollector.update(string,count);
    }

    protected String getString(DataValueDescriptor dataValueDescriptor) throws StandardException{
        return dataValueDescriptor.getString();
    }

    public static StringStatsCollector charCollector(ColumnStatsCollector<String> collector,final int strLen){
        return new StringStatsCollector(collector) {
            @Override
            @SuppressWarnings("unchecked")
            protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
                return new CharStats((ColumnStatistics<String>)build,strLen);
            }
        };
    }

    public static StringStatsCollector varcharCollector(ColumnStatsCollector<String> collector,final int strLen){
        return new StringStatsCollector(collector) {
            @Override
            protected String getString(DataValueDescriptor dataValueDescriptor) throws StandardException{
                /*
                 * DB-3813. Trim trailing whitspace so that all the equivalent strings are grouped together
                 */
                return StringUtils.trimTrailingSpaces(super.getString(dataValueDescriptor));
            }

            @Override
            @SuppressWarnings("unchecked")
            protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
                return new VarcharStats((ColumnStatistics<String>)build,strLen);
            }
        };
    }
}
