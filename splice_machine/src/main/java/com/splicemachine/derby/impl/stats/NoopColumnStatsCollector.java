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

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;

/**
 * @author Scott Fines
 *         Date: 3/10/15
 */
public class NoopColumnStatsCollector implements ColumnStatsCollector<DataValueDescriptor>{
    private static ColumnStatsCollector<DataValueDescriptor> INSTANCE = new NoopColumnStatsCollector();

    private NoopColumnStatsCollector(){}

    public static ColumnStatsCollector<DataValueDescriptor> collector(){
        return INSTANCE;
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> build() {
        return null;
    }

    @Override public void updateNull() {  }
    @Override public void updateNull(long count) {  }
    @Override public void updateSize(int size) {  }
    @Override public void update(DataValueDescriptor item) {  }
    @Override public void update(DataValueDescriptor item, long count) {  }
}
