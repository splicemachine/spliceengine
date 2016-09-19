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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.agg.Aggregator;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.stats.ColumnStatistics;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
public class ColumnStatisticsMerge implements Aggregator<ColumnStatistics,
                                                         ColumnStatistics,
                                                         ColumnStatisticsMerge>,Externalizable {
    private ColumnStatistics<DataValueDescriptor> stats;
    @Override public void init() {  } //no-op

    @Override
    @SuppressWarnings("unchecked")
    public void accumulate(ColumnStatistics value) {
        if(stats==null)stats = clone(value);
        else stats = stats.merge(value);
    }


    @Override
    public void merge(ColumnStatisticsMerge otherAggregator) {
        if(stats==null) stats = otherAggregator.stats;
        else stats = stats.merge(otherAggregator.stats);
    }

    @Override
    public ColumnStatistics terminate() {
        return stats;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(stats!=null);
        if(stats!=null)
            out.writeObject(stats);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if(in.readBoolean())
            stats = (ColumnStatistics<DataValueDescriptor>)in.readObject();

    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private ColumnStatistics<DataValueDescriptor> clone(ColumnStatistics<DataValueDescriptor> value) {
        /*
         * When you do two functions over top of a merge operation, Derby's query optimizer
         * is dumb, and doesn't realize that the same aggregate has been applied twice--therefore,
         * instead of applying the aggregate once, then computing all the functions after the aggregation
         * has occurred, it computes the same aggregation twice. Thus, we can't just accept the reference
         * blindly, as that would result in merging the same values twice. We must clone the first
         * entry to ensure that we are safe w.r.t multiple aggregates within the same row looking
         * at the ColumnStatistics
         */
        return value.getClone();
    }
}
