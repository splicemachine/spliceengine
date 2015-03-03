package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.impl.stats.BaseDvdStatistics;
import com.splicemachine.stats.ColumnStatistics;
import org.apache.derby.agg.Aggregator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

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
    @SuppressWarnings("unchecked")
    public void add(DataValueDescriptor addend) throws StandardException {
        if(!addend.isNull())
            accumulate((ColumnStatistics)addend.getObject());
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
