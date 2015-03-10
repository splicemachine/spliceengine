package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.estimate.BooleanDistribution;
import com.splicemachine.stats.estimate.Distribution;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class BooleanUniformDist implements Distribution<DataValueDescriptor> {
    private BooleanDistribution distribution;
    private long nullCount;
    public BooleanUniformDist(long nullCount,BooleanDistribution distribution) {
        this.distribution = distribution;
        this.nullCount = nullCount;
    }

    @Override
    public long selectivity(DataValueDescriptor element) {
        if(element==null||element.isNull())
            return nullCount;
        return distribution.selectivity(safeBoolean(element));
    }

    @Override
    public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
        boolean s,e;
        if(start==null||start.isNull()){
            s = true;
            includeStart = true;
        }else
            s = safeBoolean(start);
        if(stop==null||stop.isNull()){
            e = false;
            includeStop = true;
        }else e = safeBoolean(stop);
        return distribution.rangeSelectivity(s,e,includeStart,includeStop);
    }

    private boolean safeBoolean(DataValueDescriptor start) {
        try {
            return start.getBoolean();
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }
}
