package com.splicemachine.orc.metadata;

import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * Created by jleach on 3/22/17.
 */
public class StatsEval {
    public boolean hasNulls;
    public boolean allNulls;
    public boolean alwaysTrue;
    public boolean alwaysFalse;
    public DataValueDescriptor minimumDVD;
    public DataValueDescriptor maximumDVD;

    public StatsEval() {}

}
