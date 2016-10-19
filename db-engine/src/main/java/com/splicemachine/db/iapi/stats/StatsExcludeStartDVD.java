package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * Created by jleach on 10/18/16.
 */
public class StatsExcludeStartDVD extends StatsBoundaryDataValueDescriptor {

    public StatsExcludeStartDVD(DataValueDescriptor dvd) {
        super(dvd);
    }

    @Override
    public int compare(DataValueDescriptor other) throws StandardException {
        int compare = dvd.compare(other);
        return compare == 0? +1: compare;
    }

    @Override
    public int typePrecedence() {
        return Integer.MAX_VALUE;
    }
}