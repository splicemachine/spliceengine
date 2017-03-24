package com.splicemachine.orc.metadata;

import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * Created by jleach on 3/22/17.
 */
public interface MinMaxStatistics {

    DataValueDescriptor getMinDVD();

    DataValueDescriptor getMaxDVD();
}
