package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapResult;

/**
 * Created by dgomezferro on 3/16/16.
 */
public interface Callback {
    void error(Throwable t);

    void complete(OlapResult result);
}
