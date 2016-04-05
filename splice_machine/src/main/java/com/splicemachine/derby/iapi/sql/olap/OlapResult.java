package com.splicemachine.derby.iapi.sql.olap;

import java.io.Serializable;

/**
 *
 * Created by dgomezferro on 3/15/16.
 */
public interface OlapResult extends Serializable {
    Throwable getThrowable();

    boolean isSuccess();
}
