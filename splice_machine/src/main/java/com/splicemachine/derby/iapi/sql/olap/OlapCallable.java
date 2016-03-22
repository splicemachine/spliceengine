package com.splicemachine.derby.iapi.sql.olap;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 3/16/16.
 */
public interface OlapCallable<R extends OlapResult> extends Callable<R>, Serializable {
    short getCallerId();
    void setCallerId(short callerId);
}
