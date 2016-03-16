package com.splicemachine.derby.iapi.sql.olap;

import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 3/16/16.
 */
public interface OlapCallable<R extends OlapResult> extends Callable<R> {
    short getCallerId();
    void setCallerId(short callerId);
}
