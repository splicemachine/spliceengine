package com.splicemachine.olap;


/**
 * Created by dgomezferro on 3/16/16.
 */
public interface Callback<T> {
    void error(Throwable t);

    void complete(T result);
}
