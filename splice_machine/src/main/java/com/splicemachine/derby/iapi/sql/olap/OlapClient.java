package com.splicemachine.derby.iapi.sql.olap;

import java.io.IOException;

/**
 * Created by dgomezferro on 3/16/16.
 */
public interface OlapClient {
    public <R extends OlapResult> R submitOlapJob(OlapCallable<R> callable) throws IOException;
}
