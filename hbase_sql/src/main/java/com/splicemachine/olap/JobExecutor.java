package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * @author Scott Fines
 *         Date: 4/4/16
 */
public interface JobExecutor{

    Future<OlapResult> submit(DistributedJob job) throws IOException;

    void shutdown();
}
