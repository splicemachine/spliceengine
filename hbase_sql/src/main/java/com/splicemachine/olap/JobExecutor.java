package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import org.sparkproject.guava.util.concurrent.ListenableFuture;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/4/16
 */
public interface JobExecutor{

    ListenableFuture<OlapResult> submit(DistributedJob job) throws IOException;

    void shutdown();
}
