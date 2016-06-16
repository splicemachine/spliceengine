package com.splicemachine.derby.utils.stats;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 6/15/16.
 */
public class StatsCollectionJob implements Callable<Void> {
    private static final Logger LOG = Logger.getLogger(StatsCollectionJob.class);
    private final DistributedStatsCollection request;
    private final OlapStatus jobStatus;

    public StatsCollectionJob(DistributedStatsCollection request, OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        this.request = request;
        this.jobStatus = jobStatus;
    }

    @Override
    public Void call() throws Exception {
        if (!jobStatus.markRunning()) {
            //the client has already cancelled us or has died before we could get started, so stop now
            LOG.error("Client bailed out");
            return null;
        }
        try {
            List<LocatedRow> result = request.scanSetBuilder.buildDataSet(request.scope).collect();
            jobStatus.markCompleted(new StatsResult(result));
            return null;
        } catch (Exception e) {
            LOG.error("Oops", e);
            throw e;
        }
    }
}
