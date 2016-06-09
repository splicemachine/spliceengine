package com.splicemachine.stream;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.ActivationHolder;

import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 5/20/16.
 */
public class RemoteQueryJob extends DistributedJob {
    final UUID uuid;
    int rootResultSetNumber;
    ActivationHolder ah;
    String host;
    int port;
    String jobGroup;

    public RemoteQueryJob(ActivationHolder ah, int rootResultSetNumber, UUID uuid, String host, int port) {
        this.ah = ah;
        this.rootResultSetNumber = rootResultSetNumber;
        this.uuid = uuid;
        this.host = host;
        this.port = port;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new QueryJob(this, jobStatus, clock, clientTimeoutCheckIntervalMs);
    }

    @Override
    public String getName() {
        return "query";
    }
}
