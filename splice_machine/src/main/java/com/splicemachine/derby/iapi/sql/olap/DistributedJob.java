package com.splicemachine.derby.iapi.sql.olap;

import com.splicemachine.concurrent.Clock;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Representation of a Job which can be executed by a distributed server.
 *
 * @author Scott Fines
 *         Date: 4/1/16
 */
public abstract class DistributedJob implements Serializable{

    private UUID uuid;
    private boolean submitted = false;

    public DistributedJob() {
        this.uuid = UUID.randomUUID();
    }

    public abstract Callable<Void> toCallable(OlapStatus jobStatus,
                              Clock clock,
                              long clientTimeoutCheckIntervalMs);

    public abstract String getName();

    public final String getUniqueName() {
        return getName() + "-" + uuid.toString();
    }

    public final void markSubmitted() {
        if (submitted) {
            throw new IllegalStateException("Job already submitted: " + toString());
        }
        submitted = true;
    }

    public final boolean isSubmitted() {
        return submitted;
    }
}
