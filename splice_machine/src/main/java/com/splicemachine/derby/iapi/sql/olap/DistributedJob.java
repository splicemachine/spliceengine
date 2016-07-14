/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
