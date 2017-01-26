/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
