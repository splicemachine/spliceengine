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

package com.splicemachine.procedures.external;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;

/**
 * Created by jfilali on 11/14/16.
 * When a user make a modification in a external file outside of Splice, there is no way for use
 * to detect and reload the schema automatically. This Job is meant to be use with a procedure.
 *
 */
public class DistributedRefreshExternalTableSchemaJob extends DistributedJob implements Externalizable {

     private String jobGroup;
     private String location;




    public DistributedRefreshExternalTableSchemaJob() {
    }

    public DistributedRefreshExternalTableSchemaJob(String jobGroup, String location) {
        this.jobGroup = jobGroup;
        this.location = location;

    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new RefreshTableSchemaTableJob(this, jobStatus);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(jobGroup);
        out.writeUTF(location);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
       jobGroup = in.readUTF();
       location = in.readUTF();
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public String getLocation() {
        return location;
    }
}
