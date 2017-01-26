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
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import org.apache.spark.sql.types.StructType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;

/**
 * An Distributed job to get the schema of the external file
 */
public class DistributedGetSchemaExternalJob extends DistributedJob implements Externalizable {

    private String location;
    private String storedAs;
    private String jobGroup;

    public DistributedGetSchemaExternalJob() {
    }

    public DistributedGetSchemaExternalJob(String location,
                                           String jobGroup,
                                           String storedAs) {
        this.storedAs = storedAs;
        this.location = location;
        this.jobGroup = jobGroup;

    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new GetSchemaExternalJob(this, jobStatus);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeBoolean(storedAs!=null);
        if (storedAs!=null)
            out.writeUTF(storedAs);

        out.writeBoolean(location!=null);
        if (location!=null)
            out.writeUTF(location);

        out.writeUTF(jobGroup);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        storedAs    = in.readBoolean()?in.readUTF():null;
        location    = in.readBoolean()?in.readUTF():null;
        jobGroup    = in.readUTF();


    }


    public String getLocation() {
        return location;
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public String getStoredAs(){ return storedAs; }


}
