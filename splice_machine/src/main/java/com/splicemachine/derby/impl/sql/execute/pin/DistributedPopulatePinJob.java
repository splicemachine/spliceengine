/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.pin;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;

/**
 *
 *
 */
public class DistributedPopulatePinJob extends DistributedJob implements Externalizable {
    String jobGroup;
    ScanSetBuilder<ExecRow> scanSetBuilder;
    String scope;
    String prefix;
    long conglomID;

    public DistributedPopulatePinJob() {}
    public DistributedPopulatePinJob(ScanSetBuilder<ExecRow> scanSetBuilder, String scope,
                                       String jobGroup, String prefix, long conglomID) {
        this.scanSetBuilder = scanSetBuilder;
        this.scope = scope;
        this.jobGroup = jobGroup;
        this.prefix = prefix;
        this.conglomID = conglomID;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new PopulatePinJob(this, jobStatus, conglomID);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(scanSetBuilder);
        out.writeUTF(scope);
        out.writeUTF(jobGroup);
        out.writeUTF(prefix);
        out.writeLong(conglomID);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        scanSetBuilder = (ScanSetBuilder<ExecRow>) in.readObject();
        scope = in.readUTF();
        jobGroup = in.readUTF();
        prefix = in.readUTF();
        conglomID = in.readLong();
    }
}
