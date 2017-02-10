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

package com.splicemachine.derby.impl.sql.execute.altertable;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 6/15/16.
 */
public class DistributedAlterTableTransformJob extends DistributedJob implements Externalizable {
    long destConglom;
    String jobGroup;
    TxnView childTxn;
    ScanSetBuilder<KVPair> scanSetBuilder;
    String description;
    String pool;
    DDLMessage.DDLChange ddlChange;

    public DistributedAlterTableTransformJob() {}
    public DistributedAlterTableTransformJob(TxnView childTxn, ScanSetBuilder<KVPair> scanSetBuilder, long destConglom,
                                             String description, String jobName, String pool, DDLMessage.DDLChange ddlChange) {
        this.childTxn = childTxn;
        this.jobGroup = jobName;
        this.scanSetBuilder = scanSetBuilder;
        this.destConglom = destConglom;
        this.description = description;
        this.pool = pool;
        this.ddlChange = ddlChange;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new AlterTableTransformJob(this, jobStatus);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(destConglom);
        out.writeObject(scanSetBuilder);
        out.writeUTF(description);
        out.writeUTF(jobGroup);
        out.writeUTF(pool);
        out.writeObject(ddlChange.toByteArray());
        SIDriver.driver().getOperationFactory().writeTxn(childTxn,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        destConglom = in.readLong();
        scanSetBuilder = (ScanSetBuilder<KVPair>) in.readObject();
        description = in.readUTF();
        jobGroup = in.readUTF();
        pool = in.readUTF();
        byte[] bytes = (byte[]) in.readObject();
        ddlChange = DDLMessage.DDLChange.parseFrom(bytes);
        childTxn = SIDriver.driver().getOperationFactory().readTxn(in);
    }
}
