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

package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
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
public class DistributedPopulateIndexJob extends DistributedJob implements Externalizable {
    String jobGroup;
    TxnView childTxn;
    ScanSetBuilder<LocatedRow> scanSetBuilder;
    String scope;
    String prefix;
    DDLMessage.TentativeIndex tentativeIndex;
    int[] indexFormatIds;

    public DistributedPopulateIndexJob() {}
    public DistributedPopulateIndexJob(TxnView childTxn, ScanSetBuilder<LocatedRow> scanSetBuilder, String scope,
                                       String jobGroup, String prefix, DDLMessage.TentativeIndex tentativeIndex, int[] indexFormatIds) {
        this.childTxn = childTxn;
        this.scanSetBuilder = scanSetBuilder;
        this.scope = scope;
        this.jobGroup = jobGroup;
        this.prefix = prefix;
        this.tentativeIndex = tentativeIndex;
        this.indexFormatIds = indexFormatIds;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new PopulateIndexJob(this, jobStatus);
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
        out.writeObject(tentativeIndex.toByteArray());
        ArrayUtil.writeIntArray(out,indexFormatIds);
        SIDriver.driver().getOperationFactory().writeTxn(childTxn,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        scanSetBuilder = (ScanSetBuilder<LocatedRow>) in.readObject();
        scope = in.readUTF();
        jobGroup = in.readUTF();
        prefix = in.readUTF();
        byte[] bytes = (byte[]) in.readObject();
        tentativeIndex = DDLMessage.TentativeIndex.parseFrom(bytes);
        indexFormatIds = ArrayUtil.readIntArray(in);
        childTxn = SIDriver.driver().getOperationFactory().readTxn(in);
    }
}
