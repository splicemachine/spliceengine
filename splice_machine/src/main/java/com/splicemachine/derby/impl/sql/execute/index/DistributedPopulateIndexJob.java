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

package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.si.api.txn.Txn;
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
    Txn childTxn;
    ScanSetBuilder<LocatedRow> scanSetBuilder;
    String scope;
    String prefix;
    DDLMessage.TentativeIndex tentativeIndex;
    int[] indexFormatIds;

    public DistributedPopulateIndexJob() {}
    public DistributedPopulateIndexJob(Txn childTxn, ScanSetBuilder<LocatedRow> scanSetBuilder, String scope,
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
