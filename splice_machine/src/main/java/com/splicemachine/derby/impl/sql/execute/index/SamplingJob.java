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

package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;

/**
 * Created by jyuan on 10/9/18.
 */
public class SamplingJob extends DistributedJob implements Externalizable {
    String jobGroup;
    TxnView txn;
    ScanSetBuilder<ExecRow> scanSetBuilder;
    String scope;
    String prefix;
    DDLMessage.TentativeIndex tentativeIndex;
    int[] indexFormatIds;
    ActivationHolder ah;
    String tableVersion;
    String indexName;
    double sampleFraction;

    public SamplingJob() {}

    public SamplingJob(ActivationHolder ah,
                       TxnView txn,
                       ScanSetBuilder<ExecRow> scanSetBuilder,
                       String scope,
                       String jobGroup,
                       String prefix,
                       DDLMessage.TentativeIndex tentativeIndex,
                       int[]   indexFormatIds,
                       String  tableVersion,
                       String  indexName,
                       double  sampleFraction) {
        this.ah = ah;
        this.txn = txn;
        this.scanSetBuilder = scanSetBuilder;
        this.scope = scope;
        this.jobGroup = jobGroup;
        this.prefix = prefix;
        this.tentativeIndex = tentativeIndex;
        this.indexFormatIds = indexFormatIds;
        this.tableVersion = tableVersion;
        this.indexName = indexName;
        this.sampleFraction = sampleFraction;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new SamplingJobImpl(this, jobStatus);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ah);
        out.writeObject(scanSetBuilder);
        out.writeUTF(scope);
        out.writeUTF(jobGroup);
        out.writeUTF(prefix);
        out.writeObject(tentativeIndex.toByteArray());
        ArrayUtil.writeIntArray(out,indexFormatIds);
        SIDriver.driver().getOperationFactory().writeTxn(txn,out);
        out.writeUTF(tableVersion);
        out.writeUTF(indexName);
        out.writeDouble(sampleFraction);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ah = (ActivationHolder) in.readObject();
        scanSetBuilder = (ScanSetBuilder<ExecRow>) in.readObject();
        scope = in.readUTF();
        jobGroup = in.readUTF();
        prefix = in.readUTF();
        byte[] bytes = (byte[]) in.readObject();
        tentativeIndex = DDLMessage.TentativeIndex.parseFrom(bytes);
        indexFormatIds = ArrayUtil.readIntArray(in);
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        tableVersion = in.readUTF();
        indexName = in.readUTF();
        sampleFraction = in.readDouble();
    }
}
