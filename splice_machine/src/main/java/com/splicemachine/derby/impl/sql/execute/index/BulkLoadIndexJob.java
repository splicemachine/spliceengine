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
 * Created by jyuan on 10/4/17.
 */
public class BulkLoadIndexJob extends DistributedJob implements Externalizable {

    String jobGroup;
    TxnView childTxn;
    ScanSetBuilder<ExecRow> scanSetBuilder;
    String scope;
    String prefix;
    DDLMessage.TentativeIndex tentativeIndex;
    int[] indexFormatIds;
    boolean sampling;
    String  hfilePath;
    ActivationHolder ah;
    String tableVersion;
    String indexName;

    public BulkLoadIndexJob() {}
    public BulkLoadIndexJob(ActivationHolder ah,
                            TxnView childTxn,
                            ScanSetBuilder<ExecRow> scanSetBuilder,
                            String scope,
                            String jobGroup,
                            String prefix,
                            DDLMessage.TentativeIndex tentativeIndex,
                            int[]    indexFormatIds,
                            boolean  sampling,
                            String   hfilePath,
                            String   tableVersion,
                            String   indexName) {
        this.ah = ah;
        this.childTxn = childTxn;
        this.scanSetBuilder = scanSetBuilder;
        this.scope = scope;
        this.jobGroup = jobGroup;
        this.prefix = prefix;
        this.tentativeIndex = tentativeIndex;
        this.indexFormatIds = indexFormatIds;
        this.sampling = sampling;
        this.hfilePath = hfilePath;
        this.tableVersion = tableVersion;
        this.indexName = indexName;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new BulkLoadIndexJobImpl(this, jobStatus);
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
        SIDriver.driver().getOperationFactory().writeTxn(childTxn,out);
        out.writeUTF(tableVersion);
        out.writeUTF(hfilePath);
        out.writeBoolean(sampling);
        out.writeUTF(indexName);
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
        childTxn = SIDriver.driver().getOperationFactory().readTxn(in);
        tableVersion = in.readUTF();
        hfilePath = in.readUTF();
        sampling = in.readBoolean();
        indexName = in.readUTF();
    }
}
