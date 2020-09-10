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
package com.splicemachine.derby.impl.storage;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by jyuan on 2/5/18.
 */
public class DistributedCheckTableJob extends DistributedJob implements Externalizable {
    TxnView txn;
    ActivationHolder ah;
    String schemaName;
    String tableName;
    String jobGroup;
    List<DDLMessage.TentativeIndex> tentativeIndexList;
    boolean fix;
    boolean isSystemTable;

    public DistributedCheckTableJob(){
    }

    public DistributedCheckTableJob(ActivationHolder ah,
                                    TxnView txn,
                                    String schemaName,
                                    String tableName,
                                    List<DDLMessage.TentativeIndex> tentativeIndexList,
                                    boolean fix,
                                    String jobGroup,
                                    boolean isSystemTable) {
        this.ah = ah;
        this.txn = txn;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tentativeIndexList = tentativeIndexList;
        this.fix = fix;
        this.jobGroup = jobGroup;
        this.isSystemTable = isSystemTable;
    }


    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus,
                                     Clock clock,
                                     long clientTimeoutCheckIntervalMs) {
        return new CheckTableJob(this, jobStatus);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        txn = SIDriver.driver().getOperationFactory().readTxn(in);
        ah = (ActivationHolder) in.readObject();
        schemaName = in.readUTF();
        tableName = in.readUTF();
        int iSize = in.readInt();
        tentativeIndexList = new ArrayList<>(iSize);
        for (int i = 0; i< iSize; i++) {
            byte[] message = new byte[in.readInt()];
            in.readFully(message);
            tentativeIndexList.add(DDLMessage.TentativeIndex.parseFrom(message));
        }
        fix = in.readBoolean();
        jobGroup = in.readUTF();
        isSystemTable = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        SIDriver.driver().getOperationFactory().writeTxn(txn,out);
        out.writeObject(ah);
        out.writeUTF(schemaName);
        out.writeUTF(tableName);
        out.writeInt(tentativeIndexList.size());
        for (DDLMessage.TentativeIndex ti: tentativeIndexList) {
            byte[] message = ti.toByteArray();
            out.writeInt(message.length);
            out.write(message);
        }
        out.writeBoolean(fix);
        out.writeUTF(jobGroup);
        out.writeBoolean(isSystemTable);
    }

    @Override
    public String getName() {
        return "Check_Table";
    }
}
