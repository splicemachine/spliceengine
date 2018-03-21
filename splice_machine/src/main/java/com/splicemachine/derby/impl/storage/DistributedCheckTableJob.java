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

    public DistributedCheckTableJob(){
    }

    public DistributedCheckTableJob(ActivationHolder ah,
                                    TxnView txn,
                                    String schemaName,
                                    String tableName,
                                    List<DDLMessage.TentativeIndex> tentativeIndexList,
                                    String jobGroup) {
        this.ah = ah;
        this.txn = txn;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tentativeIndexList = tentativeIndexList;
        this.jobGroup = jobGroup;
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
        jobGroup = in.readUTF();
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
        out.writeUTF(jobGroup);
    }

    @Override
    public String getName() {
        return "Check_Table";
    }
}
