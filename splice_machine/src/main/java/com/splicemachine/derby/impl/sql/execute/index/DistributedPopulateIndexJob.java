package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.txn.Txn;
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
    TxnView childTxn;
    ScanSetBuilder<KVPair> scanSetBuilder;
    String scope;
    String prefix;
    DDLMessage.TentativeIndex tentativeIndex;

    public DistributedPopulateIndexJob() {}

    public DistributedPopulateIndexJob(TxnView childTxn, ScanSetBuilder<KVPair> scanSetBuilder, String scope, String prefix, DDLMessage.TentativeIndex tentativeIndex) {
        this.childTxn = childTxn;
        this.scanSetBuilder = scanSetBuilder;
        this.scope = scope;
        this.prefix = prefix;
        this.tentativeIndex = tentativeIndex;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new PopulateIndexJob(this, jobStatus, clock, clientTimeoutCheckIntervalMs);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(scanSetBuilder);
        out.writeUTF(scope);
        out.writeUTF(prefix);
        out.writeObject(tentativeIndex.toByteArray());
        SIDriver.driver().getOperationFactory().writeTxn(childTxn,out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        scanSetBuilder = (ScanSetBuilder<KVPair>) in.readObject();
        scope = in.readUTF();
        prefix = in.readUTF();
        byte[] bytes = (byte[]) in.readObject();
        tentativeIndex = DDLMessage.TentativeIndex.parseFrom(bytes);
        childTxn = SIDriver.driver().getOperationFactory().readTxn(in);
    }
}
