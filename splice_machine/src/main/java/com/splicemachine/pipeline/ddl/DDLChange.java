package com.splicemachine.pipeline.ddl;

import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.LazyTxnView;
import com.splicemachine.si.impl.TransactionStorage;

import org.apache.log4j.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class DDLChange implements Externalizable {
    private static final Logger LOG = Logger.getLogger(DDLChange.class);
    /* Currently is the sequence ID from zookeeper for this change.  Example: 16862@host0000000005 */
    private String changeId;
    private DDLChangeType changeType;
    private TentativeDDLDesc tentativeDDLDesc;
    private TxnView txn;

    /*Serialization constructor*/
    public DDLChange(){}

    public DDLChange(TxnView txn) {
        this(txn, null);
    }

    public DDLChange(TxnView txn, DDLChangeType type) {
        this.txn = txn;
        assert txn.allowsWrites(): "Cannot create a DDLChange with a read-only transaction";
        this.changeType = type;
    }

    public DDLChange(TxnView txn, DDLChangeType type, TentativeDDLDesc tentativeDDLDesc) {
        this(txn, type);
        this.tentativeDDLDesc = tentativeDDLDesc;
    }

    public void setTxn(Txn txn) {
        assert txn.allowsWrites(): "Cannot create a DDLChange with a read-only transaction";
        this.txn = txn;
    }

    public TxnView getTxn() {
        return txn;
    }

    public DDLChangeType getChangeType() {
        return changeType;
    }

    public boolean isPreCommit() {
        return changeType != null && changeType.isPreCommit();
    }

    public boolean isPostCommit() {
        return changeType != null && changeType.isPostCommit();
    }

    public TentativeDDLDesc getTentativeDDLDesc() {
        return tentativeDDLDesc;
    }

    public void setTentativeDDLDesc(TentativeDDLDesc tentativeDDLDesc) {
        this.tentativeDDLDesc = tentativeDDLDesc;
    }

    public String getChangeId() {
        return changeId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(changeType!=null);
        if(changeType!=null)
            out.writeObject(changeType);
        out.writeBoolean(changeId!=null);
        if(changeId!=null)
            out.writeUTF(changeId);
        out.writeBoolean(tentativeDDLDesc!=null);
        if(tentativeDDLDesc!=null)
            out.writeObject(tentativeDDLDesc);

        out.writeLong(txn.getTxnId());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if(in.readBoolean())
            changeType = (DDLChangeType)in.readObject();
        if(in.readBoolean())
            changeId = in.readUTF();
        if(in.readBoolean())
            tentativeDDLDesc = (TentativeDDLDesc)in.readObject();

        long txnId = in.readLong();
        txn = new LazyTxnView(txnId,TransactionStorage.getTxnSupplier());
    }
    
    public void setChangeId(String changeId) {
        this.changeId = changeId;
    }

    @Override
    public String toString() {
        return "DDLChange{" +
                "txn='" + txn + '\'' +
                ", type=" + changeType +
                ", tentativeDDLDesc=" + tentativeDDLDesc +
                ", identifier='" + changeId + '\'' +
                '}';
    }
}
