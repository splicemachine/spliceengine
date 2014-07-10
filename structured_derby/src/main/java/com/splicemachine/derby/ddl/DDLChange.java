package com.splicemachine.derby.ddl;

import com.splicemachine.si.api.TransactionStorage;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.InheritingTxnView;

import java.io.*;

public class DDLChange implements Externalizable {

    public void setTxn(Txn txn) {
        this.txn = txn;
    }

    public enum TentativeType {
        CHANGE_PK, ADD_CHECK, CREATE_FK, CREATE_INDEX, ADD_NOT_NULL, ADD_COLUMN, DROP_COLUMN
    }
    /* Currently is the sequence ID from zookeeper for this change.  Example: 16862@host0000000005 */
    private String changeId;
    private DDLChangeType changeType;
    private TentativeDDLDesc tentativeDDLDesc;
    private Txn txn;

    /*Serialization constructor*/
    public DDLChange(){}

    public DDLChange(Txn txn) {
        // what is the meaning of a null change type?
        this(txn, null);
    }

    public DDLChange(Txn txn, DDLChangeType type) {
        this.txn = txn;
        this.changeType = type;
    }

    public Txn getTxn() {
        return txn;
    }

    public DDLChangeType getChangeType() {
        return changeType;
    }

    public boolean isTentative() {
        return changeType != null && changeType.isTentative();
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

        out.writeLong(txn.getTxnId());
        out.writeLong(txn.getBeginTimestamp());
        out.writeLong(txn.getParentTransaction().getTxnId());
        out.writeBoolean(txn.isDependent());
        out.writeBoolean(txn.isAdditive());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if(in.readBoolean())
            changeType = (DDLChangeType)in.readObject();
        if(in.readBoolean())
            changeId = in.readUTF();

        long txnId = in.readLong();
        long beginTs = in.readLong();
        long parentTxnId = in.readLong();
        boolean dependent = in.readBoolean();
        boolean additive = in.readBoolean();

        Txn parentTxn = TransactionStorage.getTxnSupplier().getTransaction(parentTxnId);
        txn = new InheritingTxnView(parentTxn,txnId,beginTs,null,true,dependent,true,additive,true,true,-1l,-1l, Txn.State.ACTIVE);
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
