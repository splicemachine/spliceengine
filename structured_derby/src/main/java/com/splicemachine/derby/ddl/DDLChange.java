package com.splicemachine.derby.ddl;

import com.splicemachine.si.api.TransactionStorage;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class DDLChange implements Externalizable {


    public enum TentativeType {
        CHANGE_PK, ADD_CHECK, CREATE_FK, CREATE_INDEX, ADD_NOT_NULL, ADD_COLUMN, DROP_COLUMN
    }
    /* Currently is the sequence ID from zookeeper for this change.  Example: 16862@host0000000005 */
    private String changeId;
    private DDLChangeType changeType;
    private TentativeDDLDesc tentativeDDLDesc;
    private TxnView txn;

    private TxnView parentTxn;
    /*Serialization constructor*/
    public DDLChange(){}

    public DDLChange(TxnView txn) {
        this(txn, null);
    }

    public DDLChange(TxnView txn, DDLChangeType type) {
        this.txn = txn;
        this.changeType = type;
    }

    public void setTxn(Txn txn) {
        this.txn = txn;
    }

    public TxnView getTxn() {
        return txn;
    }

    public TxnView getParentTxn() { return parentTxn; }

    public void setParentTxn(TxnView parentTxn) { this.parentTxn = parentTxn; }

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
        out.writeLong(txn.getParentTxnId());
        out.writeBoolean(txn.isAdditive());

        out.writeBoolean(parentTxn!=null);
        if(parentTxn!=null){
            out.writeLong(parentTxn.getTxnId());
            out.writeLong(parentTxn.getParentTxnId());
            out.writeBoolean(parentTxn.isAdditive());
        }
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
        TxnSupplier txnSupplier = TransactionStorage.getTxnSupplier();
        txn = txnSupplier.getTransaction(txnId);

        if(in.readBoolean()){
            long pTxnId = in.readLong();
            long pBeginTs = pTxnId;
            long pParentTxnId = in.readLong();
            boolean pAdditive = in.readBoolean();

            parentTxn = txnSupplier.getTransaction(txnId);
        }
    }
}
