package com.splicemachine.derby.ddl;

import com.splicemachine.si.api.Txn;

import java.io.*;

public class DDLChange implements Externalizable {



    public enum TentativeType {
        CHANGE_PK, ADD_CHECK, CREATE_FK, CREATE_INDEX, ADD_NOT_NULL, ADD_COLUMN, DROP_COLUMN
    }
    /* Currently is the sequence ID from zookeeper for this change.  Example: 16862@host0000000005 */
    private String changeId;
    private String parentTransactionId;
    
    private DDLChangeType changeType;
    private TentativeDDLDesc tentativeDDLDesc;
    private Txn txn;

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

    public String getParentTransactionId() {
        return parentTransactionId;
    }

    public void setParentTransactionId(String parentTransactionId) {
        this.parentTransactionId = parentTransactionId;
    }

    public String getChangeId() {
        return changeId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException("IMPLEMENT");
    }
    
    public void setChangeId(String changeId) {
        this.changeId = changeId;
    }

    @Override
    public String toString() {
        return "DDLChange{" +
                "txn='" + txn + '\'' +
                ", parentTransactionId='" + parentTransactionId + '\'' +
                ", type=" + changeType +
                ", tentativeDDLDesc=" + tentativeDDLDesc +
                ", identifier='" + changeId + '\'' +
                '}';
    }
}
