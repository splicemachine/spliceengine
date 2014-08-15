package com.splicemachine.derby.ddl;

import java.io.Serializable;

/**
 * Object used to communicate/represent a single DDL change to remote nodes.
 */
public class DDLChange implements Serializable {

    /* Currently is the sequence ID from zookeeper for this change.  Example: 16862@host0000000005 */
    private String changeId;
    private String transactionId;
    private String parentTransactionId;
    private DDLChangeType changeType;
    private TentativeDDLDesc tentativeDDLDesc;

    public DDLChange(String transactionId) {
        // what is the meaning of a null change type?
        this(transactionId, null);
    }

    public DDLChange(String transactionId, DDLChangeType ddlChangeType) {
        this.transactionId = transactionId;
        this.changeType = ddlChangeType;
    }

    public String getTransactionId() {
        return transactionId;
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

    public void setChangeId(String changeId) {
        this.changeId = changeId;
    }

    @Override
    public String toString() {
        return "DDLChange{" +
                "transactionId='" + transactionId + '\'' +
                ", parentTransactionId='" + parentTransactionId + '\'' +
                ", type=" + changeType +
                ", tentativeDDLDesc=" + tentativeDDLDesc +
                ", identifier='" + changeId + '\'' +
                '}';
    }
}
