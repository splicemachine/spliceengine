package com.splicemachine.derby.ddl;

import java.io.Serializable;

public class DDLChange implements Serializable {

    public enum TentativeType {
        CHANGE_PK, ADD_CHECK, CREATE_FK, CREATE_INDEX, ADD_NOT_NULL, ADD_COLUMN, DROP_COLUMN
    }

    private String transactionId;
    private String parentTransactionId;
    private TentativeType type;
    private boolean tentative;
    private TentativeDDLDesc tentativeDDLDesc;
    private String identifier;

    public DDLChange(String transactionId) {
        this(transactionId, null);
    }

    public DDLChange(String transactionId, TentativeType type) {
        this.transactionId = transactionId;
        this.type = type;
        this.tentative = type != null;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public TentativeType getType() {
        return type;
    }

    public boolean isTentative() {
        return tentative;
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

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
}
