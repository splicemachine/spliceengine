package com.splicemachine.derby.ddl;

public class DDLChange {

    public enum TentativeType {
        CHANGE_PK, ADD_CHECK, CREATE_FK, CREATE_INDEX, ADD_NOT_NULL, ADD_COLUMN
    }

    private String transactionId;
    private TentativeType type;
    private boolean tentative;

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

}
