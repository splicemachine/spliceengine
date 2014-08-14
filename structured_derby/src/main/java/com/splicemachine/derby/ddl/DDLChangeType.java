package com.splicemachine.derby.ddl;

public enum DDLChangeType {

    CHANGE_PK(true),
    ADD_CHECK(true),
    CREATE_FK(true),
    CREATE_INDEX(true),
    ADD_NOT_NULL(true),
    ADD_COLUMN(true),
    DROP_COLUMN(true),

    DROP_TABLE(false);

    private boolean tentative;

    DDLChangeType(boolean tentative) {
        this.tentative = tentative;
    }

    public boolean isTentative() {
        return tentative;
    }

    @Override
    public String toString() {
        return super.toString() + "{" + "tentative=" + tentative + '}';
    }
}
