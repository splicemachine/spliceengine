package com.splicemachine.derby.ddl;

/**
 * Indicate the type of DDL change.  These are currently largely unused.  Maybe eventually consolidate with
 * action constants at top of derby's DependencyManager interface.
 */
public enum DDLChangeType {

    CHANGE_PK(true),
    ADD_CHECK(true),
    CREATE_FK(true),
    CREATE_INDEX(true),
    ADD_NOT_NULL(true),
    ADD_COLUMN(true),
    DROP_COLUMN(true),
    DROP_TABLE(true),
    DROP_SCHEMA(true),
    DROP_INDEX(true),
    ENTER_RESTORE_MODE(false);

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
