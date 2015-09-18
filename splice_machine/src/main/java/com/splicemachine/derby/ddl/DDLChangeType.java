package com.splicemachine.derby.ddl;

/**
 * Indicate the type of DDL change.  These are currently largely unused.  Maybe eventually consolidate with
 * action constants at top of derby's DependencyManager interface.
 *
 * a <em>preCommit</em> action is a ddl change which initiates some activity prior to the committing of the user
 * transaction. Because this is prior to committing, we do <em>not</em> initiate a global change as a result of these
 * changes.
 *
 * A <em>postCommit</em> action is a ddl change which does not initiate any activity until the user transaction
 * is committed. This action is specific to the type, and therefore a global DDL change is not initiated.
 */
public enum DDLChangeType {

    CHANGE_PK(true,false),
    ADD_CHECK(true,false),
    ADD_FOREIGN_KEY(true,false),
    CREATE_INDEX(true,false),
    ADD_NOT_NULL(true,false),
    ADD_COLUMN(true,false),
    ADD_PRIMARY_KEY(true,false),
    ADD_UNIQUE_CONSTRAINT(true,false),
    DROP_COLUMN(true,false),
    DROP_CONSTRAINT(true,false),
    DROP_PRIMARY_KEY(true,false),
    DROP_TABLE(true,false),
    DROP_SCHEMA(true,false),
    DROP_INDEX(true,false),
    DROP_FOREIGN_KEY(true,false),
    CLEAR_STATS_CACHE(false,true),
    ENTER_RESTORE_MODE(false,true);

    private final boolean preCommit;
    private final boolean postCommit;

    DDLChangeType(boolean preCommit,boolean postCommit) {
        this.preCommit=preCommit;
        this.postCommit=postCommit;
    }

    public boolean isPreCommit() { return preCommit; }
    public boolean isPostCommit(){ return postCommit; }

    @Override
    public String toString() {
        return super.toString() + "{" + "tentative=" +preCommit+ '}';
    }
}
