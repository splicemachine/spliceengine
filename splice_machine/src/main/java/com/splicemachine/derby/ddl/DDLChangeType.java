/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
