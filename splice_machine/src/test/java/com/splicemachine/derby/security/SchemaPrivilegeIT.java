/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.security;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTest;
import com.splicemachine.test.SlowTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.assertFailed;
import static com.splicemachine.derby.test.framework.SpliceUnitTest.resultSetSize;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * Created by jfilali on 8/22/16.
 */
@Category(SlowTest.class)
public class SchemaPrivilegeIT {

    private static final String SCHEMA1 = "MAIN_SCH";
    private static final String SECOND_SCHEMA = "SECOND_SCH";
    private static final String THIRD_SCHEMA = "THIRD_SCH";

    private static final String TABLE = "Y";
    private static final String TABLE2 = "Z";
    private static final String IX_TABLE2 = "IX_Z";
    private static final String TABLE3 = "CDL";



    protected static final String USER1 = "JOHN";
    protected static final String PASSWORD1 = "jleach";
    protected static final String ROLE1 = "ROLE1";
    protected static final String ROLE2 = "ROLE2";

    protected static final String USER2 = "JIM";
    protected static final String PASSWORD2 = "bo";

    protected static final String USER3 = "SUZY";
    protected static final String PASSWORD3 = "X)X)X";

    private static SpliceWatcher        spliceClassWatcherAdmin = new SpliceWatcher();
    private static SpliceSchemaWatcher  spliceSchemaWatcher1 = new SpliceSchemaWatcher(SCHEMA1);
    private static SpliceSchemaWatcher  spliceSchemaWatcherUser3 = new SpliceSchemaWatcher(SECOND_SCHEMA,USER3);
    private static SpliceSchemaWatcher  spliceSchema3WatcherUser3 = new SpliceSchemaWatcher(THIRD_SCHEMA,USER3);
    private static SpliceUserWatcher    spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);
    private static SpliceUserWatcher    spliceUserWatcher2 = new SpliceUserWatcher(USER2, PASSWORD2);
    private static SpliceUserWatcher    spliceUserWatcher3 = new SpliceUserWatcher(USER3, PASSWORD3);
    private static SpliceRoleWatcher    spliceRoleWatcher1 = new SpliceRoleWatcher(ROLE1);
    private static SpliceRoleWatcher    spliceRoleWatcher2 = new SpliceRoleWatcher(ROLE2);
    private static SpliceTableWatcher   tableWatcher = new SpliceTableWatcher(TABLE, SCHEMA1,"(a int PRIMARY KEY, b int, c int)" );
    private static SpliceTableWatcher   table2Watcher = new SpliceTableWatcher(TABLE2, SCHEMA1,"(a int PRIMARY KEY, b int, c int)" );
    private static SpliceIndexWatcher   index2Watcher = new SpliceIndexWatcher(TABLE2, SCHEMA1, IX_TABLE2, SCHEMA1, "(b, c)");
    private static SpliceTableWatcher   tableWatcherUser3 = new SpliceTableWatcher(TABLE, SECOND_SCHEMA,"(a int PRIMARY KEY, b int, c int)" );
    private static SpliceTableWatcher   table3WatcherUser3 = new SpliceTableWatcher(TABLE3, THIRD_SCHEMA,"(a int PRIMARY KEY, b int, c int)" );
    @Rule
    public  TestRule chain =
            RuleChain.outerRule(spliceClassWatcherAdmin)
            .around(spliceUserWatcher1)
            .around(spliceUserWatcher2)
            .around(spliceUserWatcher3)
            .around(spliceRoleWatcher1)
            .around(spliceRoleWatcher2)
            .around(spliceSchemaWatcher1)
            .around(spliceSchemaWatcherUser3)
            .around(spliceSchema3WatcherUser3)
            .around(tableWatcher)
            .around(table2Watcher)
            .around(index2Watcher)
            .around(tableWatcherUser3)
            .around(table3WatcherUser3);

    protected static TestConnection adminConn;
    protected static TestConnection user1Conn;
    protected static TestConnection user2Conn;
    protected static TestConnection user3Conn;
    protected static TestConnection user1Conn2;
    protected static TestConnection user2Conn2;
    protected static TestConnection user3Conn2;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private String selectQuery      = format("select a from %s.%s where a=1", SCHEMA1, TABLE);
    private String updateQuery      = format("update %s.%s set a = 1", SCHEMA1, TABLE);
    private String insertQuery      = format("insert into %s.%s values(12,11,12)", SCHEMA1, TABLE);
    private String deleteQuery      = format("delete from %s.%s ", SCHEMA1, TABLE);
    private String triggerQuery     = format("create trigger test_trigger after insert on %s.%s for each row values 1", SCHEMA1, TABLE);
    private String foreignKeyQuery  = format("CREATE TABLE D( d1 int, d2 int REFERENCES %s.%s(a))", SCHEMA1,TABLE);
    private String createTableQuery = format("create table %s.FOO(col1 int, col2 int, col3 int)", SCHEMA1);
    private String dropTableQuery   = format("drop table %s.%s", SCHEMA1, TABLE2);
    private String modifyTableQuery = format("alter table %s.%s add column col4 int", SCHEMA1, TABLE2);
    private String truncateTableQuery = format("truncate table %s.%s", SCHEMA1, TABLE2);
    private String pinTableQuery    = format("pin table %s.%s", SCHEMA1, TABLE);
    private String unpinTableQuery    = format("unpin table %s.%s", SCHEMA1, TABLE);
    private String renameColumnQuery = format("rename column %s.%s.c to c_new", SCHEMA1, TABLE2);
    private String renameTableQuery = format("rename table %s.%s to %s_NEW", SCHEMA1, TABLE2, TABLE2);
    // according to doc, rename index can only be performed under the current schema
    private String renameIndexQuery = format("rename index %s to %s_new", IX_TABLE2, IX_TABLE2);



    @Before
    public  void setUpClass() throws Exception {
        adminConn = spliceClassWatcherAdmin.createConnection();
        user1Conn = spliceClassWatcherAdmin.connectionBuilder().user(USER1).password(PASSWORD1).build();
        user2Conn = spliceClassWatcherAdmin.connectionBuilder().user(USER2).password(PASSWORD2).build();
        user3Conn = spliceClassWatcherAdmin.connectionBuilder().user(USER3).password(PASSWORD3).build();
        adminConn.execute( format("insert into %s.%s values ( 1, 2, 3)", SCHEMA1, TABLE ) );
        adminConn.execute( format("insert into %s.%s values ( 1, 2, 3)", SCHEMA1, TABLE2 ) );
        user3Conn.execute( format("insert into %s.%s values ( 1, 2, 3)", SECOND_SCHEMA, TABLE ) );
        user3Conn.execute(format("insert into %s.%s values ( 4, 5, 6)", THIRD_SCHEMA, TABLE3 ));
        user1Conn2 = spliceClassWatcherAdmin.connectionBuilder().port(1528).create(true).user(USER1).password(PASSWORD1).build();
        user2Conn2 = spliceClassWatcherAdmin.connectionBuilder().port(1528).create(true).user(USER2).password(PASSWORD2).build();
        user3Conn2 = spliceClassWatcherAdmin.connectionBuilder().port(1528).create(true).user(USER3).password(PASSWORD3).build();
    }

    @Test
    public void testCreateAllPrivileges() throws Exception {
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        ps.execute();
        String query = format("SELECT a from %s.%s where a=1", SCHEMA1,TABLE);
        ResultSet rs = user1Conn.query(query );
        assertEquals("Expected to be have all privileges", 1, resultSetSize(rs));
        assertFailed(user2Conn, query, SQLState.AUTH_NO_COLUMN_PERMISSION);

        //create/drop table privilege would work with grant all privileges
        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        PreparedStatement createTable = user1Conn.prepareStatement(createTableQuery);
        createTable.execute();

        query = format("drop table %s.FOO", SCHEMA1);
        assertFailed(user2Conn, query, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        PreparedStatement dropTable = user1Conn.prepareStatement(query);
        dropTable.execute();
    }

    @Test
    public void testCreateSelectPrivileges() throws Exception {
        //REVOKE ALL to start
        PreparedStatement ps = adminConn.prepareStatement( format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        ps.execute();
        //GRANT
        ps = adminConn.prepareStatement( format("GRANT SELECT ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        ps.execute();
        //SELECT
        ResultSet rs = user1Conn.query(selectQuery );
        assertEquals("Expected to be have SELECT privileges", 1, resultSetSize(rs));
        //UPDATE
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //FOREIGN KEYS
        assertFailed(user2Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //create table
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //drop table
        assertFailed(user1Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //modify table
        assertFailed(user1Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //truncate table
        assertFailed(user1Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //pin table
        assertFailed(user1Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //unpin table
        assertFailed(user1Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename column
        assertFailed(user1Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename table
        assertFailed(user1Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename index
        user1Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user1Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        user2Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user2Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    public void testCreateDeletePrivileges() throws Exception {
        //REVOKE ALL to start
        PreparedStatement ps = adminConn.prepareStatement( format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        ps.execute();
        //GRANT
        ps = adminConn.prepareStatement( format("GRANT DELETE ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        ps.execute();
        //SELECT
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        PreparedStatement delete = user1Conn.prepareStatement(deleteQuery);
        delete.execute();
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //FOREIGN KEYS
        assertFailed(user2Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //create table
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //drop table
        assertFailed(user1Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //modify table
        assertFailed(user1Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //truncate table
        assertFailed(user1Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //pin table
        assertFailed(user1Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //unpin table
        assertFailed(user1Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename column
        assertFailed(user1Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename table
        assertFailed(user1Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename index
        user1Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user1Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        user2Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user2Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);

    }

    @Test
    public void testCreateUpdatePrivileges() throws Exception {
        //REVOKE ALL to start
        PreparedStatement ps = adminConn.prepareStatement( format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        ps.execute();
        //GRANT
        ps = adminConn.prepareStatement( format("GRANT UPDATE ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        ps.execute();
        //SELECT
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE - if it failed it will throw an exception and the test will failed
        PreparedStatement update = user1Conn.prepareStatement(updateQuery );
        update.execute();
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //FOREIGN KEYS
        assertFailed(user2Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //create table
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //drop table
        assertFailed(user1Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //modify table
        assertFailed(user1Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //truncate table
        assertFailed(user1Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //pin table
        assertFailed(user1Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //unpin table
        assertFailed(user1Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename column
        assertFailed(user1Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename table
        assertFailed(user1Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename index
        user1Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user1Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        user2Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user2Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);

    }

    @Test
    public void testCreateTriggerPrivileges() throws Exception {
        //REVOKE ALL to start
        PreparedStatement ps = adminConn.prepareStatement( format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        ps.execute();
        //GRANT
        ps = adminConn.prepareStatement( format("GRANT TRIGGER ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        ps.execute();
        //SELECT
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        PreparedStatement trigger = user1Conn.prepareStatement(triggerQuery);
        trigger.execute();
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //FOREIGN KEYS
        assertFailed(user2Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //create table
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //drop table
        assertFailed(user1Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //modify table
        assertFailed(user1Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //truncate table
        assertFailed(user1Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //pin table
        assertFailed(user1Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //unpin table
        assertFailed(user1Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename column
        assertFailed(user1Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename table
        assertFailed(user1Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename index
        user1Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user1Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        user2Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user2Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test @Ignore
    public void testReferencesPrivileges() throws Exception {
        //REVOKE ALL to start
        PreparedStatement ps = adminConn.prepareStatement( format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        ps.execute();
        //GRANT
        ps = adminConn.prepareStatement( format("GRANT REFERENCES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        ps.execute();
        //SELECT
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //FOREIGN KEYS
        PreparedStatement foreignKeys = user1Conn.prepareStatement(foreignKeyQuery);
        foreignKeys.execute();
        assertFailed(user2Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //create table
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //drop table
        assertFailed(user1Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //modify table
        assertFailed(user1Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //truncate table
        assertFailed(user1Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //pin table
        assertFailed(user1Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //unpin table
        assertFailed(user1Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename column
        assertFailed(user1Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename table
        assertFailed(user1Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename index
        user1Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user1Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        user2Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user2Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    public void testModifySchemaPrivilege() throws  Exception {
        //REVOKE ALL to start
        PreparedStatement ps = adminConn.prepareStatement( format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        ps.execute();
        //GRANT MODIFY
        //GRANT INSERT/SELECT so that we can check the content of the table
        adminConn.prepareStatement( format("GRANT MODIFY, SELECT, INSERT ON SCHEMA %s TO %s", SCHEMA1,USER1  ) ).execute();

        //User1 can now create/truncate/modify/drop table in schema1
        // create table
        user1Conn.prepareStatement(format("create table %s.FOO(col1 int, col2 int, col3 int)", SCHEMA1)).execute();
        // create index
        user1Conn.prepareStatement(format("create index %s.ix_FOO on FOO(col1, col2)", SCHEMA1)).execute();

        // insert rows to the table
        user1Conn.prepareStatement(format("insert into %s.FOO values (1,1,1), (2,2,2), (3,3,3)", SCHEMA1)).execute();

        // select from the table
        String expected = "COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  1  |  1  |  1  |\n" +
                "  2  |  2  |  2  |\n" +
                "  3  |  3  |  3  |";
        ps =  user1Conn.prepareStatement(format("select * from %s.FOO ", SCHEMA1));
        ResultSet rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //alter table
        user1Conn.prepareStatement(format("alter table %s.FOO add column added_col4 int", SCHEMA1)).execute();

        // pin table
        user1Conn.prepareStatement(format("PIN table %s.FOO", SCHEMA1)).execute();

        // rename column
        user1Conn.prepareStatement(format("rename column %s.FOO.col1 to col1_new", SCHEMA1)).execute();

        // rename index
        user1Conn.execute(format("set schema %s", SCHEMA1));
        user1Conn.prepareStatement("rename index ix_FOO to ix_Foo_new").execute();

        // rename table
        user1Conn.prepareStatement(format("rename table %s.FOO to Foo_new", SCHEMA1)).execute();

        // check the content
        expected = "COL1_NEW |COL2 |COL3 |ADDED_COL4 |\n" +
                "----------------------------------\n" +
                "    1    |  1  |  1  |   NULL    |\n" +
                "    2    |  2  |  2  |   NULL    |\n" +
                "    3    |  3  |  3  |   NULL    |";
        ps =  user1Conn.prepareStatement(format("select * from %s.FOO_new --splice-properties index=ix_foo_new", SCHEMA1));
        rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // unpin table
        user1Conn.prepareStatement(format("unpin table %s.FOO_new", SCHEMA1)).execute();

        // truncate table
        user1Conn.prepareStatement(format("truncate table %s.FOO_new", SCHEMA1)).execute();

        //check the content - should be empty after truncation
        expected = "";
        ps =  user1Conn.prepareStatement(format("select * from %s.FOO_new ", SCHEMA1));
        rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // drop the table
        user1Conn.prepareStatement(format("drop table %s.FOO_new ", SCHEMA1));
    }

    @Test
    public void testRevokeModifySchemaPrivilege() throws  Exception {
        //GRANT
        adminConn.createStatement().execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        adminConn.createStatement().execute( format("REVOKE MODIFY ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        //SELECT
        user1Conn.createStatement().execute(selectQuery );
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //create table
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //drop table
        assertFailed(user1Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //modify table
        assertFailed(user1Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //truncate table
        assertFailed(user1Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //pin table
        assertFailed(user1Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //unpin table
        assertFailed(user1Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename column
        assertFailed(user1Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename table
        assertFailed(user1Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename index
        user1Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user1Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        user2Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user2Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    public void testRevokeSelectPrivileges() throws Exception {
        //GRANT
        adminConn.createStatement().execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        adminConn.createStatement().execute( format("REVOKE SELECT ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        //SELECT
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE
        user1Conn.createStatement().execute(updateQuery );
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        user1Conn.createStatement().execute(insertQuery );
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        user1Conn.createStatement().execute( deleteQuery );
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        user1Conn.createStatement().execute(triggerQuery);
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
    }

    @Test
    public void testRevokeUpdatePrivileges() throws Exception {
        //GRANT
        adminConn.createStatement().execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        adminConn.createStatement().execute( format("REVOKE UPDATE ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        //SELECT
        user1Conn.createStatement().execute(selectQuery );
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE
        assertFailed(user1Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        user1Conn.createStatement().execute(insertQuery );
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        user1Conn.createStatement().execute( deleteQuery );
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        user1Conn.createStatement().execute(triggerQuery);
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
    }

    @Test
    public void testRevokeInsertPrivileges() throws Exception {
        //GRANT
        adminConn.createStatement().execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        adminConn.createStatement().execute( format("REVOKE INSERT ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        //SELECT
        user1Conn.createStatement().execute(selectQuery );
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE
        user1Conn.createStatement().execute(updateQuery );
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        user1Conn.createStatement().execute( deleteQuery );
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        user1Conn.createStatement().execute(triggerQuery);
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
    }


    @Test
    public void testRevokeDeletePrivileges() throws Exception {
        //GRANT
        adminConn.createStatement().execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        adminConn.createStatement().execute( format("REVOKE DELETE ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        //SELECT
        user1Conn.createStatement().execute(selectQuery );
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE
        user1Conn.createStatement().execute(updateQuery );
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        user1Conn.createStatement().execute(insertQuery );
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        assertFailed(user1Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        user1Conn.createStatement().execute(triggerQuery);
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
    }

    @Test
    public void testRevokeTriggersPrivileges() throws Exception {
        //GRANT
        adminConn.createStatement().execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        adminConn.createStatement().execute( format("REVOKE DELETE ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        //SELECT
        user1Conn.createStatement().execute(selectQuery );
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE
        user1Conn.createStatement().execute(updateQuery );
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        user1Conn.createStatement().execute(insertQuery );
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        assertFailed(user1Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        user1Conn.createStatement().execute(triggerQuery);
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
    }

    @Test
    public void testPreserveOwnerPrivileges() throws Exception {
       user1Conn.createStatement().execute(format("CREATE TABLE  %s.%s(a int)",USER1,"E") );
       assertFailed(adminConn, format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s",USER1,USER1), SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED );
       user1Conn.createStatement().execute(format("SELECT * FROM %s.%s",USER1,"E" ) );
    }

    @Test
    public void testPreserveOwnerPrivilegeForModify() throws Exception {
        user1Conn.createStatement().execute(format("CREATE TABLE  %s.%s(a int)",USER1,"F") );
        assertFailed(adminConn, format("REVOKE MODIFY ON SCHEMA %s FROM %s",USER1,USER1), SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED );
        user1Conn.createStatement().execute(format("DROP TABLE %s.%s",USER1,"F" ) );
    }

    @Test
    public void testOverwriteTablePrivileges() throws Exception {
        adminConn.createStatement().execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        adminConn.createStatement().execute( format("GRANT SELECT ON TABLE %s.%s TO %s", SCHEMA1,TABLE,USER1  ) );
        //SELECT
        ResultSet rs = user1Conn.query(selectQuery );
        assertEquals("Expected to be have SELECT privileges", 1, resultSetSize(rs));
        //UPDATE
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
    }

    @Test
    public void testReadOnlyView() throws Exception {
        adminConn.createStatement().execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        adminConn.createStatement().execute( format("CREATE VIEW  %s.V as select * from %s.%s", SCHEMA1, SCHEMA1,TABLE  ) );
        String viewQuery = format("update %s.%s set a = 1", SCHEMA1, "V");
        assertFailed(user2Conn, viewQuery, SQLState.LANG_VIEW_NOT_UPDATEABLE);
        assertFailed(user1Conn, viewQuery, SQLState.LANG_VIEW_NOT_UPDATEABLE);
    }





    @Test
    public void testCreateSelectTablePrivileges() throws Exception {
        //GRANT
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT SELECT ON TABLE %s.%s  TO %s", SCHEMA1,TABLE,USER1  ) );
        ps.execute();
        //SELECT
        ResultSet rs = user1Conn.query(selectQuery );
        assertEquals("Expected to be have SELECT privileges", 1, resultSetSize(rs));
        //UPDATE
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //FOREIGN KEYS
        assertFailed(user2Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
    }


    @Test
    public void testCreateDeleteTablePrivileges() throws Exception {
        //GRANT
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT DELETE ON TABLE %s.%s TO %s", SCHEMA1, TABLE, USER1  ) );
        ps.execute();
        //SELECT
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        PreparedStatement delete = user1Conn.prepareStatement(deleteQuery);
        delete.execute();
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //FOREIGN KEYS
        assertFailed(user2Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);


    }

    @Test
    public void testCreateDeleteTablePrivilegesNoToken() throws Exception {
        //GRANT
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT DELETE ON  %s.%s TO %s", SCHEMA1, TABLE, USER1  ) );
        ps.execute();
        //SELECT
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        PreparedStatement delete = user1Conn.prepareStatement(deleteQuery);
        delete.execute();
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //FOREIGN KEYS
        assertFailed(user2Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);


    }

    @Test
    public void testCreateSelectTablePrivilegesNoToken() throws Exception {
        //GRANT
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT SELECT ON TABLE %s.%s  TO %s", SCHEMA1,TABLE,USER1  ) );
        ps.execute();
        //SELECT
        ResultSet rs = user1Conn.query(selectQuery );
        assertEquals("Expected to be have SELECT privileges", 1, resultSetSize(rs));
        //UPDATE
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //FOREIGN KEYS
        assertFailed(user2Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);


    }

    @Test
    public void testCreateAllSchemaPrivilegesToRole() throws Exception {
        ResultSet rs = null;
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,ROLE1  ) );
        ps.execute();
        //revoke all privilege on user1
        adminConn.execute(format("REVOKE ALL PRIVILEGES ON SCHEMA %s from %s", SCHEMA1, USER1));

        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);

        //grant role to user1
        adminConn.execute( format("GRANT  %s TO %s ", ROLE1,USER1) );
        user1Conn.execute( format("SET ROLE %s  ", ROLE1) );
        String query = format("SELECT a from %s.%s where a=1", SCHEMA1,TABLE);
        rs = user1Conn.query( format("SELECT a from %s.%s where a=1", SCHEMA1,TABLE) );
        assertEquals("Expected to be have all privileges", 1, resultSetSize(rs));
        assertFailed(user2Conn, query, SQLState.AUTH_NO_COLUMN_PERMISSION);

        //create/drop table privilege would work with grant all privileges
        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        user1Conn.execute(createTableQuery);


        query = format("drop table %s.FOO", SCHEMA1);
        assertFailed(user2Conn, query, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        PreparedStatement dropTable = user1Conn.prepareStatement(query);
        dropTable.execute();

    }

    @Test
    public void testCreateSelectSchemaPrivilegesToRole() throws Exception {
        //Revoke all privileges first
        adminConn.prepareStatement( format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s", SCHEMA1,ROLE1  ) ).execute();
        //GRANT
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT SELECT ON SCHEMA %s TO %s", SCHEMA1,ROLE1  ) );
        ps.execute();

        //revoke all privilege on user1
        adminConn.execute(format("REVOKE ALL PRIVILEGES ON SCHEMA %s from %s", SCHEMA1, USER1));

        // grant role1 to user1
        adminConn.execute( format("GRANT  %s TO %s ", ROLE1,USER1) );
        user1Conn.execute( format("SET ROLE %s  ", ROLE1) );
        //SELECT
        ResultSet rs = user1Conn.query(selectQuery );
        assertEquals("Expected to be have SELECT privileges", 1, resultSetSize(rs));
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //UPDATE
        assertFailed(user2Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, updateQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //INSERT
        assertFailed(user2Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, insertQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //DELETE
        assertFailed(user2Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, deleteQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //TRIGGERS
        assertFailed(user2Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        assertFailed(user1Conn, triggerQuery, SQLState.AUTH_NO_TABLE_PERMISSION);
        //FOREIGN KEYS
        assertFailed(user2Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user1Conn, foreignKeyQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);


    }

    @Test
    public void testGrantModifySchemaPrivilegeToRole() throws  Exception {
        //Revoke all privileges on role1 to start
        adminConn.prepareStatement( format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s", SCHEMA1,ROLE1  ) ).execute();
        //GRANT modifiy to role1, in addition, grant select, update to faciliate the subsequent test
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT MODIFY, SELECT, INSERT ON SCHEMA %s TO %s", SCHEMA1,ROLE1  ) );
        ps.execute();

        //revoke all privilege on user1
        adminConn.execute(format("REVOKE ALL PRIVILEGES ON SCHEMA %s from %s", SCHEMA1, USER1));

        //no right to modify schema now
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);

        // grant role1 to user1
        adminConn.execute( format("GRANT  %s TO %s ", ROLE1,USER1) );
        user1Conn.execute( format("SET ROLE %s  ", ROLE1) );

        //user2 still can't modify schema1
        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        // But User1 can now create/truncate/modify/drop table in schema1
        // create table
        user1Conn.prepareStatement(format("create table %s.FOO(col1 int, col2 int, col3 int)", SCHEMA1)).execute();
        // create index
        user1Conn.prepareStatement(format("create index %s.ix_FOO on FOO(col1, col2)", SCHEMA1)).execute();

        // insert rows to the table
        user1Conn.prepareStatement(format("insert into %s.FOO values (1,1,1), (2,2,2), (3,3,3)", SCHEMA1)).execute();

        // select from the table
        String expected = "COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  1  |  1  |  1  |\n" +
                "  2  |  2  |  2  |\n" +
                "  3  |  3  |  3  |";
        ps =  user1Conn.prepareStatement(format("select * from %s.FOO ", SCHEMA1));
        ResultSet rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //alter table
        user1Conn.prepareStatement(format("alter table %s.FOO add column added_col4 int", SCHEMA1)).execute();

        // pin table
        user1Conn.prepareStatement(format("PIN table %s.FOO", SCHEMA1)).execute();

        // rename column
        user1Conn.prepareStatement(format("rename column %s.FOO.col1 to col1_new", SCHEMA1)).execute();

        // rename index
        user1Conn.execute(format("set schema %s", SCHEMA1));
        user1Conn.prepareStatement("rename index ix_FOO to ix_Foo_new").execute();

        // rename table
        user1Conn.prepareStatement(format("rename table %s.FOO to Foo_new", SCHEMA1)).execute();

        // check the content
        expected = "COL1_NEW |COL2 |COL3 |ADDED_COL4 |\n" +
                "----------------------------------\n" +
                "    1    |  1  |  1  |   NULL    |\n" +
                "    2    |  2  |  2  |   NULL    |\n" +
                "    3    |  3  |  3  |   NULL    |";
        ps =  user1Conn.prepareStatement(format("select * from %s.FOO_new --splice-properties index=ix_foo_new", SCHEMA1));
        rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // unpin table
        user1Conn.prepareStatement(format("unpin table %s.FOO_new", SCHEMA1)).execute();

        // truncate table
        user1Conn.prepareStatement(format("truncate table %s.FOO_new", SCHEMA1)).execute();

        //check the content - should be empty after truncation
        expected = "";
        ps =  user1Conn.prepareStatement(format("select * from %s.FOO_new ", SCHEMA1));
        rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // drop the table
        user1Conn.prepareStatement(format("drop table %s.FOO_new ", SCHEMA1)).execute();
    }

    @Test
    public void testRevokeModifySchemaPrivilegeToRole() throws  Exception {
        //REVOKE MODIFY ON ROLE1 and USER1
        adminConn.createStatement().execute( format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s", SCHEMA1,USER1  ) );
        adminConn.createStatement().execute( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,ROLE1  ) );
        adminConn.createStatement().execute( format("REVOKE MODIFY ON SCHEMA %s FROM %s", SCHEMA1,ROLE1  ) );

        // grant role1 to user1
        adminConn.execute( format("GRANT  %s TO %s ", ROLE1,USER1) );
        user1Conn.execute( format("SET ROLE %s  ", ROLE1) );

        //SELECT
        user1Conn.createStatement().execute(selectQuery );
        assertFailed(user2Conn, selectQuery, SQLState.AUTH_NO_COLUMN_PERMISSION);
        //create table
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //drop table
        assertFailed(user1Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, dropTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //modify table
        assertFailed(user1Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, modifyTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //truncate table
        assertFailed(user1Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, truncateTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //pin table
        assertFailed(user1Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, pinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //unpin table
        assertFailed(user1Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, unpinTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename column
        assertFailed(user1Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameColumnQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename table
        assertFailed(user1Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        assertFailed(user2Conn, renameTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        //rename index
        user1Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user1Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        user2Conn.execute(format("set schema %s", SCHEMA1));
        assertFailed(user2Conn, renameIndexQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    public void testCreateAllPrivilegesToUserSchema() throws Exception {
        ResultSet rs = null;
        PreparedStatement ps = user3Conn.prepareStatement( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SECOND_SCHEMA,USER1  ) );
        ps.execute();
        String query = format("SELECT a from %s.%s where a=1", SECOND_SCHEMA,TABLE);
        rs = user1Conn.query(query );
        assertEquals("Expected to be have all privileges", 1, resultSetSize(rs));
        assertFailed(user2Conn, query, SQLState.AUTH_NO_COLUMN_PERMISSION);

    }

    @Test
    public void testCreateAllPrivilegesToUserSchemaNotAllowed() throws Exception {
        String query =  format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SECOND_SCHEMA,USER2  );
        assertFailed(user2Conn, query, SQLState.AUTH_NOT_OWNER);

    }

    @Test
    public void testOwnerGrantRevokeModifySchemaPrivilegeToOtherUser() throws Exception {
        // create a new table in user3's own schema
        user3Conn.execute(format("create table %s.user3_table(a3 int, b3 int, c3 int)", USER3));

        //revoke all privileges on USER1
        adminConn.execute(format("REVOKE ALL PRIVILEGES ON SCHEMA %s from %s", USER3, USER1));

        // user3 can grant modify for his own schema to other user
        user3Conn.execute(format("GRANT MODIFY, SELECT, INSERT on SCHEMA %s to %s", USER3, USER1));

        //user2 still can't modify user3's schema
        assertFailed(user2Conn, format("drop table %s.user3_table", USER3), SQLState.AUTH_NO_ACCESS_NOT_OWNER);
        // But User1 can now create/truncate/modify/drop table in user3's schema
        user1Conn.prepareStatement(format("drop table %s.user3_table", USER3)).execute();

        // more ddl/dml operations
        // create table
        user1Conn.prepareStatement(format("create table %s.FOO(col1 int, col2 int, col3 int)", USER3)).execute();
        // create index
        user1Conn.prepareStatement(format("create index %s.ix_FOO on FOO(col1, col2)", USER3)).execute();

        // insert rows to the table
        user1Conn.prepareStatement(format("insert into %s.FOO values (1,1,1), (2,2,2), (3,3,3)", USER3)).execute();

        // select from the table
        String expected = "COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  1  |  1  |  1  |\n" +
                "  2  |  2  |  2  |\n" +
                "  3  |  3  |  3  |";
        PreparedStatement ps =  user1Conn.prepareStatement(format("select * from %s.FOO ", USER3));
        ResultSet rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //alter table
        user1Conn.prepareStatement(format("alter table %s.FOO add column added_col4 int", USER3)).execute();

        // pin table
        user1Conn.prepareStatement(format("PIN table %s.FOO", USER3)).execute();

        // rename column
        user1Conn.prepareStatement(format("rename column %s.FOO.col1 to col1_new", USER3)).execute();

        // rename index
        user1Conn.execute(format("set schema %s", USER3));
        user1Conn.prepareStatement("rename index ix_FOO to ix_Foo_new").execute();

        // rename table
        user1Conn.prepareStatement(format("rename table %s.FOO to Foo_new", USER3)).execute();

        // check the content
        expected = "COL1_NEW |COL2 |COL3 |ADDED_COL4 |\n" +
                "----------------------------------\n" +
                "    1    |  1  |  1  |   NULL    |\n" +
                "    2    |  2  |  2  |   NULL    |\n" +
                "    3    |  3  |  3  |   NULL    |";
        ps =  user1Conn.prepareStatement(format("select * from %s.FOO_new --splice-properties index=ix_foo_new", USER3));
        rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // unpin table
        user1Conn.prepareStatement(format("unpin table %s.FOO_new", USER3)).execute();

        // truncate table
        user1Conn.prepareStatement(format("truncate table %s.FOO_new", USER3)).execute();

        //check the content - should be empty after truncation
        expected = "";
        ps =  user1Conn.prepareStatement(format("select * from %s.FOO_new ", USER3));
        rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // drop the table
        user1Conn.prepareStatement(format("drop table %s.FOO_new ", USER3)).execute();

        // test  revoke
        user3Conn.execute(format("REVOKE MODIFY on SCHEMA %s from %s", USER3, USER1));
        assertFailed(user1Conn, format("create table %s.FOO(col1 int, col2 int, col3 int)", USER3), SQLState.AUTH_NO_ACCESS_NOT_OWNER);
    }

    @Test
    public void testOwnerGrantRevokeModifySchemaPrivilegeToRole() throws Exception {
        // create a new table in user3's own schema
        user3Conn.execute(format("create table %s.user3_table(a3 int, b3 int, c3 int)", USER3));

        //revoke all privileges on role1
        adminConn.execute(format("REVOKE ALL PRIVILEGES ON SCHEMA %s from %s", USER3, ROLE1));

        // user3 can grant modify for his own schema to other role
        user3Conn.execute(format("GRANT MODIFY, SELECT, INSERT on SCHEMA %s to %s", USER3, ROLE1));

        //revoke all privilege on user1
        adminConn.execute(format("REVOKE ALL PRIVILEGES ON SCHEMA %s from %s", USER3, USER1));

        //no right to modify schema now
        assertFailed(user1Conn, format("drop table %s.user3_table", USER3), SQLState.AUTH_NO_ACCESS_NOT_OWNER);

        // grant role1 to user1
        adminConn.execute( format("GRANT  %s TO %s ", ROLE1,USER1) );
        user1Conn.execute( format("SET ROLE %s  ", ROLE1) );

        // User1 can now create/truncate/modify/drop table in User3's schema
        user1Conn.prepareStatement(format("drop table %s.user3_table", USER3)).execute();

        // more ddl/dml operations
        // create table
        user1Conn.prepareStatement(format("create table %s.FOO(col1 int, col2 int, col3 int)", USER3)).execute();
        // create index
        user1Conn.prepareStatement(format("create index %s.ix_FOO on FOO(col1, col2)", USER3)).execute();

        // insert rows to the table
        user1Conn.prepareStatement(format("insert into %s.FOO values (1,1,1), (2,2,2), (3,3,3)", USER3)).execute();

        // select from the table
        String expected = "COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  1  |  1  |  1  |\n" +
                "  2  |  2  |  2  |\n" +
                "  3  |  3  |  3  |";
        PreparedStatement ps =  user1Conn.prepareStatement(format("select * from %s.FOO ", USER3));
        ResultSet rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //alter table
        user1Conn.prepareStatement(format("alter table %s.FOO add column added_col4 int", USER3)).execute();

        // pin table
        user1Conn.prepareStatement(format("PIN table %s.FOO", USER3)).execute();

        // rename column
        user1Conn.prepareStatement(format("rename column %s.FOO.col1 to col1_new", USER3)).execute();

        // rename index
        user1Conn.execute(format("set schema %s", USER3));
        user1Conn.prepareStatement("rename index ix_FOO to ix_Foo_new").execute();

        // rename table
        user1Conn.prepareStatement(format("rename table %s.FOO to Foo_new", USER3)).execute();

        // check the content
        expected = "COL1_NEW |COL2 |COL3 |ADDED_COL4 |\n" +
                "----------------------------------\n" +
                "    1    |  1  |  1  |   NULL    |\n" +
                "    2    |  2  |  2  |   NULL    |\n" +
                "    3    |  3  |  3  |   NULL    |";
        ps =  user1Conn.prepareStatement(format("select * from %s.FOO_new --splice-properties index=ix_foo_new", USER3));
        rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // unpin table
        user1Conn.prepareStatement(format("unpin table %s.FOO_new", USER3)).execute();

        // truncate table
        user1Conn.prepareStatement(format("truncate table %s.FOO_new", USER3)).execute();

        //check the content - should be empty after truncation
        expected = "";
        ps =  user1Conn.prepareStatement(format("select * from %s.FOO_new ", USER3));
        rs = ps.executeQuery();
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // drop the table
        user1Conn.prepareStatement(format("drop table %s.FOO_new ", USER3)).execute();

        // test  revoke
        user3Conn.execute(format("REVOKE MODIFY on SCHEMA %s from %s", USER3, ROLE1));
        assertFailed(user1Conn, format("create table %s.FOO(col1 int, col2 int, col3 int)", USER3), SQLState.AUTH_NO_ACCESS_NOT_OWNER);

    }

    @Test
    public void testRoleInheritModifySchemaPrivilege() throws Exception {
        //Revoke all privileges on role1 & role2 to start
        adminConn.prepareStatement( format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s", SCHEMA1,ROLE1  ) ).execute();
        adminConn.prepareStatement( format("REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s", SCHEMA1,ROLE2  ) ).execute();

        //GRANT modifiy to role1, in addition, grant select, update to faciliate the subsequent test
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT MODIFY, SELECT, INSERT ON SCHEMA %s TO %s", SCHEMA1,ROLE1  ) );
        ps.execute();
        adminConn.prepareStatement(format("GRANT %s TO %s", ROLE1, ROLE2 )).execute();

        //revoke all privilege on user1
        adminConn.execute(format("REVOKE ALL PRIVILEGES ON SCHEMA %s from %s", SCHEMA1, USER1));

        //no right to modify schema now
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);

        // grant role2 to user1
        adminConn.execute( format("GRANT  %s TO %s ", ROLE2, USER1) );
        user1Conn.execute( format("SET ROLE %s  ", ROLE2) );

        // User1 can now create/truncate/modify/drop table in schema1
        user1Conn.prepareStatement(format("create table %s.FOO(col1 int, col2 int, col3 int)", SCHEMA1)).execute();
        user1Conn.prepareStatement(format("drop table %s.FOO", SCHEMA1)).execute();

        //revoke the nested role
        adminConn.prepareStatement(format("REVOKE %s FROM %s", ROLE1, ROLE2 )).execute();

        //User1 no longer have modify privilege on schema1
        assertFailed(user1Conn, createTableQuery, SQLState.AUTH_NO_ACCESS_NOT_OWNER);

    }

    @Test
    public void testGrantModifySystemSchemaNotAllowed() throws Exception {
        String query =  format("GRANT MODIFY ON SCHEMA SYS TO %s",  USER2  );
        assertFailed(adminConn, query, SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED);
        query =  format("REVOKE MODIFY ON SCHEMA SYS FROM %s",  USER2  );
        assertFailed(adminConn, query, SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED);
    }

    @Test
    public void testGrantModifyTablePrivilegeNotAllowed() throws Exception {
        String query =  format("GRANT MODIFY ON TABLE %s.%s TO %s", SECOND_SCHEMA, TABLE, USER2  );
        assertFailed(user2Conn, query, SQLState.AUTH_NOT_VALID_TABLE_PRIVILEGE);
        query =  format("REVOKE MODIFY ON TABLE %s.%s FROM %s", SECOND_SCHEMA, TABLE, USER2  );
        assertFailed(user2Conn, query, SQLState.AUTH_NOT_VALID_TABLE_PRIVILEGE);
    }

    @Test
    public void testCreateAllPrivilegesToUserTable() throws Exception {
        ResultSet rs = null;
        PreparedStatement ps = user3Conn.prepareStatement( format("GRANT ALL PRIVILEGES ON TABLE %s.%s TO %s", SECOND_SCHEMA,TABLE,USER1  ) );
        ps.execute();
        String query = format("SELECT a from %s.%s where a=1", SECOND_SCHEMA,TABLE);
        rs = user1Conn.query(query );
        assertEquals("Expected to be have all privileges", 1, resultSetSize(rs));
        assertFailed(user2Conn, query, SQLState.AUTH_NO_COLUMN_PERMISSION);

    }

    @Test
    public void testCreateAllPrivilegesToUserTableNotAllowed() throws Exception {
        String query =  format("GRANT ALL PRIVILEGES ON TABLE %s.%s TO %s", SECOND_SCHEMA,TABLE,USER2  );
        assertFailed(user2Conn, query, SQLState.AUTH_NOT_OWNER);

    }

    @Test
    @Category(HBaseTest.class)
    public void testUpdateSchemaOwner() throws Exception {
        ResultSet rs = null;
        // step 1: user 3 is owner of schema3, so it can select from schema3 from conn3, but user1 and user2 cannot
        String query = format("SELECT * from %s.%s", THIRD_SCHEMA,TABLE3);
        rs = user3Conn.query(query);
        assertEquals("Expected to be have all privileges", 1, resultSetSize(rs));
        rs.close();
        assertFailed(user1Conn, query, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user2Conn, query, SQLState.AUTH_NO_COLUMN_PERMISSION);

        // step 1-2: select on another region server, , it should behave the same
        rs = user3Conn2.query(query);
        assertEquals("Expected to be have all privileges", 1, resultSetSize(rs));
        rs.close();
        assertFailed(user1Conn2, query, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user2Conn2, query, SQLState.AUTH_NO_COLUMN_PERMISSION);

        // step 2: change the owner of schema3 to user1
        String query2 = format("CALL SYSCS_UTIL.SYSCS_UPDATE_SCHEMA_OWNER('%s','%s')", THIRD_SCHEMA, USER1);
        adminConn.execute(query2);

        // step 3: user1 can now select from schema3, but user3 can no longer select, user2 still cannot select
        rs = user1Conn.query(query);
        assertEquals("Expected to be have all privileges", 1, resultSetSize(rs));
        rs.close();
        assertFailed(user3Conn, query, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user2Conn, query, SQLState.AUTH_NO_COLUMN_PERMISSION);

        // step 3-2: select on another region server, it should behave the same
        rs = user1Conn2.query(query);
        assertEquals("Expected to be have all privileges", 1, resultSetSize(rs));
        rs.close();
        assertFailed(user3Conn2, query, SQLState.AUTH_NO_COLUMN_PERMISSION);
        assertFailed(user2Conn2, query, SQLState.AUTH_NO_COLUMN_PERMISSION);
    }
}
