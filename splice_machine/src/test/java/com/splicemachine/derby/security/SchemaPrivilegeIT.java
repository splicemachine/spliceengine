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

package com.splicemachine.derby.security;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.SlowTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.resultSetSize;
import static com.splicemachine.derby.test.framework.SpliceUnitTest.assertFailed;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by jfilali on 8/22/16.
 */
@Category(SlowTest.class)
public class SchemaPrivilegeIT {

    private static final String SCHEMA1 = "MAIN_SCH";
    private static final String SECOND_SCHEMA = "SECOND_SCH";

    private static final String TABLE = "Y";

    protected static final String USER1 = "JOHN";
    protected static final String PASSWORD1 = "jleach";
    protected static final String ROLE1 = "ROLE1";

    protected static final String USER2 = "JIM";
    protected static final String PASSWORD2 = "bo";

    protected static final String USER3 = "SUZY";
    protected static final String PASSWORD3 = "X)X)X";

    private static SpliceWatcher        spliceClassWatcherAdmin = new SpliceWatcher();
    private static SpliceSchemaWatcher  spliceSchemaWatcher1 = new SpliceSchemaWatcher(SCHEMA1);
    private static SpliceSchemaWatcher  spliceSchemaWatcherUser3 = new SpliceSchemaWatcher(SECOND_SCHEMA,USER3);
    private static SpliceUserWatcher    spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);
    private static SpliceUserWatcher    spliceUserWatcher2 = new SpliceUserWatcher(USER2, PASSWORD2);
    private static SpliceUserWatcher    spliceUserWatcher3 = new SpliceUserWatcher(USER3, PASSWORD3);
    private static SpliceRoleWatcher    spliceRoleWatcher1 = new SpliceRoleWatcher(ROLE1);
    private static SpliceTableWatcher   tableWatcher = new SpliceTableWatcher(TABLE, SCHEMA1,"(a int PRIMARY KEY, b int, c int)" );
    private static SpliceTableWatcher   tableWatcherUser3 = new SpliceTableWatcher(TABLE, SECOND_SCHEMA,"(a int PRIMARY KEY, b int, c int)" );

    @Rule
    public  TestRule chain =
            RuleChain.outerRule(spliceClassWatcherAdmin)
            .around(spliceUserWatcher1)
            .around(spliceUserWatcher2)
            .around(spliceUserWatcher3)
            .around(spliceRoleWatcher1)
            .around(spliceSchemaWatcher1)
            .around(spliceSchemaWatcherUser3)
            .around(tableWatcher)
            .around(tableWatcherUser3);

    protected static TestConnection adminConn;
    protected static TestConnection user1Conn;
    protected static TestConnection user2Conn;
    protected static TestConnection user3Conn;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private String selectQuery      = format("select a from %s.%s where a=1", SCHEMA1, TABLE);
    private String updateQuery      = format("update %s.%s set a = 1", SCHEMA1, TABLE);
    private String insertQuery      = format("insert into %s.%s values(12,11,12)", SCHEMA1, TABLE);
    private String deleteQuery      = format("delete from %s.%s ", SCHEMA1, TABLE);
    private String triggerQuery     = format("create trigger test_trigger after insert on %s.%s for each row values 1", SCHEMA1, TABLE);
    private String foreignKeyQuery  = format("CREATE TABLE D( d1 int, d2 int REFERENCES %s.%s(a))", SCHEMA1,TABLE);

    @Before
    public  void setUpClass() throws Exception {
        adminConn = spliceClassWatcherAdmin.createConnection();
        user1Conn = spliceClassWatcherAdmin.createConnection(USER1, PASSWORD1);
        user2Conn = spliceClassWatcherAdmin.createConnection(USER2, PASSWORD2);
        user3Conn = spliceClassWatcherAdmin.createConnection(USER3, PASSWORD3);
        adminConn.execute( format("insert into %s.%s values ( 1, 2, 3)", SCHEMA1, TABLE ) );
        user3Conn.execute( format("insert into %s.%s values ( 1, 2, 3)", SECOND_SCHEMA, TABLE ) );

    }

    @Test
    public void testCreateAllPrivileges() throws Exception {
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT ALL PRIVILEGES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
        ps.execute();
        String query = format("SELECT a from %s.%s where a=1", SCHEMA1,TABLE);
        ResultSet rs = user1Conn.query(query );
        assertEquals("Expected to be have all privileges", 1, resultSetSize(rs));
        assertFailed(user2Conn, query, SQLState.AUTH_NO_COLUMN_PERMISSION);

    }

    @Test
    public void testCreateSelectPrivileges() throws Exception {
        //GRANT
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT SELECT ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
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
    public void testCreateDeletePrivileges() throws Exception {
        //GRANT
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT DELETE ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
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
    public void testCreateUpdatePrivileges() throws Exception {
        //GRANT
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT UPDATE ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
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
    }

    @Test
    public void testCreateTriggerPrivileges() throws Exception {
        //GRANT
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT TRIGGER ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
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

    }

    @Test @Ignore
    public void testReferencesPrivileges() throws Exception {
        //GRANT
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT REFERENCES ON SCHEMA %s TO %s", SCHEMA1,USER1  ) );
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
    }

    @Test
    public void testNoAllowedCreateTable() throws  Exception {
        String createTable =  format("CREATE TABLE %s.O( a int )", SCHEMA1) ;
        assertFailed(user1Conn, createTable, SQLState.AUTH_NO_ACCESS_NOT_OWNER);

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
        adminConn.execute( format("GRANT  %s TO %s ", ROLE1,USER1) );
        user1Conn.execute( format("SET ROLE %s  ", ROLE1) );
        String query = format("SELECT a from %s.%s where a=1", SCHEMA1,TABLE);
        rs = user1Conn.query( format("SELECT a from %s.%s where a=1", SCHEMA1,TABLE) );
        assertEquals("Expected to be have all privileges", 1, resultSetSize(rs));
        assertFailed(user2Conn, query, SQLState.AUTH_NO_COLUMN_PERMISSION);

    }

    @Test
    public void testCreateSelectSchemaPrivilegesToRole() throws Exception {
        //GRANT
        PreparedStatement ps = adminConn.prepareStatement( format("GRANT SELECT ON SCHEMA %s TO %s", SCHEMA1,ROLE1  ) );
        ps.execute();
        adminConn.execute( format("GRANT  %s TO %s ", ROLE1,USER1) );
        user1Conn.execute( format("SET ROLE %s  ", ROLE1) );
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
}
