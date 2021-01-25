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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.*;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Tests around creating schemas
 */
public class MultiDatabaseIT extends SpliceUnitTest {
    protected static SpliceWatcher classWatcher = new SpliceWatcher();
    private static final Logger LOG = Logger.getLogger(MultiDatabaseIT.class);
    private static String OTHER_DB = MultiDatabaseIT.class.getSimpleName().toUpperCase();
    private static String OTHER_DB_OWNER_NOT_SPLICE = MultiDatabaseIT.class.getSimpleName().toUpperCase() + "2";
    private static String SCHEMA = MultiDatabaseIT.class.getSimpleName().toUpperCase();
    private static String TABLE = MultiDatabaseIT.class.getSimpleName().toUpperCase();
    private static String ROLE = "ROLE_" + MultiDatabaseIT.class.getSimpleName().toUpperCase();
    private static String USER = "USER_" + MultiDatabaseIT.class.getSimpleName().toUpperCase();
    private static String SEQUENCE = "SEQUENCE_" + MultiDatabaseIT.class.getSimpleName().toUpperCase();


    protected static SpliceDatabaseWatcher otherDbOwnerNotSpliceWatcher = new SpliceDatabaseWatcher(OTHER_DB_OWNER_NOT_SPLICE, OTHER_DB_OWNER_NOT_SPLICE);
    protected static SpliceDatabaseWatcher otherDbWatcher = new SpliceDatabaseWatcher(OTHER_DB);
    protected static SpliceSchemaWatcher spliceDbSchemaWatcher = new SpliceSchemaWatcher(null, SCHEMA);
    protected static SpliceSchemaWatcher otherDbSchemaWatcher = new SpliceSchemaWatcher(OTHER_DB, SCHEMA);
    protected static SpliceSchemaWatcher otherDbOwnerNotSpliceSchemaWatcher = new SpliceSchemaWatcher(OTHER_DB_OWNER_NOT_SPLICE, SCHEMA, OTHER_DB_OWNER_NOT_SPLICE);

    protected static TestConnection spliceDbConn;
    protected static TestConnection otherDbConn;
    protected static TestConnection otherDbOwnerNotSpliceConn;
    protected static TestConnection[] connections;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(otherDbWatcher)
            .around(otherDbOwnerNotSpliceWatcher)
            .around(spliceDbSchemaWatcher)
            .around(otherDbSchemaWatcher)
            .around(otherDbOwnerNotSpliceSchemaWatcher);
            ;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Before
    public void setUp() throws Exception {
        spliceDbConn = methodWatcher.createConnection();
        otherDbConn = methodWatcher.connectionBuilder().database(OTHER_DB).build();
        otherDbOwnerNotSpliceConn = methodWatcher.connectionBuilder().database(OTHER_DB_OWNER_NOT_SPLICE).user(OTHER_DB_OWNER_NOT_SPLICE).build();
        connections = new TestConnection[]{spliceDbConn, otherDbConn, otherDbOwnerNotSpliceConn};
    }

    private interface RunnableOnConnection {
        void run(TestConnection conn) throws SQLException;
    }

    private void runEverywhere(RunnableOnConnection func) throws SQLException {
        for (TestConnection conn: connections) {
            func.run(conn);
        }
    }

    private void executeSqlEverywhere(String sql) throws SQLException {
        runEverywhere(conn -> conn.execute(sql));
    }

    private void executeSqlEverywhere(String sql, Object... parameters) throws SQLException {
        executeSqlEverywhere(String.format(sql, parameters));
    }

    @Test
    public void testCurrentDatabase() throws SQLException {
        checkStringExpression("current server", "SPLICEDB", spliceDbConn);
        checkStringExpression("current server", OTHER_DB, otherDbConn);
        checkStringExpression("current server", OTHER_DB_OWNER_NOT_SPLICE, otherDbOwnerNotSpliceConn);
    }

    @Test
    public void testCurrentDatabaseAdmin() throws SQLException {
        checkStringExpression("current database admin", "SPLICE", spliceDbConn);
        checkStringExpression("current database admin", "SPLICE", otherDbConn);
        checkStringExpression("current database admin", OTHER_DB_OWNER_NOT_SPLICE, otherDbOwnerNotSpliceConn);
    }

    @Test
    public void testSysDatabases() throws SQLException {
        try (ResultSet rs = spliceDbConn.query(String.format(
                "select databasename, databaseid from sys.sysdatabases where databasename in ('SPLICEDB', '%s') order by 1", OTHER_DB))) {
            rs.next();
            assertEquals(OTHER_DB, rs.getString(1));
            String uuid1 = rs.getString(2);
            rs.next();
            assertEquals("SPLICEDB", rs.getString(1));
            String uuid2 = rs.getString(2);
            assertNotEquals(uuid1, uuid2);
        }
    }

    @Test
    public void testSysViewSchema() throws SQLException {
        Set<String> spliceSchemaUuids = new HashSet<>();
        for (TestConnection conn: new TestConnection[] {spliceDbConn, otherDbConn}) {
            try (ResultSet rs = conn.query("select schemaname, schemaid from sysvw.sysschemasview where schemaname = 'SPLICE'")) {
                assertTrue(rs.next());
                assertEquals("SPLICE", rs.getString(1));
                spliceSchemaUuids.add(rs.getString(2));
                assertFalse(rs.next());
            }
        }
        assertEquals(2, spliceSchemaUuids.size());

        Set<String> multiDatabaseItSchemaUuids = new HashSet<>();
        runEverywhere(conn -> {
            try (ResultSet rs = conn.query(String.format(
                    "select schemaname, schemaid from sysvw.sysschemasview where schemaname = '%s'", SCHEMA))) {
                assertTrue(rs.next());
                assertEquals(SCHEMA, rs.getString(1));
                multiDatabaseItSchemaUuids.add(rs.getString(2));
                assertFalse(rs.next());
            }
        });
        assertEquals(3, multiDatabaseItSchemaUuids.size());
    }

    @Test
    public void testTables() throws SQLException {
        try {
            for (int i = 0; i < connections.length; ++i) {
                TestConnection conn = connections[i];
                conn.createStatement().execute(String.format("create table %s.%s (a int)", SCHEMA, TABLE));
                conn.createStatement().execute(String.format("insert into %s.%s values %s", SCHEMA, TABLE, i + 10));
                conn.createStatement().execute(String.format("create table %s.%s_%s (a%s int)", SCHEMA, TABLE, i, i));
                conn.createStatement().execute(String.format("insert into %s.%s_%s values %s", SCHEMA, TABLE, i, i + 100));
            }
            for (int i = 0; i < connections.length; ++i) {
                TestConnection conn = connections[i];
                try (ResultSet rs = conn.query(String.format("select * from %s.%s", SCHEMA, TABLE))) {
                    rs.next();
                    assertEquals(10 + i, rs.getInt(1));
                }
                try (ResultSet rs = conn.query(String.format("select * from %s.%s_%s", SCHEMA, TABLE, i))) {
                    rs.next();
                    assertEquals(100 + i, rs.getInt(1));
                }
                try (ResultSet rs = conn.query(String.format(
                        "select count(*) from sysibm.systables where name in ('%s', '%s_%s')", TABLE, TABLE, i))) {
                    rs.next();
                    assertEquals(2, rs.getInt(1));
                }
                try (ResultSet rs = conn.query(String.format(
                        "select name, tbname from sysibm.syscolumns where tbname in ('%s', '%s_%s') order by tbname", TABLE, TABLE, i))) {
                    rs.next();
                    assertEquals("A", rs.getString(1));
                    assertEquals(TABLE, rs.getString(2));
                    rs.next();
                    assertEquals("A" + i, rs.getString(1));
                    assertEquals(TABLE + "_" + i, rs.getString(2));
                }
            }
        } finally {
            for (int i = 0; i < connections.length; ++i) {
                TestConnection conn = connections[i];
                conn.createStatement().execute(String.format("drop table if exists %s.%s", SCHEMA, TABLE));
                conn.createStatement().execute(String.format("drop table if exists %s.%s_%s", SCHEMA, TABLE, i));
            }
        }
    }

    @Test
    public void testNewDbNoCreate() {
        try {
            classWatcher.connectionBuilder().database(OTHER_DB + "_NO_CREATE").build();
            Assert.fail("An exception should be thrown");
        } catch (SQLException e) {
            Assert.assertEquals("42Y18", e.getSQLState());
        }
    }

    @Test
    public void testDropDatabaseRestrictCascade() throws SQLException {
        String dbName = OTHER_DB + "_TEST_DROP_RESTRICT_CASCADE";
        classWatcher.connectionBuilder().database(dbName).create(true).build();
        try {
            spliceDbConn.createStatement().execute(String.format("drop database %s restrict", dbName));
            Assert.fail("An exception should be thrown");
        } catch (SQLException e) {
            Assert.assertEquals("X0Y53", e.getSQLState());
        }
        spliceDbConn.createStatement().execute(String.format("drop database %s cascade", dbName));
        try (ResultSet rs = spliceDbConn.query(String.format(
                "select databasename from sys.sysdatabases where databasename = '%s'", dbName))) {
            assertFalse(rs.next());
        }
    }

    @Test
    public void testRoles() throws SQLException {
        for (TestConnection conn: connections) {
            conn.execute("CREATE ROLE %s", ROLE);
        }
        for (TestConnection conn: connections) {
            conn.execute("DROP ROLE %s", ROLE);
        }
    }

    @Test
    public void testUsers() throws SQLException {
        executeSqlEverywhere("call syscs_util.syscs_create_user('%s', 'pw')", USER);

        executeSqlEverywhere("create sequence %s", SEQUENCE);
        executeSqlEverywhere("grant usage on sequence %s to %s", SEQUENCE, USER);

        try (ResultSet rs = spliceDbConn.query(
                "select count(*) from sys.sysperms, sys.syssequences sq" +
                        " where grantee = '%s' and objectid = sequenceid and sequencename = '%s'",
                USER, SEQUENCE)) {
            rs.next();
            assertEquals(3, rs.getInt(1));
        }

        runEverywhere(conn -> {
            try (ResultSet rs = conn.query(
                    "select count(*) from sysvw.syspermsview, sysvw.syssequencesview sq" +
                            " where grantee = '%s' and objectid = sequenceid and sequencename = '%s'",
                    USER, SEQUENCE)) {
                rs.next();
                assertEquals(1, rs.getInt(1));
            }
        });

        // XXX also check that lambda splice user cannot execute stuff

        executeSqlEverywhere("drop sequence %s RESTRICT", SEQUENCE);
        executeSqlEverywhere("call syscs_util.syscs_drop_user('%s')", USER);
    }

    @Test
    public void testSysSchemaNotUsableFromOtherDb() {
        assertFailed(otherDbConn, "select * from sys.sysdatabases", "42Y07");
        assertFailed(otherDbOwnerNotSpliceConn, "select * from sys.sysdatabases", "42Y07");
    }
}
