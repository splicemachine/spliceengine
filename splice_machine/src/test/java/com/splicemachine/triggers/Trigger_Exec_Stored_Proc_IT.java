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

package com.splicemachine.triggers;

import java.io.File;
import java.sql.*;
import java.util.Collection;
import java.util.Properties;

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.test_dao.TriggerDAO;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

/**
 * Tests trigger execution of stored procedures.
 * <p/>
 * Note the dependency on user-defined stored procedures in this test class.<br/>
 * See {@link TriggerProcs} for instructions on adding/modifying store procedures.
 */
@Category({SerialTest.class, LongerThanTwoMinutes.class})
@RunWith(Parameterized.class)
public class Trigger_Exec_Stored_Proc_IT  extends SpliceUnitTest {

    private static final String SCHEMA = Trigger_Exec_Stored_Proc_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    private static final String DERBY_JAR_NAME = SCHEMA + ".TRIGGER_PROCS_JAR";

    private static final String CALL_SET_CLASSPATH_STRING =
        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', %s)";

    private static final String CREATE_PROC =
        "CREATE PROCEDURE "+ SCHEMA+".proc_call_audit(" +
            "in schema_name varchar(30), in table_name varchar(20)) " +
            "PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA " +
            "EXTERNAL NAME 'com.splicemachine.triggers.TriggerProcs.proc_call_audit'";

    private static final String CREATE_PROC_WITH_TRANSITION_VAR =
        "CREATE PROCEDURE "+ SCHEMA+".proc_call_audit_with_transition(" +
            "in schema_name varchar(30), in table_name varchar(20), in new_val integer, in old_val integer) " +
            "PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA " +
            "EXTERNAL NAME 'com.splicemachine.triggers.TriggerProcs.proc_call_audit_with_transition'";

    private static final String CREATE_PROC_WITH_RESULT =
        "CREATE PROCEDURE "+SCHEMA+".proc_call_audit_with_result(" +
            "in schema_name varchar(30), in table_name varchar(20)) " +
            "PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 " +
            "EXTERNAL NAME 'com.splicemachine.triggers.TriggerProcs.proc_call_audit_with_result'";

    private static final String CREATE_EXEC_PROC =
        "CREATE PROCEDURE "+SCHEMA+".proc_exec_sql(" +
            "in sqlText varchar(200)) " +
            "PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 " +
            "EXTERNAL NAME 'com.splicemachine.triggers.TriggerProcs.proc_exec_sql'";

    @ClassRule
    public static SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private TriggerBuilder tb = new TriggerBuilder();
    private TriggerDAO triggerDAO = new TriggerDAO(methodWatcher.getOrCreateConnection());

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"});
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;useSpark=true"});
        return params;
    }

    private String connectionString;

    public Trigger_Exec_Stored_Proc_IT(String connectionString) {
        this.connectionString = connectionString;
    }

    @BeforeClass
    public static void setUpClass() throws Exception {

        String storedProcsJarFilePath = System.getProperty("user.dir")+"/target/sql-it/sql-it.jar";
        // Install the jar file of stored procedures.
        File jar = new File(storedProcsJarFilePath);
        Assert.assertTrue("Can't run test without " + storedProcsJarFilePath, jar.exists());
        classWatcher.executeUpdate(String.format("CALL SQLJ.INSTALL_JAR('%s', '%s', 0)", storedProcsJarFilePath, DERBY_JAR_NAME));
        classWatcher.executeUpdate(String.format(CALL_SET_CLASSPATH_STRING, "'"+ DERBY_JAR_NAME +"'"));
        classWatcher.executeUpdate(CREATE_PROC);
        classWatcher.executeUpdate(CREATE_PROC_WITH_TRANSITION_VAR);
        classWatcher.executeUpdate(CREATE_PROC_WITH_RESULT);
        classWatcher.executeUpdate(CREATE_EXEC_PROC);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        try {
            classWatcher.executeUpdate(String.format("DROP PROCEDURE %s.%s", SCHEMA, "proc_call_audit"));
            classWatcher.executeUpdate(String.format("DROP PROCEDURE %s.%s", SCHEMA, "proc_call_audit_with_transition"));
            classWatcher.executeUpdate(String.format("DROP PROCEDURE %s.%s", SCHEMA, "proc_call_audit_with_result"));
            classWatcher.executeUpdate(String.format("DROP PROCEDURE %s.%s", SCHEMA, "proc_exec_sql"));
        } catch (Exception e) {
            System.err.println("Ignoring in test teardown: " + e.getLocalizedMessage());
        }
        try {
            classWatcher.executeUpdate(String.format(CALL_SET_CLASSPATH_STRING, "NULL"));
        } catch (Exception e) {
            System.err.println("Ignoring in test teardown: " + e.getLocalizedMessage());
        }
        try {
            classWatcher.executeUpdate(String.format("CALL SQLJ.REMOVE_JAR('%s', 0)", DERBY_JAR_NAME));
        } catch (Exception e) {
            System.err.println("Ignoring in test teardown: " + e.getLocalizedMessage());
        }
    }

    @Before
    public void setUp() throws Exception {
        classWatcher.executeUpdate("drop table if exists S");
        classWatcher.executeUpdate("create table S (id integer, name varchar(30))");
        classWatcher.executeUpdate("drop table if exists audit");
        classWatcher.executeUpdate("create table audit (username varchar(20),insert_time timestamp, new_id integer, old_id integer)");

        classWatcher.executeUpdate("drop table if exists t_out");
        classWatcher.executeUpdate("create table t_out(id int, col2 char(10), col3 varchar(50), tm_time timestamp)");
        classWatcher.executeUpdate("drop table if exists t_master");
        classWatcher.executeUpdate("create table t_master(id int, col2 char(10), col3 varchar(50), tm_time timestamp)");
        classWatcher.executeUpdate("drop table if exists t_slave");
        classWatcher.executeUpdate("create table t_slave(id int, description varchar(10),tm_time timestamp)");
        classWatcher.executeUpdate("drop table if exists t_slave2");
        classWatcher.executeUpdate("create table t_slave2(id int, description varchar(10),tm_time timestamp)");

        Connection conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setSchema(SCHEMA.toUpperCase());
        methodWatcher.setConnection(conn);
    }

    @After
    public void cleanup() throws Exception {
        triggerDAO.dropAllTriggers(SCHEMA, "S");
    }
    /**
     * Create/fire a statement trigger that records username and timestamp of an insert in another table.
     */
    @Test
    public void testStatementTriggerUserStoredProc() throws Exception {
        tb.named("auditme").before().insert().on("S").statement().
            then(String.format("CALL proc_call_audit('%s','%s')", SCHEMA,"audit"));
        createTrigger(tb);

        Connection c1 = classWatcher.createConnection();
        Statement s = c1.createStatement();
        s.execute("insert into S values (13, 'Joe')");
        s.execute("insert into S values (14, 'Henry')");

        ResultSet rs = s.executeQuery("select * from audit");
        int count =0;
        while (rs.next()) {
            ++count;
            Assert.assertEquals("splice",rs.getString(1));
            Assert.assertNotNull(rs.getObject(2));
        }
        Assert.assertEquals(2, count);

        rs.close();
        c1.close();
        triggerDAO.drop("auditme");
    }

    /**
     * Create/fire a row trigger that records username, timestamp and new transition value
     * for an row inserted into another table.
     */
    @Test
    public void testRowInsertTriggerUserStoredProc() throws Exception {
        createTrigger(tb.named("row_insert").after().insert().on("S").referencing("NEW AS N")
                        .row().then(String.format("CALL proc_call_audit_with_transition('%s','%s',%s, %s)",
                                                  SCHEMA, "audit", "N.id", null)));

        // when - insert a row
        methodWatcher.executeUpdate("insert into S values (13, 'Joe')");
        methodWatcher.executeUpdate("insert into S values (14, 'Henry')");

        ResultSet rs = methodWatcher.executeQuery("select * from audit");
        int count =0;
        while (rs.next()) {
            Assert.assertEquals("splice",rs.getString(1));
            Assert.assertNotNull(rs.getObject(2));
            Assert.assertNotNull(rs.getObject(3));
            ++count;
        }
        Assert.assertEquals(2, count);

        rs.close();
        triggerDAO.drop("row_insert");
    }

    /**
     * Create/fire a row trigger that records username, timestamp, new transition values
     * for an row updated in another table.
     */
    @Test
    public void testRowUpdateTriggerUserStoredProcNewTransitionValue() throws Exception {

        methodWatcher.executeUpdate("insert into S values (13, 'Joe')");
        methodWatcher.executeUpdate("insert into S values (14, 'Henry')");
        createTrigger(tb.named("row_update_new").after().update().on("S").referencing("New AS N")
                        .row().then(String.format("CALL proc_call_audit_with_transition('%s','%s',%s, %s)",
                                                  SCHEMA, "audit", "N.id", null)));

        // when - update a row
        methodWatcher.executeUpdate("update S set id = 39 where id = 13");

        ResultSet rs = methodWatcher.executeQuery("select * from audit");
//        TestUtils.printResult("select * from audit", rs, System.out);
        int count =0;
        while (rs.next()) {
            Assert.assertEquals("splice",rs.getString(1));
            Assert.assertNotNull(rs.getObject(2));
            int id = rs.getInt(3);
            Assert.assertNotNull(id);
            Assert.assertEquals(39,id);
            Assert.assertNull(rs.getObject(4));
            ++count;
        }
        Assert.assertEquals(1, count);

        rs.close();
        triggerDAO.drop("row_update_new");
    }

    /**
     * Create/fire a row trigger that records username, timestamp, old transition values
     * for an row updated in another table.
     */
    @Test
    public void testRowUpdateTriggerUserStoredProcOldTransitionValue() throws Exception {

        methodWatcher.executeUpdate("insert into S values (13, 'Joe')");
        methodWatcher.executeUpdate("insert into S values (14, 'Henry')");
        createTrigger(tb.named("row_update_old").after().update().on("S").referencing("OLD AS o")
                        .row().then(String.format("CALL proc_call_audit_with_transition('%s','%s',%s, %s)",
                                                  SCHEMA, "audit", null, "O.id")));

        // when - update a row
        methodWatcher.executeUpdate("update S set id = 39 where id = 13");

        ResultSet rs = methodWatcher.executeQuery("select * from audit");
        int count =0;
        while (rs.next()) {
            Assert.assertEquals("splice",rs.getString(1));
            Assert.assertNotNull(rs.getObject(2));
            Assert.assertNull(rs.getObject(3));
            int oldID = rs.getInt(4);
            Assert.assertNotNull(oldID);
            Assert.assertEquals(13, oldID);
            ++count;
        }
        Assert.assertEquals(1, count);

        rs.close();
        triggerDAO.drop("row_update_old");
    }

    /**
     * Create/fire a row trigger that records username, timestamp, new transition values
     * for an row updated in another table.
     */
    @Test
    public void testRowUpdateTriggerUserStoredProcNewAndOldTransitionValues() throws Exception {

        methodWatcher.executeUpdate("insert into S values (13, 'Joe')");
        methodWatcher.executeUpdate("insert into S values (14, 'Henry')");
        createTrigger(tb.named("row_update_new").after().update().on("S").referencing("New AS N OLD AS O")
                        .row().then(String.format("CALL proc_call_audit_with_transition('%s','%s',%s, %s)",
                                                  SCHEMA, "audit", "N.id", "O.id")));

        // when - update a row
        methodWatcher.executeUpdate("update S set id = 39 where id = 13");

        ResultSet rs = methodWatcher.executeQuery("select * from audit");
        int count =0;
        while (rs.next()) {
            Assert.assertEquals("splice", rs.getString(1));
            Assert.assertNotNull(rs.getObject(2));
            int newIDd = rs.getInt(3);
            Assert.assertNotNull(newIDd);
            Assert.assertEquals(39, newIDd);
            int oldID = rs.getInt(4);
            Assert.assertNotNull(oldID);
            Assert.assertEquals(13, oldID);
            ++count;
        }
        Assert.assertEquals(1, count);

        rs.close();
        triggerDAO.drop("row_update_new");
    }

    /**
     * Create/fire a row trigger that records username, timestamp, new and old transition values
     * for an row updated in another table.
     */
    @Test
    public void testRowUpdateTriggerUserStoredProcTwoTriggers() throws Exception {

        methodWatcher.executeUpdate("insert into S values (13, 'Joe')");
        methodWatcher.executeUpdate("insert into S values (14, 'Henry')");
        createTrigger(tb.named("row_update_new").after().update().on("S").referencing("New AS N")
                        .row().then(String.format("CALL proc_call_audit_with_transition('%s','%s',%s, %s)",
                                                  SCHEMA, "audit", "N.id", null)));
        createTrigger(tb.named("row_update_old").after().update().on("S").referencing("OLD AS o")
                        .row().then(String.format("CALL proc_call_audit_with_transition('%s','%s',%s, %s)",
                                                  SCHEMA, "audit", null, "O.id")));

        // when - update a row
        methodWatcher.executeUpdate("update S set id = 39 where id = 13");

        triggerDAO.drop("row_update_new");
        triggerDAO.drop("row_update_old");
    }

    /**
     * Create/fire a statement trigger that records username and timestamp of an insert in another table.
     */
    @Test
    public void testStatementTriggerUserStoredProcWithResult() throws Exception {
        tb.named("auditme2").before().insert().on("S").statement().
            then(String.format("CALL proc_call_audit_with_result('%s','%s')", SCHEMA,"audit"));
        createTrigger(tb);

        Connection c1 = classWatcher.createConnection();
        Statement s = c1.createStatement();
        s.execute("insert into S values (13,'Joe')");
        s.execute("insert into S values (-1,'Henry')");

        ResultSet rs = s.executeQuery("select * from audit");
        int count =0;
        while (rs.next()) {
            ++count;
            Assert.assertEquals("splice",rs.getString(1));
            Assert.assertNotNull(rs.getObject(2));
        }
        Assert.assertEquals(2, count);

        rs.close();
        c1.close();
        triggerDAO.drop("auditme2");
    }

    /**
     * Create/fire a statement trigger that calls a splice system procedure.
     */
    @Test
    public void testStatementTriggerSysStoredProc() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("call SYSCS_UTIL.SYSCS_GET_LOGGER_LEVEL('com.splicemachine.tools.version.ManifestFinder')");
        String originalLevel = null;
        while (rs.next()) {
            originalLevel = rs.getString(1);
        }
        Assert.assertNotNull(originalLevel);
        String newlevel = "INFO";
        if (originalLevel.equals("INFO")) {
            newlevel = "WARN";
        }
        tb.named("log_level_change").before().insert().on("S").statement().
            then("call SYSCS_UTIL.SYSCS_SET_LOGGER_LEVEL('com.splicemachine.tools.version.ManifestFinder', '"+newlevel+"')");
        createTrigger(tb);

        Connection c1 = classWatcher.createConnection();
        Statement s = c1.createStatement();
        s.execute("insert into S values (13,'Joe')");

        rs = methodWatcher.executeQuery("call SYSCS_UTIL.SYSCS_GET_LOGGER_LEVEL('com.splicemachine.tools.version.ManifestFinder')");
        String changedLevel = null;
        while (rs.next()) {
            changedLevel = rs.getString(1);
        }
        Assert.assertNotNull(changedLevel);
        Assert.assertEquals(newlevel, changedLevel);

        c1.createStatement().execute("call SYSCS_UTIL.SYSCS_SET_LOGGER_LEVEL('com.splicemachine.tools.version" +
                                         ".ManifestFinder', '" + originalLevel+"')");

        rs.close();
        c1.close();
        triggerDAO.drop("log_level_change");
    }

    /**
     * Create/fire a statement trigger that calls a splice system procedure with a SQL string to execute.
     */
    @Test @Ignore("DB-3424: Executing trigger action insert thru a stored proc (disallowed directly) causes infinite recursion.")
    public void testExecSQLRecursion() throws Exception {

        methodWatcher.executeUpdate("insert into t_master values (13, 'grrr', 'I''m a pirate!', CURRENT_TIMESTAMP)");
        methodWatcher.executeUpdate("insert into t_slave values (99, 'trigger01', CURRENT_TIMESTAMP)");
        methodWatcher.executeUpdate("insert into t_slave2 values (22, 'trigger02', CURRENT_TIMESTAMP)");

        createTrigger(tb.named("TriggerMaster01").before().insert().on("t_master")
                        .statement().then(String.format("CALL proc_exec_sql('%s')",
                  "insert into "+SCHEMA+".t_master select id, description, ''Is the REAL deal'', CURRENT_TIMESTAMP from "+SCHEMA+".t_slave")));

        createTrigger(tb.named("TriggerMaster02").before().insert().on("t_master")
                        .statement().then(String.format("CALL proc_exec_sql('%s')",
                  "insert into "+SCHEMA+".t_master select id, description, ''Is the REAL deal'', CURRENT_TIMESTAMP from "+SCHEMA+".t_slave2")));

        // never returns...
        methodWatcher.executeUpdate("insert into "+SCHEMA+".t_master values (01, 'roar', 'I''m a tiger!', CURRENT_TIMESTAMP)");

        triggerDAO.drop("TriggerMaster01");
        triggerDAO.drop("TriggerMaster02");
    }

    /**
     * Create/fire a statement trigger that calls a splice system procedure with a SQL string to execute.
     */
    @Test
    public void testExecSQL() throws Exception {

        methodWatcher.executeUpdate("insert into t_master values (13, 'grrr', 'I''m a pirate!', CURRENT_TIMESTAMP)");
        methodWatcher.executeUpdate("insert into t_slave values (22, 'trigger01', CURRENT_TIMESTAMP)");
        methodWatcher.executeUpdate("insert into t_slave2 values (99, 'trigger02', CURRENT_TIMESTAMP)");

        createTrigger(tb.named("TriggerMaster01").before().insert().on("t_master")
                        .statement().then(String.format("CALL proc_exec_sql('%s')",
                  "insert into "+SCHEMA+".t_out select id, description, ''Is the REAL deal'', CURRENT_TIMESTAMP from "+SCHEMA+".t_slave")));

        createTrigger(tb.named("TriggerMaster02").before().insert().on("t_master")
                        .statement().then(String.format("CALL proc_exec_sql('%s')",
                  "insert into "+SCHEMA+".t_out select id, description, ''Is the REAL deal'', CURRENT_TIMESTAMP from "+SCHEMA+".t_slave2")));

        methodWatcher.executeUpdate("insert into " + SCHEMA + ".t_master values (01, 'roar', 'I''m a tiger!', CURRENT_TIMESTAMP)");

//        String query = "select * from t_master";
//        ResultSet rs = methodWatcher.executeQuery(query);
//        TestUtils.printResult(query, rs, System.out);
//        query = "select * from t_out";
//        rs = methodWatcher.executeQuery(query);
//        TestUtils.printResult(query, rs, System.out);

        ResultSet rs = methodWatcher.executeQuery("select * from t_master");
        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertEquals("Expected 2 rows in t_master", 2, count);

        rs = methodWatcher.executeQuery("select * from t_out order by id");
        Timestamp t1 = null;
        Timestamp t2 = null;
        count = 0;
        while (rs.next()) {
            ++count;
            if (count == 1) {
                Assert.assertEquals("trigger01", rs.getString(2).trim());
                t1 = rs.getTimestamp(4);
            } else {
                Assert.assertEquals("trigger02", rs.getString(2).trim());
                t2 = rs.getTimestamp(4);
            }
        }
        Assert.assertEquals("Expected 2 rows in t_out", 2, count);
        Assert.assertNotNull(t1);
        Assert.assertNotNull(t2);
        Assert.assertTrue(t1.before(t2));

        rs.close();
        triggerDAO.drop("TriggerMaster01");
        triggerDAO.drop("TriggerMaster02");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // Utility
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void createTrigger(TriggerBuilder tb) throws Exception {
//        System.out.println(tb.build());
        methodWatcher.executeUpdate(tb.build());
    }
}
