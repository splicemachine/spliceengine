package com.splicemachine.triggers;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.test_dao.TriggerDAO;

/**
 * Tests trigger execution of stored procedures.
 * <p/>
 * Note the dependency on user-defined stored procedures in this test class.<br/>
 * See {@link TriggerProcs} for instructions on adding/modifying store procedures.
 * <p/>
 * Note also, to run tests in this class in your IDE, set your test configuration working
 * directory to the splice_machine_test directory of maven profile you're running:
 * i.e. in Idea - $MODULE_DIR$
 *
 */
public class Trigger_Exec_Stored_Proc_IT {

    private static final String SCHEMA = Trigger_Exec_Stored_Proc_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    private static final String STORED_PROCS_JAR_FILE = SpliceUnitTest.getBaseDirectory()+"/target/test-classes/trigger_procs.jar";
    private static final String DERBY_JAR_NAME = schemaWatcher.schemaName + ".TRIGGER_PROCS_JAR";
    private static final String CALL_SET_CLASSPATH_STRING =
        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', %s)";
    private static final String CREATE_PROC =
        "CREATE PROCEDURE "+ schemaWatcher.schemaName+".proc_call_audit(" +
            "in schema_name varchar(30), in table_name varchar(20)) " +
            "PARAMETER STYLE JAVA LANGUAGE JAVA READS SQL DATA " +
            "EXTERNAL NAME 'com.splicemachine.triggers.TriggerProcs.proc_call_audit'";

    private static final String CREATE_PROC_WITH_RESULT =
        "CREATE PROCEDURE "+schemaWatcher.schemaName+".proc_call_audit_with_result(" +
            "in schema_name varchar(30), in table_name varchar(20)) " +
            "PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 " +
            "EXTERNAL NAME 'com.splicemachine.triggers.TriggerProcs.proc_call_audit_with_result'";

    @BeforeClass
    public static void setUpClass() throws Exception {
        // Install the jar file of stored procedures.
        File jar = new File(STORED_PROCS_JAR_FILE);
        Assert.assertTrue("Can't run test without " + STORED_PROCS_JAR_FILE, jar.exists());
        classWatcher.executeUpdate(String.format("CALL SQLJ.INSTALL_JAR('%s', '%s', 0)",
                                                 STORED_PROCS_JAR_FILE, DERBY_JAR_NAME));
        classWatcher.executeUpdate(String.format(CALL_SET_CLASSPATH_STRING, "'"+ DERBY_JAR_NAME +"'"));
        classWatcher.executeUpdate(CREATE_PROC);
        classWatcher.executeUpdate(CREATE_PROC_WITH_RESULT);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        try {
            classWatcher.executeUpdate(String.format("DROP PROCEDURE %s.%s", schemaWatcher.schemaName, "proc_call_audit"));
            classWatcher.executeUpdate(String.format("DROP PROCEDURE %s.%s", schemaWatcher.schemaName, "proc_call_audit_with_result"));
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
        classWatcher.executeUpdate("create table S (id int)");
        classWatcher.executeUpdate("create table audit (username varchar(20),insert_time timestamp, stmt varchar(300))");
    }

    @After
    public void tearDown() throws Exception {
        classWatcher.executeUpdate("drop table S");
        classWatcher.executeUpdate("drop table audit");
    }

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private TriggerBuilder tb = new TriggerBuilder();
    private TriggerDAO triggerDAO = new TriggerDAO(methodWatcher.getOrCreateConnection());

    /**
     * Create/fire a statement trigger that records username and timestamp of an insert
     * in another table.
     * @throws Exception
     */
    @Test
    public void testStatementTriggerUserStoredProc() throws Exception {
        tb.named("auditme").before().insert().on("S").statement().
            then(String.format("CALL proc_call_audit('%s','%s')", schemaWatcher.schemaName,"audit"));
        createTrigger(tb);

        Connection c1 = classWatcher.createConnection();
        Statement s = c1.createStatement();
        s.execute("insert into S (id) values (13)");
        s.execute("insert into S (id) values (-1)");

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
     * Create/fire a statement trigger that records username and timestamp of an insert
     * in another table.
     * @throws Exception
     */
    @Test
    public void testStatementTriggerUserStoredProcWithResult() throws Exception {
        tb.named("auditme2").before().insert().on("S").statement().
            then(String.format("CALL proc_call_audit_with_result('%s','%s')", schemaWatcher.schemaName,"audit"));
        createTrigger(tb);

        Connection c1 = classWatcher.createConnection();
        Statement s = c1.createStatement();
        s.execute("insert into S (id) values (13)");
        s.execute("insert into S (id) values (-1)");

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
     * @throws Exception
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
        s.execute("insert into S (id) values (13)");

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

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void createTrigger(TriggerBuilder tb) throws Exception {
        // DEBUG
        System.out.println(tb.build());
        methodWatcher.executeUpdate(tb.build());
    }
}
