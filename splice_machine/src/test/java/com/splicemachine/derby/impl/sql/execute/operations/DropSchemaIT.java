package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceUserWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;

public class DropSchemaIT extends SpliceUnitTest {

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = DropSchemaIT.class.getSimpleName().toUpperCase();

    private static final String SCHEMA_A = CLASS_NAME + "_SCHEMA_A";
    private static final String SCHEMA_B = CLASS_NAME + "_SCHEMA_B";

    protected static final String USER1 = CLASS_NAME + "_TOM";
    protected static final String PASSWORD1 = "tom";

    private static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);
    private static String STORED_PROCS_JAR_FILE;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceUserWatcher1);

    protected static TestConnection adminConn;
    protected static TestConnection user1Conn;

    @BeforeClass
    public static void setUpClass() throws Exception {
        adminConn = spliceClassWatcher.createConnection();
        user1Conn = spliceClassWatcher.connectionBuilder().user(USER1).password(PASSWORD1).build();

        STORED_PROCS_JAR_FILE = getSqlItJarFile();
        assertTrue("Cannot find procedures jar file: "+STORED_PROCS_JAR_FILE, STORED_PROCS_JAR_FILE != null &&
                STORED_PROCS_JAR_FILE.endsWith("jar"));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        user1Conn.close();
        adminConn.execute(format("call syscs_util.syscs_drop_user('%s')", USER1));
        ResultSet rs = adminConn.query(format("select count(*) from sys.sysschemas where schemaname='%s'", SCHEMA_B));
        if (rs.next() && rs.getInt(1) > 0) {
            adminConn.execute(format("drop schema %s cascade", SCHEMA_B));
        }
        rs.close();

        rs = adminConn.query(format("select count(*) from sys.sysschemas where schemaname='%s'", SCHEMA_A));
        if (rs.next() && rs.getInt(1) > 0) {
            adminConn.execute(format("drop schema %s cascade", SCHEMA_A));
        }
        rs.close();
        adminConn.close();
    }

    @Test
    public void testDropSchemaWithVariousObjects() throws Exception {
        /* create objects with the following dependencies in SCHEMA_A:
                          View v2                                      Sequence S1                 T6 (with self-referencing PK)
                            |
                         --------------------------
                         |                        |
                         Alias AS_V1          Alias AS_T4
                         |                        |
        Trigger Trig1   View V1                Table T4 (FK->PK)
               |         |                        |
               ----------                     Table T3 (FK->PK)        Table T5 (FK->PK)
                     |                            |                         |
                   Table T1                       ---------------------------
                                                                 |
                                                          Table T2 (PK) with index idx_t2
         */

        adminConn.execute("create schema " + SCHEMA_A);
        adminConn.execute(format("create table %s.T1 (a1 int, b1 int, c1 int)", SCHEMA_A));
        adminConn.execute(format("CREATE TRIGGER %s.trig1\n" +
                "    BEFORE INSERT\n" +
                "    ON %s.t1\n" +
                "    REFERENCING NEW AS NEWROW\n" +
                "    FOR EACH ROW\n" +
                "    BEGIN ATOMIC values 1; end\n", SCHEMA_A, SCHEMA_A));
        adminConn.execute(format("create view %s.v1 as select * from %s.t1 where a1=1", SCHEMA_A, SCHEMA_A));
        adminConn.execute(format("create alias %s.AS_V1 for %s.v1", SCHEMA_A, SCHEMA_A));

        adminConn.execute(format("create table %s.T2 (a2 int, b2 int, c2 int unique not null, primary key(a2))", SCHEMA_A));
        adminConn.execute(format("create index idx_t2 on %s.T2(b2, c2)", SCHEMA_A));
        adminConn.execute(format("create table %s.T3 (a3 int, b3 int, c3 int unique, constraint fk foreign key(a3) references %s.t2(a2))", SCHEMA_A, SCHEMA_A));
        adminConn.execute(format("create table %s.T5 (a5 int, b5 int, c5 int, constraint fk3 foreign key(a5) references %s.T2(c2))", SCHEMA_A, SCHEMA_A));

        adminConn.execute(format("create table %s.T4 (a4 int, b4 int, c4 int, constraint fk2 foreign key(a4) references %s.T3(c3))", SCHEMA_A, SCHEMA_A));
        adminConn.execute(format("create alias %s.AS_T4 for %s.T4", SCHEMA_A, SCHEMA_A));
        adminConn.execute(format("create view %s.V2 as select a1, a4 from %s.AS_V1, %s.AS_T4 where a1=a4", SCHEMA_A, SCHEMA_A, SCHEMA_A));

        adminConn.execute(format("create sequence %s.s1", SCHEMA_A));

        adminConn.execute(format("create table %s.T6(a6 int primary key, b6 int, constraint fk6 foreign key(b6) references %s.T6(a6))", SCHEMA_A, SCHEMA_A));

        // user 1 can see the schema but does not have permission to drop the schema
        adminConn.execute(format("grant access on schema %s to %s", SCHEMA_A, USER1));
        try {
            user1Conn.execute(format("drop schema %s cascade", SCHEMA_A));
            Assert.fail("expected exception to be thrown but none was");
        } catch (SQLException e) {
            Assert.assertEquals(format("User '%s' can not perform the operation in schema '%s'.", USER1, SCHEMA_A), e.getMessage());
        }

        // grant user1 as the owner of this schema
        adminConn.execute(format("call syscs_util.SYSCS_UPDATE_SCHEMA_OWNER('%s', '%s')", SCHEMA_A, USER1));

        // now we should be able to drop the schema successfully
        try {
            user1Conn.execute(format("drop schema %s cascade", SCHEMA_A));
        } catch (SQLException e) {
            Assert.fail("Drop Schema should not fail, but fail with exception "+ e.getMessage());
        }

        ResultSet rs = adminConn.query(format("select count(*) from sys.sysschemas where schemaname='%s'", SCHEMA_A));
        Assert.assertTrue(format("Schema %s still exists!", SCHEMA_A), (rs.next() && rs.getInt(1)==0));
        rs.close();
    }

    @Test
    public void testTriggersWithTargetTableOutsideSchema() throws Exception {
        /* schema A has a table T1, schema B has a view and a trigger defined on T1 */
        adminConn.execute("create schema " + SCHEMA_A);
        adminConn.execute(format("create table %s.T1 (a1 int, b1 int, c1 int)", SCHEMA_A));
        adminConn.execute("create schema " + SCHEMA_B);
        adminConn.execute(format("create view %s.V1 as select a1 from %s.T1", SCHEMA_B, SCHEMA_A));
        adminConn.execute(format("CREATE TRIGGER %s.trig1\n" +
                "    BEFORE INSERT\n" +
                "    ON %s.t1\n" +
                "    REFERENCING NEW AS NEWROW\n" +
                "    FOR EACH ROW\n" +
                "    BEGIN ATOMIC values 1; end", SCHEMA_B, SCHEMA_A));
        try {
            adminConn.execute(format("drop schema %s cascade", SCHEMA_A));
            Assert.fail("expected exception to be thrown but none was");
        } catch (SQLException e) {
            Assert.assertEquals(format("Operation 'DROP SCHEMA %s CASCADE' is not successful, because Table 'T1' has dependent(s) from outside the schema.", SCHEMA_A), e.getMessage());
        }

        /* after dropping SCHEMA_B, SCHEMA_A can be dropped */
        try {
            adminConn.execute(format("drop schema %s cascade", SCHEMA_B));
            adminConn.execute(format("drop schema %s cascade", SCHEMA_A));
        } catch (SQLException e) {
            Assert.fail("Drop Schema should not fail, but fail with exception "+ e.getMessage());
        }
    }

    @Test
    public void testDropSchemaWithUDF() throws Exception {
        adminConn.execute("create schema " + SCHEMA_A);
        // Install the jar file of user-defined stored procedures.
        adminConn.execute(format("CALL SQLJ.INSTALL_JAR('%s', '%s', 0)", STORED_PROCS_JAR_FILE, SCHEMA_A + ".\"SQLJ_IT_procs_JAR\""));
        // Add the jar file into the local DB class path.
        adminConn.execute(format("CALL SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('derby.database.classpath', '%s')", SCHEMA_A+".\"SQLJ_IT_procs_JAR\""));
        // Create the user-defined stored procedure.
        adminConn.execute(format("CREATE PROCEDURE %s.SIMPLE_ONE_ARG_PROC(IN name VARCHAR(30)) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.SIMPLE_ONE_ARG_PROC'", SCHEMA_A));

        // Call the user-defined stored procedure.
        ResultSet rs = adminConn.query(format("CALL " + SCHEMA_A + ".SIMPLE_ONE_ARG_PROC('%s')", "foobar"));
        assertTrue("Incorrect rows returned!", resultSetSize(rs) > 10);
        rs.close();

        /* drop schema A with restrict should not be successful, but cascade should*/
        try {
            adminConn.execute(format("drop schema %s restrict", SCHEMA_A));
            Assert.fail("expected exception to be thrown but none was");
        } catch (SQLException e) {
            Assert.assertEquals(format("Schema '%s' cannot be dropped because it is not empty.", SCHEMA_A), e.getMessage());
        }

        try {
            adminConn.execute(format("drop schema %s cascade", SCHEMA_A));
        } catch (SQLException e) {
            Assert.fail("Drop Schema should not fail, but fail with exception "+ e.getMessage());
        }
    }
}