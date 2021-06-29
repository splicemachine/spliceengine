package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

public class SQLWarningIT extends SpliceUnitTest {
    public static final String CLASS_NAME = SQLWarningIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @BeforeClass
    public static void setup() throws Exception {
        setup(spliceClassWatcher);
    }

    private static void setup(SpliceWatcher spliceClassWatcher) throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table A (c1 int)")
                .withInsert("insert into A values(?)")
                .withRows(rows(
                        row(1),
                        row(2))).create();

        new TableCreator(conn)
                .withCreate("create table T1 (c1 char(250), c2 varchar(100), c3 varchar(100))")
                .withInsert("insert into T1 values (?,?,?)")
                .withRows(rows(
                        row("a", "b", "c")))
                .create();
    }

    @Test
    public void testNoRowsAffectedDB2Warning() throws Exception {
        try(Connection connection = spliceClassWatcher.getOrCreateConnection();
            Statement s = connection.createStatement()) {
            s.execute("call syscs_util.syscs_set_global_database_property('splice.db2.error.compatible', true)");
            s.execute("delete from a where c1 = 0");
            SQLWarning warning = s.getWarnings();
            Assert.assertTrue(warning!=null);
            Assert.assertEquals("02000", warning.getSQLState());
            Assert.assertEquals(100, warning.getErrorCode());
            s.execute("call syscs_util.syscs_set_global_database_property('splice.db2.error.compatible', false)");
        }
    }
    @Test
    public void testNoRowsAffectedDB2WarningForPrepareStatement() throws Exception {
        try(Connection connection = spliceClassWatcher.getOrCreateConnection()){
            try (PreparedStatement ps = connection.prepareStatement
                    ("call syscs_util.syscs_set_global_database_property('splice.db2.error.compatible', true)")) {
                ps.execute();
            }

            try (PreparedStatement ps = connection.prepareStatement("delete from a where c1 = 0")) {
                ps.execute();
                SQLWarning warning = ps.getWarnings();
                Assert.assertTrue(warning != null);
                Assert.assertEquals("02000", warning.getSQLState());
                Assert.assertEquals(100, warning.getErrorCode());
            }

            try (PreparedStatement ps = connection.prepareStatement
                    ("call syscs_util.syscs_set_global_database_property('splice.db2.error.compatible', false)")) {
                ps.execute();
            }
        }
    }
    @Test
    public void testNoRowsAffectedWarning() throws Exception {
        try(Connection connection = spliceClassWatcher.getOrCreateConnection();
            Statement s = connection.createStatement()) {
            s.execute("call syscs_util.syscs_set_global_database_property('splice.db2.error.compatible', false)");
            s.execute("delete from a where c1 = 0");
            SQLWarning warning = s.getWarnings();
            Assert.assertTrue(warning == null);
        }
    }

    @Test
    public void testTruncateWarning1() throws Exception {
        String query = "SELECT * FROM sysibm.sysdummy1 WHERE '?2,BL' NOT IN ( CAST('1969-12-16 17:40:41' AS VARCHAR(16)))";
        try(Connection connection = spliceClassWatcher.getOrCreateConnection();
            Statement s = connection.createStatement()) {
            s.execute(query);
            SQLWarning warning = s.getWarnings();
            Assert.assertTrue(warning != null);
            Assert.assertEquals("01004", warning.getSQLState());
            Assert.assertEquals("Data truncation", warning.getMessage());
        }
    }

    @Ignore
    @Test
    public void testTruncateWarning2() throws Exception {
        String query = "select cast(c1 as char(5)), cast(c2 as char(5)), cast(c3 as char(5)) from t1";
        try(Connection connection = spliceClassWatcher.getOrCreateConnection();
            Statement s = connection.createStatement()) {
            s.execute(query);
            SQLWarning warning = s.getWarnings();
            Assert.assertTrue(warning == null);

            s.execute("update t1 set c2='abcdef'");
            s.execute(query);
            // We do have the data truncate warning set during s.execute(query) above. However, we receive
            // a RDBRLLBCK code point (rollback) as the final step of s.execute(), which clears warnings.
            // It might be related to test framework?
            // This warning can be seen in sqlshell.
            warning = s.getWarnings();
            Assert.assertTrue(warning != null);
            Assert.assertEquals("01004", warning.getSQLState());
            Assert.assertEquals("Data truncation", warning.getMessage());
        }
    }
}
