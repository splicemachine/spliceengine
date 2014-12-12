package com.splicemachine.derby.transactions;

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;

/**
 * ITs relating to transactional behavior when queries time out.
 * <p/>
 * This test uses stored procedures in {@link com.splicemachine.derby.transactions.TestProcs}
 * to create a manufactured lag in update time.  We assure the update is rolled back when
 * it goes beyond the timeout set on the Statement.
 *
 * @author Jeff Cunningham
 *         Date: 12/8/14
 */
public class QueryTimeoutIT extends SpliceUnitTest {
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(QueryTimeoutIT.class.getSimpleName());

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int)");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        Statement statement = classWatcher.getStatement();
                        statement.execute("insert into "+table+" (a,b) values (1,1)");
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            });

    private static final String STORED_PROCS_JAR_FILE = getBaseDirectory()+"/target/test-classes/test_procs.jar";
    private static final String DERBY_JAR_NAME = schemaWatcher.schemaName + ".TESTS_PROCS_JAR";
    private static final String CALL_INSTALL_JAR_STRING = "CALL SQLJ.INSTALL_JAR('%s', '%s', 0)";
    private static final String CALL_SET_CLASSPATH_STRING =
        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', %s)";
    private static final String CALL_REMOVE_JAR_FORMAT_STRING = "CALL SQLJ.REMOVE_JAR('%s', 0)";

    public static final String UPDATE_WITH_TIMEOUT_AFTER_PROC = "UPDATE_WITH_TIMEOUT_AFTER";
    private static final String CREATE_PROC_UPDATE_TIMEOUT_AFTER =
        "CREATE PROCEDURE "+schemaWatcher.schemaName+".UPDATE_WITH_TIMEOUT_AFTER(" +
        "IN updateString VARCHAR(300), IN sleepSecs INTEGER) " +
         "PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 " +
         "EXTERNAL NAME 'com.splicemachine.derby.transactions.TestProcs.UPDATE_WITH_TIMEOUT_AFTER'";

    public static final String UPDATE_WITH_TIMEOUT_BEFORE_PROC = "UPDATE_WITH_TIMEOUT_BEFORE";
    private static final String CREATE_PROC_UPDATE_TIMEOUT_BEFORE =
        "CREATE PROCEDURE "+schemaWatcher.schemaName+".UPDATE_WITH_TIMEOUT_BEFORE(" +
        "IN updateString VARCHAR(300), IN sleepSecs INTEGER) " +
         "PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 " +
         "EXTERNAL NAME 'com.splicemachine.derby.transactions.TestProcs.UPDATE_WITH_TIMEOUT_BEFORE'";

    private static String getDropProc(String procName) {
        return String.format("DROP PROCEDURE %s.%s", schemaWatcher.schemaName, procName);
    }

    private static final String UPDATE_WITH_TIMEOUT_AFTER = "CALL " + schemaWatcher.schemaName +
        ".UPDATE_WITH_TIMEOUT_AFTER('%s', %d)";

    private static final String UPDATE_WITH_TIMEOUT_BEFORE = "CALL " + schemaWatcher.schemaName +
        ".UPDATE_WITH_TIMEOUT_BEFORE('%s', %d)";

    @Before
    public void setUpClass() throws Exception {
        // Install the jar file of user-defined stored procedures.
        File jar = new File(STORED_PROCS_JAR_FILE);
        Assert.assertTrue("Can't run test without " + STORED_PROCS_JAR_FILE, jar.exists());
        classWatcher.executeUpdate(String.format(CALL_INSTALL_JAR_STRING, STORED_PROCS_JAR_FILE, DERBY_JAR_NAME));
        classWatcher.executeUpdate(String.format(CALL_SET_CLASSPATH_STRING, "'"+ DERBY_JAR_NAME +"'"));
        classWatcher.executeUpdate(CREATE_PROC_UPDATE_TIMEOUT_BEFORE);
        classWatcher.executeUpdate(CREATE_PROC_UPDATE_TIMEOUT_AFTER);
    }

    @After
    public void tearDownClass() throws Exception {
        try {
            classWatcher.executeUpdate(getDropProc(UPDATE_WITH_TIMEOUT_BEFORE_PROC));
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            classWatcher.executeUpdate(getDropProc(UPDATE_WITH_TIMEOUT_AFTER_PROC));
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            classWatcher.executeUpdate(String.format(CALL_SET_CLASSPATH_STRING, "NULL"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            classWatcher.executeUpdate(String.format(CALL_REMOVE_JAR_FORMAT_STRING, DERBY_JAR_NAME));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSetRetrieveTimeout() throws Exception {
        int expectedTimeout = 3;
        Statement statement = null;
        int checkedTimeout;
        try {
            statement = classWatcher.getStatement();
            statement.setQueryTimeout(expectedTimeout);
            checkedTimeout = statement.getQueryTimeout();
        } finally {
            closeQuietly(statement);
        }
        Assert.assertEquals(expectedTimeout, checkedTimeout);

        expectedTimeout = 13;
        try {
            statement = classWatcher.getStatement();
            statement.setQueryTimeout(expectedTimeout);
            checkedTimeout = statement.getQueryTimeout();
        } finally {
            closeQuietly(statement);
        }
        Assert.assertEquals(expectedTimeout, checkedTimeout);

        expectedTimeout = 0;
        try {
            statement = classWatcher.getStatement();
            statement.setQueryTimeout(expectedTimeout);
            checkedTimeout = statement.getQueryTimeout();
        } finally {
            closeQuietly(statement);
        }
        Assert.assertEquals(expectedTimeout, checkedTimeout);

        expectedTimeout = 1300459;
        try {
            statement = classWatcher.getStatement();
            statement.setQueryTimeout(expectedTimeout);
            checkedTimeout = statement.getQueryTimeout();
        } finally {
            closeQuietly(statement);
        }
        Assert.assertEquals(expectedTimeout, checkedTimeout);

        expectedTimeout = -1;
        try {
            statement = classWatcher.getStatement();
            statement.setQueryTimeout(expectedTimeout);
            Assert.fail("Expected failure when query timeout set to -1");
        } catch (Exception e) {
            // expected
        } finally {
            closeQuietly(statement);
        }
    }

    @Test
    public void testUpdateAndSelectWithTimeoutBefore300_0() throws Exception {
        helpTestQueryTimeout(UPDATE_WITH_TIMEOUT_BEFORE, 300, 0);
    }

    @Test
    public void testUpdateAndSelectWithTimeoutBefore1_2() throws Exception {
        helpTestQueryTimeout(UPDATE_WITH_TIMEOUT_BEFORE, 1, 2);
    }

    @Test
    public void testUpdateAndSelectWithTimeoutBefore5_10() throws Exception {
        helpTestQueryTimeout(UPDATE_WITH_TIMEOUT_BEFORE, 5, 10);
    }

    @Test
    public void testUpdateAndSelectWithTimeoutBefore2_2() throws Exception {
        helpTestQueryTimeout(UPDATE_WITH_TIMEOUT_BEFORE, 2, 2);
    }

    @Test
    public void testUpdateAndSelectWithTimeoutBefore3_2() throws Exception {
        helpTestQueryTimeout(UPDATE_WITH_TIMEOUT_BEFORE, 3, 2);
    }

    @Test
    public void testUpdateAndSelectWithTimeoutAfter300_0() throws Exception {
        helpTestQueryTimeout(UPDATE_WITH_TIMEOUT_AFTER, 300, 0);
    }

    @Test
    public void testUpdateAndSelectWithTimeoutAfter1_2() throws Exception {
        helpTestQueryTimeout(UPDATE_WITH_TIMEOUT_AFTER, 1, 2);
    }

    @Test
    public void testUpdateAndSelectWithTimeoutAfter5_10() throws Exception {
        helpTestQueryTimeout(UPDATE_WITH_TIMEOUT_AFTER, 5, 10);
    }

    @Test
    public void testUpdateAndSelectWithTimeoutAfter2_2() throws Exception {
        helpTestQueryTimeout(UPDATE_WITH_TIMEOUT_AFTER, 2, 2);
    }

    @Test
    public void testUpdateAndSelectWithTimeoutAfter3_2() throws Exception {
        helpTestQueryTimeout(UPDATE_WITH_TIMEOUT_AFTER, 3, 2);
    }

    private void helpTestQueryTimeout(String procedure, int timeoutSecs, int sleepSecs) throws Exception {
        // we expect a query timeout if we're to sleep longer or equal to the timeout we're setting
        boolean expectTimeout = (sleepSecs >= timeoutSecs);

        String sqlText =  String.format("SELECT * FROM %s.%s", schemaWatcher.schemaName, "A");
        // get a representative expected result. we'll use this to compare with later result
        // to make sure a roll back occurred if there was a query timeout.
        ResultSet rs = classWatcher.executeQuery(sqlText);
        String expectedResults = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        rs.close();

        String updateTxt = String.format("insert into %s.%s values ("+timeoutSecs+","+sleepSecs+")",
                                         schemaWatcher.schemaName, "A");

        Statement statement = null;
        String updateSQL = String.format(procedure, updateTxt, sleepSecs);
        try {
            statement = classWatcher.getStatement();
            // set the provided timeout on the Statement
            statement.setQueryTimeout(timeoutSecs);
            statement.executeUpdate(updateSQL);
            if (expectTimeout) {
                // we updated the table and expected to get a query timeout exception
                Assert.fail(updateSQL+"\nExpected the update to timeout. Timeout set to: "+timeoutSecs+" sec; slept: "+sleepSecs+" sec.");
            }
        } catch (SQLException e) {
            // can happen if procedure is not compiled/jarred/installed correctly
            Assert.assertFalse(e.getMessage(), e.getMessage().contains("is not recognized as a function or procedure"));
            Assert.assertFalse(e.getMessage(), e.getMessage().contains("does not exist or is inaccessible."));

            if (expectTimeout) {
                // We timed out and caught an exception. Make sure we get the correct exception
                Assert.assertEquals("Expected a query timeout.", "08006", e.getSQLState());

                // Make sure the update txn got rolled back
                rs = classWatcher.executeQuery(sqlText);
                Assert.assertEquals("Expected update should have rolled back.\n" + updateSQL + "\n", expectedResults,
                                    TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            } else {
                // unexpected
                Assert.fail(String.valueOf(e));
            }
        } finally {
            closeQuietly(rs);
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
