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

package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.Transactions;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

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
@Category({Transactions.class,SerialTest.class })
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
        String STORED_PROCS_JAR_FILE = System.getProperty("user.dir")+"/target/sql-it/sql-it.jar";
        assertTrue("Cannot find procedures jar file: "+STORED_PROCS_JAR_FILE, STORED_PROCS_JAR_FILE != null &&
                STORED_PROCS_JAR_FILE.endsWith("jar"));
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

    @Ignore
    /*
    This test will usually run failed since any little gc pause from the server will make
    the query timeout.
     */
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
                Assert.assertEquals("Expected a query timeout.", "XCL52", e.getSQLState());

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
