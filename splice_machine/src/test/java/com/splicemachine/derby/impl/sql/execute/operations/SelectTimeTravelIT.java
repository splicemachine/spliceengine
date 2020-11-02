package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.utils.SpliceDateTimeFormatter;
import com.splicemachine.test.HBaseTest;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import static org.junit.Assert.fail;

// there are some MRP tests in this suite that reach a code path which calls TxnStore.getTxnAt, this method
// is not implemented in mem profile, that's why we limit this test suite to run only in HBase-providing profiles
// for these tests to run properly.
@Category(HBaseTest.class)
public class SelectTimeTravelIT {

    private static final String SCHEMA = SelectTimeTravelIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceWatcher watcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    private long getTxId() throws SQLException {
        long result = -1;
        try(ResultSet rs =  watcher.executeQuery("CALL SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()")) {
            Assert.assertTrue(rs.next());
            result = rs.getLong(1);
            Assert.assertFalse(rs.next());
            return result;
        }
    }

    static File tempDir;
    @BeforeClass
    public static void createTempDirectory() throws Exception {
        tempDir = SpliceUnitTest.createTempDirectory(SCHEMA);
    }

    @AfterClass
    public static void deleteTempDirectory() throws Exception {
        SpliceUnitTest.deleteTempDirectory(tempDir);
    }

    static int counter = 0;
    private static String generateTableName() {
        return "T" + counter++;
    }

    @Test
    public void testTimeTravelWorks() throws Exception {
        String tbl = generateTableName();
        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");

        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 1");

        long txId = getTxId();

        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 2");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 3");

        // check that data exists
        try(ResultSet rs =  watcher.executeQuery("SELECT * FROM " + tbl + " ORDER BY a ASC")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }

        // travel back in time
        try(ResultSet rs =  watcher.executeQuery("SELECT * FROM " + tbl + " AS OF " + txId + " ORDER BY a ASC")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testTimeTravelMultipleTablesWorks() throws Exception {
        String tbl = generateTableName();

        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 1");
        long txId1 = getTxId();
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 2");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 3");
        // check which transaction we are at the moment after adding both numbers
        long txId2 = getTxId();

        // add noise
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 200");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 300");

        // check that data exists
        try(ResultSet rs =  watcher.executeQuery("SELECT * FROM " + tbl + " ORDER BY a ASC")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getInt(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(200, rs.getInt(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(300, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }

        // 4. travel back in time
        try(ResultSet rs =  watcher.executeQuery("SELECT * FROM " + tbl + " L AS OF " + txId1 + ", "
                + tbl + " R AS OF " + txId2 + " ORDER BY R.a ASC")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals(1, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals(2, rs.getInt(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals(3, rs.getInt(2));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testCreateTableFromTimeTravelSelectWorks() throws Exception {
        String tbl = generateTableName();

        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 1");
        long txId = getTxId();
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 2");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 3");

        String newTbl = generateTableName();
        watcher.executeUpdate("CREATE TABLE " + newTbl + " AS SELECT * FROM " + tbl + " AS OF " + txId);

        try(ResultSet rs = watcher.executeQuery("SELECT * FROM " + newTbl)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testInsertIntoFromTimeTravelSelectWorks() throws Exception {
        String tbl = generateTableName();

        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 1");
        long txId = getTxId();
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 2");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 3");

        watcher.executeUpdate("INSERT INTO " + tbl + " SELECT * FROM " + tbl + " AS OF " + txId);

        try(ResultSet rs = watcher.executeQuery("SELECT * FROM " + tbl + " ORDER BY a ASC")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1)); // added by time-travel
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    // this test should sync up with documentation
    @Test
    public void testTimeTravelAfterTruncateDoesNotWork() throws Exception {
        String tbl = generateTableName();

        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 1");
        long txId = getTxId();
        watcher.executeUpdate("TRUNCATE TABLE " + tbl);

        try(ResultSet rs = watcher.executeQuery("SELECT * FROM " + tbl + " AS OF " + txId)) {
            Assert.assertFalse(rs.next()); // we don't get old rows back after truncate.
        }
    }

    @Test
    public void testTimeTravelWithJoinsWorks() throws  Exception {
        String tblA = generateTableName();
        String tblB = generateTableName();

        watcher.executeUpdate("CREATE TABLE " + tblA + "(id INT, i INT)");
        watcher.executeUpdate("CREATE TABLE " + tblB + "(id INT, c CHAR(1))");

        watcher.executeUpdate("INSERT INTO " + tblA + " VALUES (0, 0)");
        watcher.executeUpdate("INSERT INTO " + tblA + " VALUES (2, 2)");
        long txIdA = getTxId();

        watcher.executeUpdate("INSERT INTO " + tblB + " VALUES (0, 'a')");
        watcher.executeUpdate("INSERT INTO " + tblB + " VALUES (1, 'b')");
        long txIdB = getTxId();

        // add noise
        watcher.executeUpdate("INSERT INTO " + tblA + " VALUES (0, 42)");
        watcher.executeUpdate("INSERT INTO " + tblA + " VALUES (0, 43)");
        watcher.executeUpdate("INSERT INTO " + tblA + " VALUES (1, 44)");
        watcher.executeUpdate("INSERT INTO " + tblB + " VALUES (0, 'x')");
        watcher.executeUpdate("INSERT INTO " + tblB + " VALUES (1, 'y')");
        watcher.executeUpdate("INSERT INTO " + tblB + " VALUES (2, 'z')");

        try (ResultSet rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " INNER JOIN " + tblB +
                " AS OF " + txIdB + " ON " + tblA + ".id = " + tblB + ".id")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getInt(1));
            Assert.assertEquals("a", rs.getString(2));
            Assert.assertFalse(rs.next());
        }

        try (ResultSet rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " FULL JOIN " + tblB +
                " AS OF " + txIdB + " ON " + tblA + ".id = " + tblB + ".id ORDER BY i ASC")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getInt(1));
            Assert.assertEquals("a", rs.getString(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
            rs.getString(2);
            Assert.assertTrue(rs.wasNull());
            Assert.assertTrue(rs.next());
            rs.getInt(1);
            Assert.assertTrue(rs.wasNull());
            Assert.assertEquals("b", rs.getString(2));
            Assert.assertFalse(rs.next());
        }


        try (ResultSet rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " LEFT OUTER JOIN " + tblB +
                " AS OF " + txIdB + " ON " + tblA + ".id = " + tblB + ".id ORDER BY i ASC")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getInt(1));
            Assert.assertEquals("a", rs.getString(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
            rs.getString(2);
            Assert.assertTrue(rs.wasNull());
            Assert.assertFalse(rs.next());
        }

        try (ResultSet rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " RIGHT OUTER JOIN " + tblB +
                " AS OF " + txIdB + " ON " + tblA + ".id = " + tblB + ".id ORDER BY i ASC")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getInt(1));
            Assert.assertEquals("a", rs.getString(2));
            Assert.assertTrue(rs.next());
            rs.getInt(1);
            Assert.assertTrue(rs.wasNull());
            Assert.assertEquals("b", rs.getString(2));
            Assert.assertFalse(rs.next());
        }

        try (ResultSet rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " CROSS JOIN " + tblB +
                " AS OF " + txIdB + " ORDER BY i,c ASC")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getInt(1));
            Assert.assertEquals("a", rs.getString(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getInt(1));
            Assert.assertEquals("b", rs.getString(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
            Assert.assertEquals("a", rs.getString(2));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
            Assert.assertEquals("b", rs.getString(2));
            Assert.assertFalse(rs.next());
        }

        try (ResultSet rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " NATURAL JOIN " + tblB +
                " AS OF " + txIdB + " ORDER BY i,c ASC")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getInt(1));
            Assert.assertEquals("a", rs.getString(2));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testTimeTravelWithUpdatedWorks() throws Exception {
        String tbl = generateTableName();

        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 1");
        long txId = getTxId();
        watcher.executeUpdate("UPDATE " + tbl + " SET a=42");

        try(ResultSet rs = watcher.executeQuery("SELECT * FROM " + tbl + " AS OF " + txId)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }

    }

    @Test
    public void testTimeTravelWithDeleteWorks() throws Exception {
        String tbl = generateTableName();

        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 1");
        long txId = getTxId();
        watcher.executeUpdate("DELETE FROM " + tbl);

        try(ResultSet rs = watcher.executeQuery("SELECT * FROM " + tbl + " AS OF " + txId)){
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testTimeTravelWithSparkOptionWorks() throws Exception {
        String tbl = generateTableName();
        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 1");

        long txId = getTxId();

        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 2");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 3");

        // travel back in time
        String query = "SELECT * FROM " + tbl + " AS OF " + txId + " --splice-properties useSpark=true\n ORDER BY a ASC";
        try(ResultSet rs =  watcher.executeQuery(query)) {
            Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testTimeTravelWithViewIsNotAllowed() throws Exception {
        String tbl = generateTableName();
        String view = generateTableName();
        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");
        watcher.executeUpdate("CREATE VIEW " + view + " AS SELECT * FROM " + tbl);

        try {
            watcher.executeQuery("SELECT * FROM " + view + " AS OF 42");
            fail("Expected: java.sql.SQLException: ERROR 42ZD2: Using time travel clause with views is not allowed");
        } catch (SQLException e) {
            Assert.assertEquals("42ZD2", e.getSQLState());
            return;
        }
        fail("Expected: java.sql.SQLException: ERROR 42ZD2: Using time travel clause with views is not allowed");
    }

    @Test
    public void testTimeTravelWithExternalTablesIsNotAllowed() throws Exception {
        String externalTable = generateTableName();
        String externalTableLocation = tempDir.toString() + "/testTimeTravelWithExternalTablesIsNotAllowed";
        watcher.executeUpdate("CREATE EXTERNAL TABLE " + externalTable + "(a INT, b INT) PARTITIONED BY (a) " +
                "STORED AS PARQUET LOCATION '" + externalTableLocation + "'");

        try {
            watcher.executeQuery("SELECT * FROM " + externalTable + " AS OF 42");
            fail("Expected: java.sql.SQLException: ERROR 42ZD2: Using time travel clause with external tables is not allowed");
        } catch (SQLException e) {
            Assert.assertEquals("42ZD2", e.getSQLState());
            return;
        }
        fail("Expected: java.sql.SQLException: ERROR 42ZD2: Using time travel clause with external tables is not allowed");
    }

    @Test
    public void testTimeTravelWithCommonTableExpressionIsNotAllowed() throws Exception {
        String someTable = generateTableName();
        watcher.executeUpdate("CREATE TABLE " + someTable + "(a INT)");

        try {
            watcher.executeQuery("WITH cte AS (SELECT * FROM " + someTable + ") SELECT * FROM cte AS OF 1234");
            fail("Expected: java.sql.SQLException: ERROR 42ZD2: Using time travel clause with common table expressions is not allowed");
        } catch (SQLException e) {
            Assert.assertEquals("42ZD2", e.getSQLState());
            return;
        }
        fail("Expected: java.sql.SQLException: ERROR 42ZD2: Using time travel clause with common table expressions is not allowed");
    }

    @Test
    public void testTimeTravelWithDistinctScanOperationWorks() throws Exception {
        String someTable = generateTableName();
        watcher.executeUpdate(String.format("CREATE TABLE %s(a1 INT, b1 INT, c1 INT, PRIMARY KEY (a1,b1))", someTable));

        watcher.executeUpdate(String.format("INSERT INTO %s VALUES (1, 1, 1)", someTable));
        long txIdA = getTxId();

        watcher.executeUpdate(String.format("INSERT INTO %s VALUES (1000, 1000, 1000)", someTable));
        try(ResultSet rs =  watcher.executeQuery(String.format("SELECT DISTINCT a1 FROM %s ORDER BY a1 ASC", someTable))) {
            Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
            Assert.assertTrue(rs.next()); Assert.assertEquals(1000, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
        try(ResultSet rs =  watcher.executeQuery(String.format("SELECT DISTINCT a1 FROM %s AS OF %d", someTable, txIdA))) {
            Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testTimeTravelWithLastKeyTableScanOperationWorks() throws Exception {
        String someTable = generateTableName();
        watcher.executeUpdate(String.format("CREATE TABLE %s(a1 INT, b1 INT, c1 INT, PRIMARY KEY (a1,b1))", someTable));

        watcher.executeUpdate(String.format("INSERT INTO %s VALUES (1, 1, 1)", someTable));
        long txIdA = getTxId();

        watcher.executeUpdate(String.format("INSERT INTO %s VALUES (1000, 1000, 1000)", someTable));
        try(ResultSet rs =  watcher.executeQuery(String.format("SELECT MAX(a1) FROM %s", someTable))) {
            Assert.assertTrue(rs.next()); Assert.assertEquals(1000, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
        try(ResultSet rs =  watcher.executeQuery(String.format("SELECT MAX(a1) FROM %s AS OF %d", someTable, txIdA))) {
            Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    private void shouldExceedMrp(String query) {
        try {
            watcher.executeQuery(query);
            Assert.fail("expected exception with the following message: time travel query is outside the minimum retention period of 2 second(s)");
        } catch(Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            SQLException sqlException = (SQLException)e;
            Assert.assertEquals("42ZD7", sqlException.getSQLState());
            Assert.assertTrue(e.getMessage().contains("time travel query is outside the minimum retention period of 2 second(s)"));
        }
    }

    @Test
    public void testTimeTravelOutsideMinRetentionPeriodWorks() throws Exception {
        String someTable = generateTableName();
        watcher.executeUpdate(String.format("CREATE TABLE %s(a1 INT)", someTable));
        int mrp = 2; // seconds
        watcher.execute(String.format("CALL SYSCS_UTIL.SET_MIN_RETENTION_PERIOD('%s', '%s', %d)", SCHEMA, someTable, mrp));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 1", someTable));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 2", someTable));
        Thread.sleep(mrp * 1000 + 300);
        shouldExceedMrp(String.format("SELECT * FROM %s AS OF TIMESTAMP('%s')", someTable,
                new SimpleDateFormat(SpliceDateTimeFormatter.defaultTimestampFormatString).format(new Timestamp(System.currentTimeMillis() - (mrp + 2) * 1000))));
        watcher.execute(String.format("CALL SYSCS_UTIL.SET_MIN_RETENTION_PERIOD('%s', '%s', null)", SCHEMA, someTable));
        try(ResultSet rs = watcher.executeQuery(String.format("SELECT * FROM %s AS OF TIMESTAMP('%s')", someTable,
                new SimpleDateFormat(SpliceDateTimeFormatter.defaultTimestampFormatString).format(new Timestamp(System.currentTimeMillis() - (mrp + 200) * 1000))))) {
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testTimeTravelWithinMinRetentionPeriodWorks() throws Exception {
        String someTable = generateTableName();
        watcher.executeUpdate(String.format("CREATE TABLE %s(a1 INT)", someTable));
        int mrp = 2000; // seconds, large value so we don't run into sporadic test failures due to slow down in query execution .
        watcher.execute(String.format("CALL SYSCS_UTIL.SET_MIN_RETENTION_PERIOD('%s', '%s', %d)", SCHEMA, someTable, mrp));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 1", someTable));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 2", someTable));
        Thread.sleep(1000);
        String timestamp = new SimpleDateFormat(SpliceDateTimeFormatter.defaultTimestampFormatString).format(new Timestamp(System.currentTimeMillis()));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 200", someTable));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 400", someTable));
        try(ResultSet rs = watcher.executeQuery(String.format("SELECT * FROM %s AS OF TIMESTAMP('%s') ORDER BY a1 ASC", someTable, timestamp))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testTimeTravelWithQueryHintWorksCorrectly() throws Exception {
        String someTable = generateTableName();
        watcher.executeUpdate(String.format("CREATE TABLE %s(a1 INT)", someTable));
        int mrp = 2; // seconds
        watcher.execute(String.format("CALL SYSCS_UTIL.SET_MIN_RETENTION_PERIOD('%s', '%s', %d)", SCHEMA, someTable, mrp));
        String initialTime = new SimpleDateFormat(SpliceDateTimeFormatter.defaultTimestampFormatString).format(new Timestamp(System.currentTimeMillis()));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 3", someTable));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 4", someTable));
        Thread.sleep(mrp * 1000 * 2); // we are already beyond MRP for upcoming SELECTs
        shouldExceedMrp(String.format("SELECT * FROM %s AS OF TIMESTAMP('%s')", someTable, initialTime));
        try(ResultSet rs = watcher.executeQuery(String.format("SELECT * FROM %s AS OF TIMESTAMP('%s') --SPLICE-PROPERTIES unboundedTimeTravel=true", someTable, initialTime))) {
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testTimeTravelWithQueryHintsOnDifferentTablesWorks() throws Exception {
        String someTable1 = generateTableName();
        watcher.executeUpdate(String.format("CREATE TABLE %s(a1 INT)", someTable1));
        String someTable2 = generateTableName();
        watcher.executeUpdate(String.format("CREATE TABLE %s(a1 INT)", someTable2));
        int mrp = 2; // seconds
        watcher.execute(String.format("CALL SYSCS_UTIL.SET_MIN_RETENTION_PERIOD('%s', '%s', %d)", SCHEMA, someTable1, mrp));
        watcher.execute(String.format("CALL SYSCS_UTIL.SET_MIN_RETENTION_PERIOD('%s', '%s', %d)", SCHEMA, someTable2, mrp));
        String initialTime = new SimpleDateFormat(SpliceDateTimeFormatter.defaultTimestampFormatString).format(new Timestamp(System.currentTimeMillis()));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 3", someTable1));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 4", someTable1));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 3", someTable2));
        watcher.executeUpdate(String.format("INSERT INTO %s VALUES 4", someTable2));
        Thread.sleep(mrp * 1000 * 2); // we are already beyond MRP for upcoming SELECT
        shouldExceedMrp(String.format("SELECT * FROM %s AS OF TIMESTAMP('%s'), %s AS OF TIMESTAMP('%s')", someTable1, initialTime, someTable2, initialTime));
        shouldExceedMrp(String.format("SELECT * FROM %s AS OF TIMESTAMP('%s'), %s AS OF TIMESTAMP('%s') --SPLICE-PROPERTIES unboundedTimeTravel=true", someTable1, initialTime, someTable2, initialTime));
        shouldExceedMrp(String.format("SELECT * FROM %s AS OF TIMESTAMP('%s') --SPLICE-PROPERTIES unboundedTimeTravel=true\n, %s AS OF TIMESTAMP('%s')", someTable1, initialTime, someTable2, initialTime));
        try(ResultSet rs = watcher.executeQuery(String.format("SELECT * FROM %s AS OF TIMESTAMP('%s') --SPLICE-PROPERTIES unboundedTimeTravel=true\n, %s AS OF TIMESTAMP('%s') --SPLICE-PROPERTIES unboundedTimeTravel=true",
                someTable1, initialTime, someTable2, initialTime))) {
            Assert.assertFalse(rs.next());
        }
    }
}
