package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.fail;

public class SelectTimeTravelIT {

    private static final String SCHEMA = SelectTimeTravelIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceWatcher watcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    private long getTxId() throws SQLException {
        long result = -1;
        ResultSet rs =  watcher.executeQuery("CALL SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
        Assert.assertTrue(rs.next());
        result = rs.getLong(1);
        Assert.assertFalse(rs.next());
        return result;
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
        ResultSet rs =  watcher.executeQuery("SELECT * FROM " + tbl + " ORDER BY a ASC");
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
        Assert.assertTrue(rs.next()); Assert.assertEquals(2, rs.getInt(1));
        Assert.assertTrue(rs.next()); Assert.assertEquals(3, rs.getInt(1));
        Assert.assertFalse(rs.next());

        // travel back in time
        rs =  watcher.executeQuery("SELECT * FROM " + tbl + " AS OF " + txId + " ORDER BY a ASC");
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
        Assert.assertFalse(rs.next());
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
        ResultSet rs =  watcher.executeQuery("SELECT * FROM " + tbl + " ORDER BY a ASC");
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
        Assert.assertTrue(rs.next()); Assert.assertEquals(2, rs.getInt(1));
        Assert.assertTrue(rs.next()); Assert.assertEquals(3, rs.getInt(1));
        Assert.assertTrue(rs.next()); Assert.assertEquals(200, rs.getInt(1));
        Assert.assertTrue(rs.next()); Assert.assertEquals(300, rs.getInt(1));
        Assert.assertFalse(rs.next());

        // 4. travel back in time
        rs =  watcher.executeQuery("SELECT * FROM " + tbl + " L AS OF " + txId1 + ", "
                + tbl + " R AS OF " + txId2 + " ORDER BY R.a ASC");
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1)); Assert.assertEquals(1, rs.getInt(2));
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1)); Assert.assertEquals(2, rs.getInt(2));
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1)); Assert.assertEquals(3, rs.getInt(2));
        Assert.assertFalse(rs.next());
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

        ResultSet rs = watcher.executeQuery("SELECT * FROM " + newTbl);
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
        Assert.assertFalse(rs.next());
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

        ResultSet rs = watcher.executeQuery("SELECT * FROM " + tbl + " ORDER BY a ASC");
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1)); // added by time-travel
        Assert.assertTrue(rs.next()); Assert.assertEquals(2, rs.getInt(1));
        Assert.assertTrue(rs.next()); Assert.assertEquals(3, rs.getInt(1));
        Assert.assertFalse(rs.next());
    }

    // this test should sync up with documentation
    @Test
    public void testTimeTravelAfterTruncateDoesNotWork() throws Exception {
        String tbl = generateTableName();

        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 1");
        long txId = getTxId();
        watcher.executeUpdate("TRUNCATE TABLE " + tbl);

        ResultSet rs = watcher.executeQuery("SELECT * FROM " + tbl + " AS OF " + txId);
        Assert.assertFalse(rs.next()); // we don't get old rows back after truncate.
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

        ResultSet rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " INNER JOIN " + tblB +
                " AS OF " + txIdB +" ON " + tblA + ".id = " + tblB + ".id");
        Assert.assertTrue(rs.next()); Assert.assertEquals(0, rs.getInt(1));Assert.assertEquals("a", rs.getString(2));
        Assert.assertFalse(rs.next());

        rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " FULL JOIN " + tblB +
                " AS OF " + txIdB +" ON " + tblA + ".id = " + tblB + ".id ORDER BY i ASC");
        Assert.assertTrue(rs.next()); Assert.assertEquals(0, rs.getInt(1));Assert.assertEquals("a", rs.getString(2));
        Assert.assertTrue(rs.next()); Assert.assertEquals(2, rs.getInt(1));rs.getString(2); Assert.assertTrue(rs.wasNull());
        Assert.assertTrue(rs.next()); rs.getInt(1); Assert.assertTrue(rs.wasNull());Assert.assertEquals("b", rs.getString(2));
        Assert.assertFalse(rs.next());

        rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " LEFT OUTER JOIN " + tblB +
                " AS OF " + txIdB +" ON " + tblA + ".id = " + tblB + ".id ORDER BY i ASC");
        Assert.assertTrue(rs.next()); Assert.assertEquals(0, rs.getInt(1));Assert.assertEquals("a", rs.getString(2));
        Assert.assertTrue(rs.next()); Assert.assertEquals(2, rs.getInt(1));rs.getString(2); Assert.assertTrue(rs.wasNull());
        Assert.assertFalse(rs.next());

        rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " RIGHT OUTER JOIN " + tblB +
                " AS OF " + txIdB +" ON " + tblA + ".id = " + tblB + ".id ORDER BY i ASC");
        Assert.assertTrue(rs.next()); Assert.assertEquals(0, rs.getInt(1));Assert.assertEquals("a", rs.getString(2));
        Assert.assertTrue(rs.next()); rs.getInt(1); Assert.assertTrue(rs.wasNull());Assert.assertEquals("b", rs.getString(2));
        Assert.assertFalse(rs.next());

        rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " CROSS JOIN " + tblB +
                " AS OF " + txIdB + " ORDER BY i,c ASC");
        Assert.assertTrue(rs.next()); Assert.assertEquals(0, rs.getInt(1));Assert.assertEquals("a", rs.getString(2));
        Assert.assertTrue(rs.next()); Assert.assertEquals(0, rs.getInt(1));Assert.assertEquals("b", rs.getString(2));
        Assert.assertTrue(rs.next()); Assert.assertEquals(2, rs.getInt(1));Assert.assertEquals("a", rs.getString(2));
        Assert.assertTrue(rs.next()); Assert.assertEquals(2, rs.getInt(1));Assert.assertEquals("b", rs.getString(2));
        Assert.assertFalse(rs.next());

        rs = watcher.executeQuery("SELECT i,c FROM " + tblA + " AS OF " + txIdA + " NATURAL JOIN " + tblB +
                " AS OF " + txIdB + " ORDER BY i,c ASC");
        Assert.assertTrue(rs.next()); Assert.assertEquals(0, rs.getInt(1));Assert.assertEquals("a", rs.getString(2));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testTimeTravelWithUpdatedWorks() throws Exception {
        String tbl = generateTableName();

        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 1");
        long txId = getTxId();
        watcher.executeUpdate("UPDATE " + tbl + " SET a=42");

        ResultSet rs = watcher.executeQuery("SELECT * FROM " + tbl + " AS OF " + txId);
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testTimeTravelWithDeleteWorks() throws Exception {
        String tbl = generateTableName();

        watcher.executeUpdate("CREATE TABLE " + tbl + "(a INT)");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES 1");
        long txId = getTxId();
        watcher.executeUpdate("DELETE FROM " + tbl);

        ResultSet rs = watcher.executeQuery("SELECT * FROM " + tbl + " AS OF " + txId);
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
        Assert.assertFalse(rs.next());
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
        ResultSet rs =  watcher.executeQuery(query);
        Assert.assertTrue(rs.next()); Assert.assertEquals(1, rs.getInt(1));
        Assert.assertFalse(rs.next());
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
        watcher.executeUpdate("CREATE EXTERNAL TABLE " + externalTable + "(a INT, b INT) PARTITIONED BY (a) " +
                "STORED AS PARQUET LOCATION '/tmp/bla'");

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
}
