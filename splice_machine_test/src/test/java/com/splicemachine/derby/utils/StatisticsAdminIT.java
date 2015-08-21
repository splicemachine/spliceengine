package com.splicemachine.derby.utils;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
@Category(SerialTest.class)
public class StatisticsAdminIT {
	private static final String SCHEMA = StatisticsAdminIT.class.getSimpleName().toUpperCase();

    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    private static final String TABLE_EMPTY = "EMPTY";
    private static final String TABLE_OCCUPIED = "OCCUPIED";
    
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
    	.around(spliceSchemaWatcher);

    @Rule
    public final SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {

        Connection conn = spliceClassWatcher.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table " + TABLE_OCCUPIED + " (a int)")
                .withInsert("insert into " + TABLE_OCCUPIED + " (a) values (?)")
                .withRows(rows(row(1)))
                .create();

        new TableCreator(conn)
                .withCreate("create table " + TABLE_EMPTY + " (a int)")
                .create();
    }

    @Test
    public void testTableStatisticsAreCorrectForEmptyTable() throws Exception {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        CallableStatement callableStatement = conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?)");
        callableStatement.setString(1, SCHEMA);
        callableStatement.setString(2, TABLE_EMPTY);
        callableStatement.setBoolean(3, false);

        callableStatement.execute();
        /*
         * Now we need to make sure that the statistics were properly recorded.
         *
         * There are 2 tables in particular of interest: the Column stats, and the row stats. Since
         * the emptyTable is not configured to collect any column stats, we only check the row stats for
         * values.
         */
        long conglomId = SpliceAdmin.getConglomNumbers(conn, SCHEMA, TABLE_EMPTY)[0];
        PreparedStatement check = conn.prepareStatement("select * from sys.systablestats where conglomerateId = ?");
        check.setLong(1, conglomId);
        ResultSet resultSet = check.executeQuery();
        Assert.assertTrue("Unable to find statistics for table!", resultSet.next());
        Assert.assertEquals("Incorrect row count!", 0l, resultSet.getLong(6));
        Assert.assertEquals("Incorrect partition size!",0l,resultSet.getLong(7));
        Assert.assertEquals("Incorrect row width!",0l,resultSet.getInt(8));

		conn.rollback();
		conn.reset();
    }

    @Test
    public void testTableStatisticsCorrectForOccupiedTable() throws Exception {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        CallableStatement callableStatement = conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?)");
        callableStatement.setString(1, SCHEMA);
        callableStatement.setString(2, TABLE_OCCUPIED);
        callableStatement.setBoolean(3, false);

        callableStatement.execute();
        /*
         * Now we need to make sure that the statistics were properly recorded.
         *
         * There are 2 tables in particular of interest: the Column stats, and the row stats. Since
         * the emptyTable is not configured to collect any column stats, we only check the row stats for
         * values.
         */
        long conglomId = SpliceAdmin.getConglomNumbers(conn, SCHEMA, TABLE_OCCUPIED)[0];
        PreparedStatement check = conn.prepareStatement("select * from sys.systablestats where conglomerateId = ?");
        check.setLong(1, conglomId);
        ResultSet resultSet = check.executeQuery();
        Assert.assertTrue("Unable to find statistics for table!", resultSet.next());
        Assert.assertEquals("Incorrect row count!", 1l, resultSet.getLong(6));
        /*
         * We would love to assert specifics about the size of the partition and the width
         * of the row, but doing so results in a fragile test--the size of the row changes after the
         * transaction system performed read resolution, so if you wait for long enough (i.e. have a slow
         * enough system) this test will end up breaking. However, we do know that there is only a single
         * row in this table, so the partition size should be the same as the avgRowWidth
         */
        long partitionSize = resultSet.getLong(7);
        long rowWidth = resultSet.getLong(8);
        Assert.assertTrue("partition size != row width!",partitionSize==rowWidth);

        conn.rollback();
		conn.reset();
    }

    @Test
    public void testCanEnableColumnStatistics() throws Exception {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        CallableStatement cs = conn.prepareCall("call SYSCS_UTIL.ENABLE_COLUMN_STATISTICS(?,?,?)");
        cs.setString(1, SCHEMA);
        cs.setString(2, TABLE_EMPTY);
        cs.setString(3, "A");

        cs.execute(); //shouldn't get an error
 
        //make sure it's enabled
        PreparedStatement ps = conn.prepareStatement(
    		"select c.* from " +
            "sys.sysschemas s, sys.systables t, sys.syscolumns c " +
            "where s.schemaid = t.schemaid " +
            "and t.tableid = c.referenceid " +
            "and s.schemaname = ?" +
            "and t.tablename = ?" +
            "and c.columnname = ?");
        ps.setString(1, SCHEMA);
        ps.setString(2, TABLE_EMPTY);
        ps.setString(3, "A");
        ResultSet resultSet = ps.executeQuery();
        Assert.assertTrue("No columns found!",resultSet.next());
        boolean statsEnabled = resultSet.getBoolean("collectstats");
        Assert.assertTrue("Stats were not enabled!",statsEnabled);
        resultSet.close();

        //now verify that it can be disabled as well
        cs.close();
        cs = conn.prepareCall("call SYSCS_UTIL.DISABLE_COLUMN_STATISTICS(?,?,?)");
        cs.setString(1, SCHEMA);
        cs.setString(2, TABLE_EMPTY);
        cs.setString(3, "A");

        cs.execute(); //shouldn't get an error
        cs.close();

        //make sure it's disabled
        resultSet = ps.executeQuery();
        Assert.assertTrue("No columns found!",resultSet.next());
        statsEnabled = resultSet.getBoolean("collectstats");
        Assert.assertFalse("Stats were still enabled!",statsEnabled);
        resultSet.close();

        conn.rollback();
		conn.reset();
    }
}
