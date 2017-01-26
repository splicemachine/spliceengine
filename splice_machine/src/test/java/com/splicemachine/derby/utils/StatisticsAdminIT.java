/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
@Category(SerialTest.class)
public class StatisticsAdminIT{
    private static final String SCHEMA=StatisticsAdminIT.class.getSimpleName().toUpperCase();
    private static final String SCHEMA2=SCHEMA+"2";
    private static final String SCHEMA3=SCHEMA+"3";

    private static final SpliceWatcher spliceClassWatcher=new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(SCHEMA);
    private static final SpliceWatcher spliceClassWatcher2=new SpliceWatcher(SCHEMA2);
    private static final SpliceSchemaWatcher spliceSchemaWatcher2=new SpliceSchemaWatcher(SCHEMA2);
    private static final SpliceWatcher spliceClassWatcher3=new SpliceWatcher(SCHEMA3);
    private static final SpliceSchemaWatcher spliceSchemaWatcher3=new SpliceSchemaWatcher(SCHEMA3);

    private static final String TABLE_EMPTY="EMPTY";
    private static final String TABLE_OCCUPIED="OCCUPIED";
    private static final String TABLE_OCCUPIED2="OCCUPIED2";
    private static final String MIXED_CASE_TABLE="MixedCaseTable";
    private static final String MIXED_CASE_SCHEMA="MixedCaseSchema";
    private static final String UPDATE="UP_TABLE";
    private static final String WITH_NULLS_NUMERIC = "WITH_NULLS_NUMERIC";

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @ClassRule
    public static TestRule chain2=RuleChain.outerRule(spliceClassWatcher2)
            .around(spliceSchemaWatcher2);

    @ClassRule
    public static TestRule chain3=RuleChain.outerRule(spliceClassWatcher3)
            .around(spliceSchemaWatcher3);

    @Rule
    public final SpliceWatcher methodWatcher=new SpliceWatcher(SCHEMA);

    @Rule
    public final SpliceWatcher methodWatcher2=new SpliceWatcher(SCHEMA2);

    @Rule
    public final SpliceWatcher methodWatcher3=new SpliceWatcher(SCHEMA3);

    @BeforeClass
    public static void createSharedTables() throws Exception{

        Connection conn=spliceClassWatcher.getOrCreateConnection();
        doCreateSharedTables(conn);

        Connection conn2=spliceClassWatcher2.getOrCreateConnection();
        doCreateSharedTables(conn2);

        Connection conn3=spliceClassWatcher3.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table \""+MIXED_CASE_TABLE+"\" (a int)")
                .create();

        new TableCreator(conn3)
                .withCreate("create table "+WITH_NULLS_NUMERIC+" " +
                        "(id int, mybigint BIGINT, mydecimal DECIMAL(5,2), " +
                        "mydec DEC(7,5), mydouble DOUBLE, mydoublep DOUBLE PRECISION, " +
                        "myfloat FLOAT, myinteger INTEGER, mynumeric NUMERIC, myreal REAL, " +
                        "mysmallint SMALLINT)")
                .create();


    }

    private static void doCreateSharedTables(Connection conn) throws Exception{

        new TableCreator(conn)
                .withCreate("create table "+TABLE_OCCUPIED+" (a int)")
                .withInsert("insert into "+TABLE_OCCUPIED+" (a) values (?)")
                .withIndex("create index idx_o on "+TABLE_OCCUPIED+" (a)")
                .withRows(rows(row(1)))
                .create();

        new TableCreator(conn)
                .withCreate("create table "+TABLE_OCCUPIED2+" (a int)")
                .withInsert("insert into "+TABLE_OCCUPIED2+" (a) values (?)")
                .withIndex("create index idx_o2 on "+TABLE_OCCUPIED2+" (a)")
                .withRows(rows(row(101)))
                .create();

        new TableCreator(conn)
                .withCreate("create table "+TABLE_EMPTY+" (a int)")
                .create();

        new TableCreator(conn)
                .withCreate("create table "+UPDATE+" (a int, b int)")
                .withInsert("insert into "+UPDATE+" (a,b) values (1,1)")
                .create();
    }

    @Test
    public void testTableStatisticsAreCorrectAfterUpdate() throws Exception{
        /*
         * Regression test for SPLICE-856. Confirms that updates don't break the statistics
         * scanning
         */
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        try(Statement s = conn.createStatement()){
            s.executeUpdate("update "+UPDATE+" set b = a");
        }

        try(CallableStatement cs = conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?)")){
            cs.setString(1,SCHEMA);
            cs.setString(2,UPDATE);
            cs.setBoolean(3, false);
            cs.execute();

        }
        try(PreparedStatement ps = conn.prepareStatement("select null_count,null_fraction from sys.syscolumnstatistics where schemaname = ? and tablename = ?")){
            ps.setString(1,SCHEMA);
            ps.setString(2,UPDATE);

            try(ResultSet rs = ps.executeQuery()){
                int countDown = 2; //there should only be 2 rows returned
                while(rs.next()){
                    countDown--;
                    Assert.assertTrue("Too many rows returned!",countDown>=0);

                    Assert.assertEquals("Incorrect null count!",0,rs.getLong(1));
                    Assert.assertFalse("Did not return a value for null count!",rs.wasNull());

                    Assert.assertEquals("Incorrect null fraction!",0d,rs.getDouble(2),0d);
                    Assert.assertFalse("Did not return a value for null fraction!",rs.wasNull());
                }
                Assert.assertEquals("Not enough rows returned!",0,countDown);
            }
        }
    }

    @Test
    public void testTableStatisticsAreCorrectForEmptyTable() throws Exception{
        TestConnection conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        CallableStatement callableStatement=conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?)");
        callableStatement.setString(1,SCHEMA);
        callableStatement.setString(2,TABLE_EMPTY);
        callableStatement.setBoolean(3,false);
        callableStatement.execute();

        /*
         * Now we need to make sure that the statistics were properly recorded.
         *
         * There are 2 tables in particular of interest: the Column stats, and the row stats. Since
         * the emptyTable is not configured to collect any column stats, we only check the row stats for
         * values.
         */
        long conglomId=conn.getConglomNumbers(SCHEMA,TABLE_EMPTY)[0];
        PreparedStatement check=conn.prepareStatement("select * from sys.systablestats where conglomerateId = ?");
        check.setLong(1,conglomId);
        ResultSet resultSet=check.executeQuery();
        Assert.assertTrue("Unable to find statistics for table!",resultSet.next());
        Assert.assertEquals("Incorrect row count!",0l,resultSet.getLong(6));
        Assert.assertEquals("Incorrect partition size!",0l,resultSet.getLong(7));
        Assert.assertEquals("Incorrect row width!",0l,resultSet.getInt(8));

        conn.rollback();
        conn.reset();
    }

    @Test
    public void testTableStatisticsCorrectForOccupiedTable() throws Exception{
        TestConnection conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        try (CallableStatement callableStatement=conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?)")) {
            callableStatement.setString(1, SCHEMA);
            callableStatement.setString(2, TABLE_OCCUPIED);
            callableStatement.setBoolean(3, false);
            callableStatement.execute();
        }

        /*
         * Now we need to make sure that the statistics were properly recorded.
         *
         * There are 2 tables in particular of interest: the Column stats, and the row stats. Since
         * the emptyTable is not configured to collect any column stats, we only check the row stats for
         * values.
         */
        long conglomId=conn.getConglomNumbers(SCHEMA,TABLE_OCCUPIED)[0];
        try (PreparedStatement check=conn.prepareStatement("select * from sys.systablestats where conglomerateId = ?")) {
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
            long partitionSize=resultSet.getLong(7);
            long rowWidth=resultSet.getLong(8);
            Assert.assertTrue("partition size != row width!",partitionSize==rowWidth);
        }

        conn.rollback();
        conn.reset();
    }

    private static String checkEnabledQuery =
        "select c.collectstats from "+
            "sys.sysschemas s, sys.systables t, sys.syscolumns c "+
            "where s.schemaid = t.schemaid "+
            "and t.tableid = c.referenceid "+
            "and s.schemaname = ?"+
            "and t.tablename = ?"+
            "and c.columnname = ?";

    @Test
    public void testCanEnableColumnStatistics() throws Exception{
        TestConnection conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        try (CallableStatement cs=conn.prepareCall("call SYSCS_UTIL.ENABLE_COLUMN_STATISTICS(?,?,?)")) {
            cs.setString(1, SCHEMA);
            cs.setString(2, TABLE_EMPTY);
            cs.setString(3, "A");
            cs.execute();
        }

        //make sure it's enabled
        PreparedStatement ps=conn.prepareStatement(checkEnabledQuery);
        ps.setString(1, SCHEMA);
        ps.setString(2, TABLE_EMPTY);
        ps.setString(3, "A");

        try (ResultSet resultSet = ps.executeQuery()) {
            Assert.assertTrue("No columns found!", resultSet.next());
            boolean statsEnabled = (Boolean)resultSet.getObject(1);
            Assert.assertTrue("Stats were not enabled!", statsEnabled);
        }

        //now verify that it can be disabled as well
        try (CallableStatement cs=conn.prepareCall("call SYSCS_UTIL.DISABLE_COLUMN_STATISTICS(?,?,?)")) {
            cs.setString(1, SCHEMA);
            cs.setString(2, TABLE_EMPTY);
            cs.setString(3, "A");
            cs.execute();
        }

        ps=conn.prepareStatement(checkEnabledQuery);
        ps.setString(1, SCHEMA);
        ps.setString(2, TABLE_EMPTY);
        ps.setString(3, "A");

        //make sure it's disabled
        try (ResultSet resultSet = ps.executeQuery()) {
            Assert.assertTrue("No columns found!", resultSet.next());
            boolean statsEnabled = (Boolean)resultSet.getObject(1);
            Assert.assertFalse("Stats were still enabled!", statsEnabled);
        }

        conn.rollback();
        conn.reset();
    }


    @Test
    public void testNullFractionCalculation() throws Exception {
        TestConnection conn = methodWatcher3.getOrCreateConnection();
        methodWatcher3.executeUpdate("insert into "+WITH_NULLS_NUMERIC + " values " +
                "(null, null, 123.45, 12.34567, 1.79769E+308, 2.225E-307, 1.79769E+308, -2147483648, " +
                "123.456, 3.402E+38, 32767)");
        methodWatcher3.executeUpdate("insert into "+WITH_NULLS_NUMERIC + " values " +
                "(null, -9223372036854775808, null, 1.34567, -1.79769E+308, -2.225E-307, -1.79769E+308," +
                " 2147483647, 0.456, -1.175E-37, -32768)");
        methodWatcher3.executeUpdate("insert into "+WITH_NULLS_NUMERIC + " values " +
                "(null, 36854775808, 1.2, 1.12, -1.79769E+308, -2.225E-307, -1.79769E+308, 2147483647, 0.456, " +
                "-1.175E-37, -32768)");
        methodWatcher3.executeUpdate("insert into "+WITH_NULLS_NUMERIC + " values " +
                "(null, null, null, null, null, null, null, null, null, null, null)");
        try(CallableStatement cs = conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,false)")){
            cs.setString(1,spliceSchemaWatcher3.schemaName);
            cs.setString(2,"\""+WITH_NULLS_NUMERIC+"\"");
            cs.execute();
        }

        ResultSet rs = methodWatcher3.executeQuery("select null_fraction from sys.SYSCOLUMNSTATISTICS where TABLENAME like '"+WITH_NULLS_NUMERIC+"' and SCHEMANAME like '" + spliceSchemaWatcher3.schemaName+ "' and columnname='MYFLOAT'");
        Assert.assertTrue("statistics missing",rs.next());
        Assert.assertEquals("statistics missing",0.25f,rs.getFloat(1),0.000001f);
    }

    @Test
    public void testDropSchemaStatistics() throws Exception {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        TestConnection conn2 = methodWatcher2.getOrCreateConnection();
        conn2.setAutoCommit(false);

        try (CallableStatement callableStatement = conn.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,?)")) {
            callableStatement.setString(1, SCHEMA);
            callableStatement.setBoolean(2, false);
            callableStatement.execute();
        }

        try (CallableStatement cs2=conn2.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,?)")) {
            cs2.setString(1, SCHEMA2);
            cs2.setBoolean(2, false);
            cs2.execute();
        }

        // Check collected stats for both schemas
        verifyStatsCounts(conn,SCHEMA,null,5,6);
        verifyStatsCounts(conn2,SCHEMA2,null,4,5);

        // Drop stats for schema 1
        try (CallableStatement callableStatement = conn.prepareCall("call SYSCS_UTIL.DROP_SCHEMA_STATISTICS(?)")) {
            callableStatement.setString(1, SCHEMA);
            callableStatement.execute();
        }

        // Make sure only schema 1 stats were dropped, not schema 2 stats
        verifyStatsCounts(conn,SCHEMA,null,0,0);
        verifyStatsCounts(conn2,SCHEMA2,null,4,5);

        // Drop stats again for schema 1 to make sure it works with no stats
        try (CallableStatement callableStatement = conn.prepareCall("call SYSCS_UTIL.DROP_SCHEMA_STATISTICS(?)")) {
            callableStatement.setString(1,SCHEMA);
            callableStatement.execute();
        }

        // Make sure only schema 1 stats were dropped, not schema 2 stats
        verifyStatsCounts(conn,SCHEMA,null,0,0);
        verifyStatsCounts(conn2,SCHEMA2,null,4,5);

        // Drop stats for schema 2
        try (CallableStatement cs2=conn2.prepareCall("call SYSCS_UTIL.DROP_SCHEMA_STATISTICS(?)")) {
            cs2.setString(1, SCHEMA2);
            cs2.execute();
        }

        // Make sure stats are gone for both schemas
        verifyStatsCounts(conn,SCHEMA,null,0,0);
        verifyStatsCounts(conn2,SCHEMA2,null,0,0);

        conn2.rollback();
        conn2.reset();

        conn.rollback();
        conn.reset();
    }

    @Test
    public void testDropTableStatistics() throws Exception{
        TestConnection conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        TestConnection conn2=methodWatcher2.getOrCreateConnection();
        conn2.setAutoCommit(false);

        try (CallableStatement callableStatement=conn.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,?)")) {
            callableStatement.setString(1, SCHEMA);
            callableStatement.setBoolean(2, false);
            callableStatement.execute();
        }

        try (CallableStatement cs2=conn2.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,?)")) {
            cs2.setString(1, SCHEMA2);
            cs2.setBoolean(2, false);
            cs2.execute();
        }
        // Check collected stats for both schemas
        verifyStatsCounts(conn,SCHEMA,null,5,6);
        verifyStatsCounts(conn2,SCHEMA2,null,4,5);

        // Drop stats for schema 1, table 1
        try (CallableStatement callableStatement=conn.prepareCall("call SYSCS_UTIL.DROP_TABLE_STATISTICS(?,?)")) {
            callableStatement.setString(1, SCHEMA);
            callableStatement.setString(2, TABLE_OCCUPIED);
            callableStatement.execute();
        }

        // Make sure stats for both table and index were dropped in schema 1.
        verifyStatsCounts(conn,SCHEMA,null,4,5);
        verifyStatsCounts(conn,SCHEMA,TABLE_OCCUPIED,0,0);
        verifyStatsCounts(conn2,SCHEMA2,null,4,5);

        // Drop stats again for schema 1 to make sure it works with no stats
        try (CallableStatement callableStatement=conn.prepareCall("call SYSCS_UTIL.DROP_TABLE_STATISTICS(?,?)")) {
            callableStatement.setString(1, SCHEMA);
            callableStatement.setString(2, TABLE_OCCUPIED);
            callableStatement.execute();
        }

        // Same as prior check
        verifyStatsCounts(conn,SCHEMA,null,4,5);
        verifyStatsCounts(conn,SCHEMA,TABLE_OCCUPIED,0,0);
        verifyStatsCounts(conn2,SCHEMA2,null,4,5);

        conn2.rollback();
        conn2.reset();

        conn.rollback();
        conn.reset();
    }

    @Test
    public void canCollectOnMixedCaseTable() throws Exception{
        /*
         * DB-4184 Regression test. Just make sure that we don't get any errors.
         */
        TestConnection conn=methodWatcher.getOrCreateConnection();

        try(CallableStatement cs = conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,false)")){
            cs.setString(1,spliceSchemaWatcher.schemaName);
            cs.setString(2,"\""+MIXED_CASE_TABLE+"\"");

            cs.execute();
        }
    }

    @Test
    public void canCollectOnMixedCaseSchema() throws Exception{
        /*
         * DB-4184 Regression test. Just make sure that we don't get any errors.
         */
        TestConnection conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        try(Statement s = conn.createStatement()){
            s.execute("create schema \""+MIXED_CASE_SCHEMA+"\"");

            try(CallableStatement cs = conn.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
                cs.setString(1,"\""+MIXED_CASE_SCHEMA+"\"");

                cs.execute();
            }
        }finally{
            conn.rollback();
        }

    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void verifyStatsCounts(Connection conn,String schema,String table,int tableStatsCount,int colStatsCount) throws Exception{
        try (
            PreparedStatement check = (table == null) ?
            conn.prepareStatement("select count(*) from sys.systablestatistics where schemaname = ?"):
            conn.prepareStatement("select count(*) from sys.systablestatistics where schemaname = ? and tablename = ?")) {

            check.setString(1, schema);
            if (table != null) check.setString(2, table);
            ResultSet resultSet = check.executeQuery();
            Assert.assertTrue("Unable to count stats for schema", resultSet.next());
            int rowCount = resultSet.getInt(1);
            Assert.assertEquals("Incorrect row count", tableStatsCount, rowCount);
        }

        try (
            PreparedStatement check2=(table==null)?
            conn.prepareStatement("select count(*) from sys.syscolumnstatistics where schemaname = ?"):
            conn.prepareStatement("select count(*) from sys.syscolumnstatistics where schemaname = ? and tablename = ?")) {

            check2.setString(1, schema);
            if (table != null) check2.setString(2, table);
            ResultSet resultSet2 = check2.executeQuery();
            Assert.assertTrue("Unable to count stats for schema", resultSet2.next());
            int rowCount = resultSet2.getInt(1);
            resultSet2.close();
            Assert.assertEquals("Incorrect row count", colStatsCount, rowCount);
        }
    }
}
