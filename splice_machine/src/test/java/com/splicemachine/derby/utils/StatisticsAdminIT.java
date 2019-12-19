/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.db.impl.sql.catalog.SYSTABLESTATISTICSRowFactory;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.test.HBaseTest;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
@Category(SerialTest.class)
public class StatisticsAdminIT extends SpliceUnitTest {
    private static final String SCHEMA=StatisticsAdminIT.class.getSimpleName().toUpperCase();
    private static final String SCHEMA2=SCHEMA+"2";
    private static final String SCHEMA3=SCHEMA+"3";
    private static final String SCHEMA4=SCHEMA+"4";

    private static final SpliceWatcher spliceClassWatcher=new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(SCHEMA);
    private static final SpliceWatcher spliceClassWatcher2=new SpliceWatcher(SCHEMA2);
    private static final SpliceSchemaWatcher spliceSchemaWatcher2=new SpliceSchemaWatcher(SCHEMA2);
    private static final SpliceWatcher spliceClassWatcher3=new SpliceWatcher(SCHEMA3);
    private static final SpliceSchemaWatcher spliceSchemaWatcher3=new SpliceSchemaWatcher(SCHEMA3);
    private static final SpliceWatcher spliceClassWatcher4=new SpliceWatcher(SCHEMA4);
    private static final SpliceSchemaWatcher spliceSchemaWatcher4=new SpliceSchemaWatcher(SCHEMA4);

    private static final String TABLE_EMPTY="EMPTY";
    private static final String TABLE_OCCUPIED="OCCUPIED";
    private static final String TABLE_OCCUPIED2="OCCUPIED2";
    private static final String MIXED_CASE_TABLE="MixedCaseTable";
    private static final String MIXED_CASE_SCHEMA="MixedCaseSchema";
    private static final String UPDATE="UP_TABLE";
    private static final String WITH_NULLS_NUMERIC = "WITH_NULLS_NUMERIC";
    private static final String TABLE_EMPTY1="EMPTY1";

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @ClassRule
    public static TestRule chain2=RuleChain.outerRule(spliceClassWatcher2)
            .around(spliceSchemaWatcher2);

    @ClassRule
    public static TestRule chain3=RuleChain.outerRule(spliceClassWatcher3)
            .around(spliceSchemaWatcher3);

    @ClassRule
    public static TestRule chain4=RuleChain.outerRule(spliceClassWatcher4)
            .around(spliceSchemaWatcher4);

    @Rule
    public final SpliceWatcher methodWatcher=new SpliceWatcher(SCHEMA);

    @Rule
    public final SpliceWatcher methodWatcher2=new SpliceWatcher(SCHEMA2);

    @Rule
    public final SpliceWatcher methodWatcher3=new SpliceWatcher(SCHEMA3);

    @Rule
    public final SpliceWatcher methodWatcher4=new SpliceWatcher(SCHEMA4);

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

        Connection conn4=spliceClassWatcher4.getOrCreateConnection();

        new TableCreator(conn4)
                .withCreate("create table t1(a1 int, b1 int, c1 int)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(3,3,3),
                        row(4,4,4),
                        row(5,5,5),
                        row(6,6,6),
                        row(7,7,7),
                        row(8,8,8),
                        row(9,9,9),
                        row(10,10,10)))
                .create();

        for (int i = 0; i < 2; i++) {
            spliceClassWatcher4.executeUpdate("insert into t1 select * from t1");
        }

        new TableCreator(conn4)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, constraint con1 primary key (a2))")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,1,1),
                        row(3,1,1),
                        row(4,1,1),
                        row(5,1,1),
                        row(6,2,2),
                        row(7,2,2),
                        row(8,2,2),
                        row(9,2,2),
                        row(10,2,2)))
                .create();

        int factor = 10;
        for (int i = 1; i <= 12; i++) {
            spliceClassWatcher4.executeUpdate(format("insert into t2 select a2+%d, b2,c2 from t2", factor));
            factor = factor * 2;
        }

        new TableCreator(conn4)
                .withCreate("create table "+TABLE_EMPTY1+" (a1 int, b1 int, c1 int, d1 int, e1 int, f1 int)")
                .create();

        new TableCreator(conn4)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, d3 int, e3 int, f3 int, primary key (b3,c3))")
                .withIndex("create index ind_t3_1 on t3(a3, e3)")
                .withInsert("insert into t3 values (?,?,?,?,?,?)")
                .withRows(rows(
                        row(1,1,1,1,1,1),
                        row(2,2,2,2,2,2),
                        row(3,3,3,3,3,3)))
                .create();

        new TableCreator(conn4)
                .withCreate("create table t4 (a4 varchar(3), b4 varchar(3), primary key(a4))")
                .withInsert("insert into t4 values (?,?)")
                .withRows(rows(
                        row("",""),
                        row("A","A"),
                        row("F",""),
                        row("B","B"),
                        row("G",""),
                        row("C","C"),
                        row("H",""),
                        row("D","D"),
                        row("I",""),
                        row("E","E")))
                .create();

        new TableCreator(conn4)
                .withCreate("create table t5 (a5 tinyint, b5 smallint, c5 int, d5 bigint, e5 real, f5 double, g5 decimal(10,2), " +
                        "h5 date, i5 timestamp, j5 time, k5 varchar(10),  primary key(c5))")
                .withInsert("insert into t5 values (?,?,?,?,?,?,?,?,?,?,?)")
                .withRows(rows(
                        row(1,1,1,1,1.0,1.0,10.01,"2018-12-12","2013-03-23 09:45:00", "15:09:02", "AAAA")))
                .create();


        new TableCreator(conn4)
                .withCreate("create table t6 (a6 int, b6 int, c6 int, primary key (a6))")
                .withInsert("insert into t6 values (?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(3,3,3),
                        row(4,4,4),
                        row(5,5,5),
                        row(6,6,6),
                        row(7,7,7),
                        row(8,8,8),
                        row(9,9,9),
                        row(10,10,10)))
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
        try(PreparedStatement ps = conn.prepareStatement("select null_count,null_fraction from sysvw.syscolumnstatistics where schemaname = ? and tablename = ?")){
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
        conn.rollback();
        conn.reset();
    }

    @Test
    public void testTableStatisticsAreCorrectForEmptyTable() throws Exception{
        TestConnection conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        try (CallableStatement callableStatement=conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?)")) {
            callableStatement.setString(1, SCHEMA);
            callableStatement.setString(2, TABLE_EMPTY);
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
        long conglomId=conn.getConglomNumbers(SCHEMA,TABLE_EMPTY)[0];
        try (PreparedStatement check=conn.prepareStatement("select * from sys.systablestats where conglomerateId = ?")) {
            check.setLong(1, conglomId);
            ResultSet resultSet = check.executeQuery();
            Assert.assertTrue("Unable to find statistics for table!", resultSet.next());
            Assert.assertEquals("Incorrect row count!", 0l, resultSet.getLong(6));
            Assert.assertEquals("Incorrect partition size!", 0l, resultSet.getLong(7));
            Assert.assertEquals("Incorrect row width!", 0l, resultSet.getInt(8));
        }

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

        ResultSet rs = methodWatcher3.executeQuery("select null_fraction from sysvw.SYSCOLUMNSTATISTICS where TABLENAME like '"+WITH_NULLS_NUMERIC+"' and SCHEMANAME like '" + spliceSchemaWatcher3.schemaName+ "' and columnname='MYFLOAT'");
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

        conn.rollback();
        conn.reset();
    }

    @Test
    public void testCollectNonMergedSampleStats() throws Exception{
        TestConnection conn4=methodWatcher4.getOrCreateConnection();

        // test regular stats
        conn4.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_NONMERGED_TABLE_STATISTICS('%s','t1', false)",
                spliceSchemaWatcher4));
        //check the statsType and sample fraction in sys.systablestats by querying the system view systablestatistics
        ResultSet resultSet = conn4.createStatement().
                executeQuery(format("select * from sysvw.systablestatistics where schemaName='%s' and tablename='T1'", spliceSchemaWatcher4));
        long rowCount = 0;
        while (resultSet.next()) {
            //stats type should be 0 (which represents regular non-merged stats)
            Assert.assertTrue("statsType is expected to be 0(REGULAR_NONMERGED_STATS)",
                    resultSet.getInt(10)== SYSTABLESTATISTICSRowFactory.REGULAR_NONMERGED_STATS);
            //stats fraction should be 0.0
            Assert.assertTrue("sampleFraction does not match",
                    Math.abs(resultSet.getDouble(11)) < 1e-9);
            rowCount += resultSet.getLong(4);
        }
        //check if the number of rows is correct
        Assert.assertTrue("rowcount does not match the actual, expect 40, actual is" + rowCount, rowCount == 40);
        resultSet.close();

        // test sample stats
        conn4.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_NONMERGED_TABLE_SAMPLE_STATISTICS('%s','t2', 50, false)",
                spliceSchemaWatcher4));
        //check the statsType and sample fraction in sys.systablestats by querying the system view systablestatistics
        resultSet = conn4.createStatement().
                executeQuery(format("select * from sysvw.systablestatistics where schemaName='%s' and tablename='T2'", spliceSchemaWatcher4));
        rowCount = 0;
        boolean firstRow = true;
        int statsType = 0;
        double sampleFraction = 0.0d;
        while (resultSet.next()) {
            if (firstRow) {
                statsType = resultSet.getInt(10);
                sampleFraction = resultSet.getDouble(11);
                // mem platform does not support sample stats, so statsType will be 0 (which represents regular stats)
                // for all other platforms, statsType should be 1 (which represents sample stats)
                if (statsType == SYSTABLESTATISTICSRowFactory.SAMPLE_NONMERGED_STATS)
                    Assert.assertTrue("sampleFraction should be 0.5",
                            Math.abs(sampleFraction - 0.5) < 1e-9);
                else if(statsType == SYSTABLESTATISTICSRowFactory.REGULAR_NONMERGED_STATS)
                    Assert.assertTrue("sampleFraction should be 0.0",
                            Math.abs(sampleFraction) < 1e-9);
                else
                    Assert.fail("statsType should be either 0(REGULAR_NONMERGED_STATS) or 1(SAMPLE_NONMERGED_STATS), but is " + statsType);

                firstRow = false;
            } else {
                //stats type and sample fraction should be consistent across all partitions
                Assert.assertEquals(statsType, resultSet.getInt(10));
                //stats fraction should be 0.5
                Assert.assertTrue("sampleFraction does not match",
                        Math.abs(resultSet.getDouble(11) - sampleFraction) < 1e-9);
            }
            rowCount += resultSet.getLong(4);
        }
        //check if the number of rows sampled is proportional to the sample ratio
        if (statsType == SYSTABLESTATISTICSRowFactory.SAMPLE_NONMERGED_STATS)
            Assert.assertTrue("sampled rowcount does not match the specified sample fraciton", Math.abs((double)rowCount/sampleFraction - 40960 )/40960 < 0.2);
        else // if (statsType == SYSTABLESTATISTICSRowFactory.REGULAR_NONMERGED_STATS)
            Assert.assertTrue("sampled rowcount does not match the specified sample fraciton", Math.abs((double)rowCount - 40960 )/40960 < 0.02);
        resultSet.close();

        // test estimations under sample stats
        // case 1: test row count
        String sqlText = "explain select * from t2";
        double outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be around 40960, actual is %s", outputRows), Math.abs(outputRows - 40960)/40960 < 0.2);

        // case 2: test selectivity (with matches)
        sqlText = "explain select * from t2 where b2=1";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be around 20480, actual is %s", outputRows), Math.abs(outputRows - 20480)/20480 < 0.2);

        // case 3: test selectivity (without matches)
        sqlText = "explain select * from t2 where b2=3";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be 1, actual is %s", outputRows), Math.abs(outputRows - 1) < 0.2);

        //case 4: test range selectivity
        sqlText = "explain select * from t2 where a2 > 20480";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be around 20480, actual is %s", outputRows), Math.abs(outputRows - 20480)/20480 < 0.2);

        //case 5:  test cardinality
        sqlText = "explain select * from --splice-properties joinOrder=fixed \n" +
                "t1, t2 --splice-properties joinStrategy=NESTEDLOOP \n" +
                "where c1=c2";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(4, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be around 20480, actual is %s", outputRows),Math.abs(outputRows - 20480)/20480 < 0.2);

    }

    @Test

    public void testCollectMergedSampleStats() throws Exception{
        TestConnection conn4=methodWatcher4.getOrCreateConnection();

        // test regular stats
        conn4.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','t1', false)",
                spliceSchemaWatcher4));
        //check the statsType and sample fraction in sys.systablestats by querying the system view systablestatistics
        ResultSet resultSet = conn4.createStatement().
                executeQuery(format("select * from sysvw.systablestatistics where schemaName='%s' and tablename='T1'", spliceSchemaWatcher4));
        long rowCount = 0;
        while (resultSet.next()) {
            //stats type should be 2 (which represents regular merged stats)
            Assert.assertTrue("statsType is expected to be 2(REGULAR_MERGED_STATS)",
                    resultSet.getInt(10)==SYSTABLESTATISTICSRowFactory.REGULAR_MERGED_STATS);
            //stats fraction should be 0.0
            Assert.assertTrue("sampleFraction does not match",
                    Math.abs(resultSet.getDouble(11)) < 1e-9);
            rowCount += resultSet.getLong(4);
        }
        //check if the number of rows is correct
        Assert.assertTrue("rowcount does not match the actual, expect 40, actual is" + rowCount, rowCount == 40);
        resultSet.close();

        // test sample stats
        conn4.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_TABLE_SAMPLE_STATISTICS('%s','t2', 50, false)",
                spliceSchemaWatcher4));
        //check the statsType and sample fraction in sys.systablestats by querying the system view systablestatistics
        resultSet = conn4.createStatement().
                executeQuery(format("select * from sysvw.systablestatistics where schemaName='%s' and tablename='T2'", spliceSchemaWatcher4));
        rowCount = 0;
        boolean firstRow = true;
        int statsType = SYSTABLESTATISTICSRowFactory.REGULAR_NONMERGED_STATS;
        double sampleFraction = 0.0d;
        while (resultSet.next()) {
            if (firstRow) {
                statsType = resultSet.getInt(10);
                sampleFraction = resultSet.getDouble(11);
                // mem platform does not support sample stats, so statsType will be 2 (which represents regular merged stats)
                // for all other platforms, statsType should be 3 (which represents sample merged stats)
                if (statsType == SYSTABLESTATISTICSRowFactory.SAMPLE_MERGED_STATS)
                    Assert.assertTrue("sampleFraction should be 0.5",
                            Math.abs(sampleFraction - 0.5) < 1e-9);
                else if (statsType == SYSTABLESTATISTICSRowFactory.REGULAR_MERGED_STATS)
                    Assert.assertTrue("sampleFraction should be 0.0",
                            Math.abs(sampleFraction) < 1e-9);
                else
                    Assert.fail("statsType should be either 2(REGULAR_MERGED_STATS) or 3(SAMPLE_MERGED_STATS), but is " + statsType);

                firstRow = false;
            } else {
                //stats type and sample fraction should be consistent across all partitions
                Assert.assertEquals(statsType, resultSet.getInt(10));
                //stats fraction should be 0.5
                Assert.assertTrue("sampleFraction does not match",
                        Math.abs(resultSet.getDouble(11) - sampleFraction) < 1e-9);
            }
            rowCount += resultSet.getLong(4);
        }
        //check if the number of rows sampled is proportional to the sample ratio
        if (statsType ==SYSTABLESTATISTICSRowFactory.SAMPLE_MERGED_STATS)
            Assert.assertTrue("sampled rowcount does not match the specified sample fraciton", Math.abs((double)rowCount/sampleFraction - 40960 )/40960 < 0.2);
        else // if (statsType == SYSTABLESTATISTICSRowFactory.REGULAR_MERGED_STATS
            Assert.assertTrue("sampled rowcount does not match the specified sample fraciton", Math.abs((double)rowCount - 40960 )/40960 < 0.02);
        resultSet.close();

        // test estimations under sample stats
        // case 1: test row count
        String sqlText = "explain select * from t2";
        double outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be around 40960, actual is %s", outputRows), Math.abs(outputRows - 40960)/40960 < 0.2);

        // case 2: test selectivity (with matches)
        sqlText = "explain select * from t2 where b2=1";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be around 20480, actual is %s", outputRows), Math.abs(outputRows - 20480)/20480 < 0.2);

        // case 3: test selectivity (without matches)
        sqlText = "explain select * from t2 where b2=3";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be 1, actual is %s", outputRows), Math.abs(outputRows - 1) < 0.2);

        //case 4: test range selectivity
        sqlText = "explain select * from t2 where a2 > 20480";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be around 20480, actual is %s", outputRows), Math.abs(outputRows - 20480)/20480 < 0.2);

        //case 5:  test cardinality
        sqlText = "explain select * from --splice-properties joinOrder=fixed \n" +
                "t1, t2 --splice-properties joinStrategy=NESTEDLOOP \n" +
                "where c1=c2";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(4, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be around 20480, actual is %s", outputRows),Math.abs(outputRows - 20480)/20480 < 0.2);

    }

    @Test
    public void testCollectSampleStatsViaAnalyze() throws Exception {
        TestConnection conn4 = methodWatcher4.getOrCreateConnection();

        // test sample stats
        conn4.createStatement().executeQuery(format(
                "analyze table %s.t2 estimate statistics sample 50 percent",
                spliceSchemaWatcher4));
        //check the statsType and sample fraction in sys.systablestats by querying the system view systablestatistics
        ResultSet resultSet = conn4.createStatement().
                executeQuery(format("select * from sysvw.systablestatistics where schemaName='%s' and tablename='T2'", spliceSchemaWatcher4));
        long rowCount = 0;
        boolean firstRow = true;
        int statsType = SYSTABLESTATISTICSRowFactory.REGULAR_NONMERGED_STATS;
        double sampleFraction = 0.0d;
        while (resultSet.next()) {
            if (firstRow) {
                statsType = resultSet.getInt(10);
                sampleFraction = resultSet.getDouble(11);
                // mem platform does not support sample stats, so statsType will be 2 (which represents regular merged stats)
                // for all other platforms, statsType should be 3 (which represents sample merged stats)
                if (statsType == SYSTABLESTATISTICSRowFactory.SAMPLE_MERGED_STATS)
                    Assert.assertTrue("sampleFraction should be 0.5",
                            Math.abs(sampleFraction - 0.5) < 1e-9);
                else if (statsType == SYSTABLESTATISTICSRowFactory.REGULAR_MERGED_STATS)
                    Assert.assertTrue("sampleFraction should be 0.0",
                            Math.abs(sampleFraction) < 1e-9);
                else
                    Assert.fail("statsType should be either 2(REGULAR_MERGED_STATS) or 3(SAMPLE_MERGED_STATS), but is " + statsType);

                firstRow = false;
            } else {
                //stats type and sample fraction should be consistent across all partitions
                Assert.assertEquals(statsType, resultSet.getInt(10));
                //stats fraction should be 0.5
                Assert.assertTrue("sampleFraction does not match",
                        Math.abs(resultSet.getDouble(11) - sampleFraction) < 1e-9);
            }
            rowCount += resultSet.getLong(4);
        }
        //check if the number of rows sampled is proportional to the sample ratio
        if (statsType == SYSTABLESTATISTICSRowFactory.SAMPLE_MERGED_STATS)
            Assert.assertTrue("sampled rowcount does not match the specified sample fraciton", Math.abs((double) rowCount / sampleFraction - 40960) / 40960 < 0.2);
        else // if (statsType == SYSTABLESTATISTICSRowFactory.REGULAR_MERGED_STATS
            Assert.assertTrue("sampled rowcount does not match the specified sample fraciton", Math.abs((double) rowCount - 40960) / 40960 < 0.02);
        resultSet.close();

        // test estimations under sample stats
        // case 1: test row count
        String sqlText = "explain select * from t2";
        double outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be around 40960, actual is %s", outputRows), Math.abs(outputRows - 40960) / 40960 < 0.2);

        // case 2: test selectivity (with matches)
        sqlText = "explain select * from t2 where b2=1";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be around 20480, actual is %s", outputRows), Math.abs(outputRows - 20480) / 20480 < 0.2);

        // case 3: test selectivity (without matches)
        sqlText = "explain select * from t2 where b2=3";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be 1, actual is %s", outputRows), Math.abs(outputRows - 1) < 0.2);

        //case 4: test range selectivity
        sqlText = "explain select * from t2 where a2 > 20480";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4));
        Assert.assertTrue(format("OutputRows is expected to be around 20480, actual is %s", outputRows), Math.abs(outputRows - 20480) / 20480 < 0.2);

        //case 5:  test cardinality
        sqlText = "explain select * from --splice-properties joinOrder=fixed \n" +
                "t1, t2 --splice-properties joinStrategy=NESTEDLOOP \n" +
                "where c1=c2";
        outputRows = SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(4, sqlText, methodWatcher4));

        Assert.assertTrue(format("OutputRows is expected to be around 20480, actual is %s", outputRows), Math.abs(outputRows - 20480) / 20480 < 0.2);
    }

    @Test
    public void testTableStatisticsAreCorrectForEmptyTable1() throws Exception{
        //TestConnection conn=methodWatcher.getOrCreateConnection();
        /* disable stats for c1, d1, e1 */
        methodWatcher4.executeUpdate(String.format("call SYSCS_UTIL.DISABLE_COLUMN_STATISTICS('%s','%s','c1')", SCHEMA4, TABLE_EMPTY1));
        methodWatcher4.executeUpdate(String.format("call SYSCS_UTIL.DISABLE_COLUMN_STATISTICS('%s','%s','d1')", SCHEMA4, TABLE_EMPTY1));
        methodWatcher4.executeUpdate(String.format("call SYSCS_UTIL.DISABLE_COLUMN_STATISTICS('%s','%s','e1')", SCHEMA4, TABLE_EMPTY1));

        /* only collects stats on a1, b1, f1 */
        methodWatcher4.executeQuery(String.format("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','%s',false)", SCHEMA4, TABLE_EMPTY1));

        /* With empty table, we should expect 3 entries in the syscolumnstats table corresponding to a1, b1, f1 respectively */
        ResultSet rs = methodWatcher4.executeQuery(String.format("select partition_id, count(*)\n" +
                "from sys.syscolumnstats cs, sys.sysschemas s, sys.systables t, sys.sysconglomerates c\n" +
                "where t.tablename='%s' and s.schemaname='%s' and t.schemaid=s.schemaid and t.tableid=c.tableid and c.conglomeratenumber = cs.conglom_id\n" +
                "group by cs.partition_id", TABLE_EMPTY1, SCHEMA4));
        Assert.assertTrue("Unable to find column statistics for table!", rs.next());
        Assert.assertEquals("Incorrect row count!", 3, rs.getLong(2));
    }

    @Test
    public void testCollectIndexStatsOnlyProperty() throws Exception{
        /* 1. t3 has an index ind_t3_1 on (a3, e3) */

        /* 2. set the database property splice.database.collectIndexStatsOnly to true */
        methodWatcher4.executeUpdate("call SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('derby.database.collectIndexStatsOnly', 'true')");

        /* 3. confirm the setting */
        String expected = "1  |\n" +
                "------\n" +
                "true |";
        ResultSet rs = methodWatcher4.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.collectIndexStatsOnly')");
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("Expect the property derby.database.collectIndexStatsOnly to be true, actual is " + resultString, expected, resultString);
        rs.close();

        /* 4. collect stats */
        methodWatcher4.executeQuery(String.format("analyze table %s.t3", SCHEMA4));

        /* 5. Verify stats collected, we should expect
           stats to be collected on just 4 columns, a3, b3, c3, e3, where (b3,c3) are primary keys,
           (a3, e3) are index columns
         */
        rs = methodWatcher4.executeQuery(String.format("select distinct columnname from sysvw.syscolumnstatistics where schemaname='%s' and tablename='T3' order by 1", SCHEMA4));
        expected = "COLUMNNAME |\n" +
                "------------\n" +
                "    A3     |\n" +
                "    B3     |\n" +
                "    C3     |\n" +
                "    E3     |";
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("List of columns where stats are collected does not match.", expected, resultString);
        rs.close();

        /* 6. change the property to default */
        methodWatcher4.executeUpdate("call SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('derby.database.collectIndexStatsOnly', 'false')");

        /* 7. confirm the setting */
        expected = "1   |\n" +
                "-------\n" +
                "false |";
        rs = methodWatcher4.executeQuery("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('derby.database.collectIndexStatsOnly')");
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("Expect the property derby.database.collectIndexStatsOnly to be false, actual is " + resultString, expected, resultString);
        rs.close();

        /* 8. collect stats */
        methodWatcher4.executeQuery(String.format("analyze table %s.t3", SCHEMA4));

        /* 9. Verify stats collected, we should expect stats to be collected on all 6 columns
         */
        rs = methodWatcher4.executeQuery(String.format("select distinct columnname from sysvw.syscolumnstatistics where schemaname='%s' and tablename='T3' order by 1", SCHEMA4));
        expected = "COLUMNNAME |\n" +
                "------------\n" +
                "    A3     |\n" +
                "    B3     |\n" +
                "    C3     |\n" +
                "    D3     |\n" +
                "    E3     |\n" +
                "    F3     |";
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("List of columns where stats are collected does not match.", expected, resultString);
        rs.close();
    }

    @Test
    public void testEanbleDisableAllColumnStatistics() throws Exception {
        /* 1 check the collectstats property of all columns */
        String expected1 = "COLUMNNAME |COLLECTSTATS |\n" +
                "--------------------------\n" +
                "    A3     |    true     |\n" +
                "    B3     |    true     |\n" +
                "    C3     |    true     |\n" +
                "    D3     |    true     |\n" +
                "    E3     |    true     |\n" +
                "    F3     |    true     |";
        ResultSet rs = methodWatcher4.executeQuery(format("select columnname, collectstats\n" +
                "from sys.systables as T, sys.syscolumns as C, sys.sysschemas as S\n" +
                "where S.schemaId = T.schemaId and T.tableId=C.referenceId and S.schemaName='%s' and t.tablename='T3'", SCHEMA4));
        String resultString = TestUtils.FormattedResult.ResultFactory.toString(rs);
        assertEquals("CollectStats properties of columns do not match expected result.", expected1, resultString);
        rs.close();

        /* 2 test disable all column stats */
        methodWatcher4.executeUpdate(format("call syscs_util.disable_all_column_statistics('%s', 'T3')", SCHEMA4));

        /* check the collectstats property of all columns */
        String expected2 = "COLUMNNAME |COLLECTSTATS |\n" +
                "--------------------------\n" +
                "    A3     |    true     |\n" +
                "    B3     |    true     |\n" +
                "    C3     |    true     |\n" +
                "    D3     |    false    |\n" +
                "    E3     |    true     |\n" +
                "    F3     |    false    |";
        rs = methodWatcher4.executeQuery(format("select columnname, collectstats\n" +
                "from sys.systables as T, sys.syscolumns as C, sys.sysschemas as S\n" +
                "where S.schemaId = T.schemaId and T.tableId=C.referenceId and S.schemaName='%s' and t.tablename='T3'", SCHEMA4));
        resultString = TestUtils.FormattedResult.ResultFactory.toString(rs);
        assertEquals("CollectStats properties of columns do not match expected result.", expected2, resultString);
        rs.close();

        /* test eanble all column stats */
        methodWatcher4.executeUpdate(format("call syscs_util.enable_all_column_statistics('%s', 'T3')", SCHEMA4));

        /* check collectstats property again */
        rs = methodWatcher4.executeQuery(format("select columnname, collectstats\n" +
                "from sys.systables as T, sys.syscolumns as C, sys.sysschemas as S\n" +
                "where S.schemaId = T.schemaId and T.tableId=C.referenceId and S.schemaName='%s' and t.tablename='T3'", SCHEMA4));
        resultString = TestUtils.FormattedResult.ResultFactory.toString(rs);
        assertEquals("CollectStats properties of columns do not match expected result.", expected1, resultString);
        rs.close();
    }

    @Test
    public void testAnalyzeOnCharColumnWithEmptyString() throws Exception{
        /* only collects stats on a1, b1, f1 */
        methodWatcher4.executeQuery(String.format("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS('%s','T4',false)", SCHEMA4));

        /* check column statistics on A4  */
        ResultSet rs = methodWatcher4.executeQuery(String.format("select cardinality, min_value, max_value from " +
                        "sysvw.syscolumnstatistics where schemaname='%s' and tablename='T4' and columnname='A4'", SCHEMA4));
        Assert.assertTrue("Unable to find column statistics for table!", rs.next());
        // theta sketch rejects empty string, so cardinality is 9 instead of 10
        Assert.assertEquals("Incorrect cardinality!", 9, rs.getLong(1));
        Assert.assertEquals("Incorrect min value", "", rs.getString(2));
        Assert.assertEquals("Incorrect max value", "I", rs.getString(3));

        rs.close();

        /* check column statistics on B4  */
        rs = methodWatcher4.executeQuery(String.format("select cardinality, min_value, max_value from " +
                "sysvw.syscolumnstatistics where schemaname='%s' and tablename='T4' and columnname='B4'", SCHEMA4));
        Assert.assertTrue("Unable to find column statistics for table!", rs.next());
        Assert.assertEquals("Incorrect cardinality!", 5, rs.getLong(1));
        Assert.assertEquals("Incorrect min value", "", rs.getString(2));
        Assert.assertEquals("Incorrect max value", "E", rs.getString(3));

        rs.close();

        /* check estimation using the stats */
        String sqlText = "explain select * from t4 where a4=''";
        long outputRows = (long)(SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4)));
        Assert.assertEquals("OutputRows estimation does not match", 1, outputRows);

        sqlText = "explain select * from t4 where a4<>''";
        outputRows = (long)(SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4)));
        Assert.assertEquals("OutputRows estimation does not match", 9, outputRows);

        sqlText = "explain select * from t4 where b4=''";
        outputRows = (long)(SpliceUnitTest.parseOutputRows(SpliceUnitTest.getExplainMessage(3, sqlText, methodWatcher4)));
        Assert.assertEquals("OutputRows estimation does not match", 5, outputRows);

        rs.close();
    }

    @Test
    public void testSetUseExtrapolation() throws Exception {
        /* 1 check the useExtrapolation column */
        String expected1 = "COLUMNNAME |USEEXTRAPOLATION |\n" +
                "------------------------------\n" +
                "    A5     |        0        |\n" +
                "    B5     |        0        |\n" +
                "    C5     |        0        |\n" +
                "    D5     |        0        |\n" +
                "    E5     |        0        |\n" +
                "    F5     |        0        |\n" +
                "    G5     |        0        |\n" +
                "    H5     |        0        |\n" +
                "    I5     |        0        |\n" +
                "    J5     |        0        |\n" +
                "    K5     |        0        |";
        ResultSet rs = methodWatcher4.executeQuery(format("select columnname, useExtrapolation\n" +
                "from sys.systables as T, sys.syscolumns as C, sys.sysschemas as S\n" +
                "where S.schemaId = T.schemaId and T.tableId=C.referenceId and S.schemaName='%s' and t.tablename='T5'", SCHEMA4));
        String resultString = TestUtils.FormattedResult.ResultFactory.toString(rs);
        assertEquals("UseExtrapolation properties of columns do not match expected result.", expected1, resultString);
        rs.close();

        /* 2 enable extraploation */
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 1)", SCHEMA4, "A5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 1)", SCHEMA4, "B5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 1)", SCHEMA4, "C5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 1)", SCHEMA4, "D5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 1)", SCHEMA4, "E5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 1)", SCHEMA4, "F5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 1)", SCHEMA4, "G5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 1)", SCHEMA4, "H5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 1)", SCHEMA4, "I5"));
        try {
            methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 1)", SCHEMA4, "J5"));
        }  catch (SQLException e) {
            assertEquals(format("Wrong error state! Expected: %s, actual:%s", ErrorState.LANG_STATS_EXTRAPOLATION_NOT_SUPPORTED.getSqlState(), e.getSQLState()),
                    ErrorState.LANG_STATS_EXTRAPOLATION_NOT_SUPPORTED.getSqlState(), e.getSQLState());
        }
        try {
            methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 1)", SCHEMA4, "K5"));
        } catch (SQLException e) {
            assertEquals(format("Wrong error state! Expected: %s, actual:%s", ErrorState.LANG_STATS_EXTRAPOLATION_NOT_SUPPORTED.getSqlState(), e.getSQLState()),
                    ErrorState.LANG_STATS_EXTRAPOLATION_NOT_SUPPORTED.getSqlState(), e.getSQLState());
        }

        /* check the useExtrapolation property of all columns */
        String expected2 = "COLUMNNAME |USEEXTRAPOLATION |\n" +
                "------------------------------\n" +
                "    A5     |        1        |\n" +
                "    B5     |        1        |\n" +
                "    C5     |        1        |\n" +
                "    D5     |        1        |\n" +
                "    E5     |        1        |\n" +
                "    F5     |        1        |\n" +
                "    G5     |        1        |\n" +
                "    H5     |        1        |\n" +
                "    I5     |        1        |\n" +
                "    J5     |        0        |\n" +
                "    K5     |        0        |";
        rs = methodWatcher4.executeQuery(format("select columnname, useExtrapolation\n" +
                "from sys.systables as T, sys.syscolumns as C, sys.sysschemas as S\n" +
                "where S.schemaId = T.schemaId and T.tableId=C.referenceId and S.schemaName='%s' and t.tablename='T5'", SCHEMA4));
        resultString = TestUtils.FormattedResult.ResultFactory.toString(rs);
        assertEquals("UseExtrapolation properties of columns do not match expected result.", expected2, resultString);
        rs.close();

        /* reset useExtrapolation */
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 0)", SCHEMA4, "A5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 0)", SCHEMA4, "B5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 0)", SCHEMA4, "C5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 0)", SCHEMA4, "D5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 0)", SCHEMA4, "E5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 0)", SCHEMA4, "F5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 0)", SCHEMA4, "G5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 0)", SCHEMA4, "H5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 0)", SCHEMA4, "I5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 0)", SCHEMA4, "J5"));
        methodWatcher4.executeUpdate(format("call syscs_util.set_stats_extrapolation_for_column('%s', 'T5', '%s', 0)", SCHEMA4, "K5"));



        /* check useExtrapolation property again */
        rs = methodWatcher4.executeQuery(format("select columnname, useExtrapolation\n" +
                "from sys.systables as T, sys.syscolumns as C, sys.sysschemas as S\n" +
                "where S.schemaId = T.schemaId and T.tableId=C.referenceId and S.schemaName='%s' and t.tablename='T5'", SCHEMA4));
        resultString = TestUtils.FormattedResult.ResultFactory.toString(rs);
        assertEquals("UseExtrapolation properties of columns do not match expected result.", expected1, resultString);
        rs.close();
    }

    @Category(HBaseTest.class)
    @Test
    public void testCollectStatsWithNoStaleRegionInfo() throws Exception {
        /* step 1 create and populate the table */
        methodWatcher4.execute("create table TAB_TO_SPLIT(a1 int, b1 int, c1 int, primary key (a1))");
        methodWatcher4.execute("insert into TAB_TO_SPLIT values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9), (10,10,10)");
        /* step 2: run a query against the table TAB_TO_SPLIT to populate the region info cache with the initial info.
           At this point, we should have only one region.
         */
        methodWatcher4.executeQuery("select * from TAB_TO_SPLIT --splice-properties useSpark=true");

        /* step 3: run analyze, the result should indicate only one partition */
        ResultSet rs = methodWatcher4.executeQuery("analyze table TAB_TO_SPLIT");
        if (rs.next()) {
            // get the number of regions/partitions
            long numOfRegions = rs.getLong(6);
            assertEquals("Region number does not match, expected: 1, actual: "+numOfRegions, 1, numOfRegions);
        } else {
            Assert.fail("Expected to have one row returned!");
        }
        rs.close();

        /* step 4: split the region into 2, making the info in the region info cache stale */
        String dir = SpliceUnitTest.getResourceDirectory();
        methodWatcher4.execute(format("call syscs_util.syscs_split_table_or_index_at_points('%s','TAB_TO_SPLIT',null,'\\x85')", SCHEMA4));

        /* wait for a second */
        Thread.sleep(1000);

        /* step 5: do analyze again, we should see get 2 regions returend from stats collection */
        rs = methodWatcher4.executeQuery("analyze table TAB_TO_SPLIT");
        if (rs.next()) {
            // get the number of regions/partitions
            long numOfRegions = rs.getLong(6);
            assertEquals("Region number does not match, expected: 2, actual: "+numOfRegions, 2, numOfRegions);
        } else {
            Assert.fail("Expected to have one row returned!");
        }
        rs.close();

        /* step 6: clean up */
        methodWatcher4.execute("drop table TAB_TO_SPLIT");
    }

    @Test
    public void testQueryUsingFakeStats() throws Exception {
        methodWatcher4.execute(format("call syscs_util.fake_table_statistics('%s', 'T6', 10000, 100, 4)", SCHEMA4));
        methodWatcher4.execute(format("call syscs_util.fake_column_statistics('%s', 'T6', '%s', 0, 1000)", SCHEMA4, "B6"));
        methodWatcher4.execute(format("call syscs_util.fake_column_statistics('%s', 'T6', '%s', 0, 10000)", SCHEMA4, "A6"));

        // check the result of sys stats views
        ResultSet rs = methodWatcher4.executeQuery(format("select schemaname, tablename, total_row_count, avg_row_count, total_size, num_partitions, stats_type from sysvw.systablestatistics where schemaname='%s' and tablename='%s'", SCHEMA4, "T6"));
        String expected = "SCHEMANAME     | TABLENAME | TOTAL_ROW_COUNT | AVG_ROW_COUNT |TOTAL_SIZE |NUM_PARTITIONS |STATS_TYPE |\n" +
                "----------------------------------------------------------------------------------------------------------\n" +
                "STATISTICSADMINIT4 |    T6     |      10000      |   2500.0000   |  1000000  |       4       |     4     |";
        String resultString = TestUtils.FormattedResult.ResultFactory.toString(rs);
        assertEquals("Fake table stats does not match expected result.", expected, resultString);
        rs.close();

        rs = methodWatcher4.executeQuery(format("select schemaname, tablename, columnname, cardinality, null_count, null_fraction, min_value, max_value from sysvw.syscolumnstatistics where schemaname='%s' and tablename='%s' and columnname='%s'", SCHEMA4, "T6", "B6"));
        expected = "SCHEMANAME     | TABLENAME |COLUMNNAME | CARDINALITY |NULL_COUNT | NULL_FRACTION | MIN_VALUE | MAX_VALUE |\n" +
                "--------------------------------------------------------------------------------------------------------------\n" +
                "STATISTICSADMINIT4 |    T6     |    B6     |     10      |     0     |      0.0      |   NULL    |   NULL    |";
        resultString = TestUtils.FormattedResult.ResultFactory.toString(rs);
        assertEquals("Fake column stats does not match expected result.", expected, resultString);
        rs.close();

        // check the estimations in explain for table stats
        rowContainsQuery(new int[]{3}, format("explain select * from %s.t6", SCHEMA4), methodWatcher,
                new String[]{"scannedRows=10000,outputRows=10000", "partitions=4"});

        // check the estimations in explain for point range selectivity
        rowContainsQuery(new int[]{3}, format("explain select * from %s.t6 where b6=5", SCHEMA4), methodWatcher,
                new String[]{"scannedRows=10000,outputRows=1000", "partitions=4,preds=[(B6[0:2] = 5)]"});

        rowContainsQuery(new int[]{3}, format("explain select * from %s.t6 where a6=5", SCHEMA4), methodWatcher,
                new String[]{"scannedRows=1,outputRows=1", "partitions=4,preds=[(A6[0:1] = 6)]"});
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void verifyStatsCounts(Connection conn,String schema,String table,int tableStatsCount,int colStatsCount) throws Exception{
        try (
            PreparedStatement check = (table == null) ?
            conn.prepareStatement("select count(*) from sysvw.systablestatistics where schemaname = ?"):
            conn.prepareStatement("select count(*) from sysvw.systablestatistics where schemaname = ? and tablename = ?")) {

            check.setString(1, schema);
            if (table != null) check.setString(2, table);
            ResultSet resultSet = check.executeQuery();
            Assert.assertTrue("Unable to count stats for schema", resultSet.next());
            int rowCount = resultSet.getInt(1);
            Assert.assertEquals("Incorrect row count", tableStatsCount, rowCount);
        }

        try (
            PreparedStatement check2=(table==null)?
            conn.prepareStatement("select count(*) from sysvw.syscolumnstatistics where schemaname = ?"):
            conn.prepareStatement("select count(*) from sysvw.syscolumnstatistics where schemaname = ? and tablename = ?")) {

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
