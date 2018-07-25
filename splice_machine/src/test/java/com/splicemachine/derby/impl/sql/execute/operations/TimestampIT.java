/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.io.File;
import java.sql.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test different operations on extended-range timestamps.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class TimestampIT extends SpliceUnitTest {
    private static final String SCHEMA = TimestampIT.class.getSimpleName().toUpperCase();
    private Boolean useSpark;
    private static boolean extendedTimestamps = true;
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    protected static SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private static File BADDIR;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(spliceSchemaWatcher)
                                            .around(methodWatcher);
   // @Rule
   // public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);
     //    methodWatcher.setAutoCommit(false);


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(4);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }

    public TimestampIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        methodWatcher.setAutoCommit(false);
        createSharedTables(spliceClassWatcher.getOrCreateConnection());
    }

    public static void createSharedTables(Connection conn) throws Exception {
        BADDIR = SpliceUnitTest.createBadLogDirectory(SCHEMA);
        assertNotNull(BADDIR);

        try (Statement s = conn.createStatement()) {
            s.execute("CALL SYSCS_UTIL.SYSCS_EMPTY_GLOBAL_STATEMENT_CACHE()");
            s.execute("CALL SYSCS_UTIL.INVALIDATE_GLOBAL_DICTIONARY_CACHE()");
            s.execute("call syscs_util.syscs_set_global_database_property('derby.database.convertOutOfRangeTimeStamps', 'true')");

            s.executeUpdate(String.format("create table %s ", SCHEMA + ".t1") + "(col1 timestamp, col2 int, primary key(col1,col2))");
            s.executeUpdate(String.format("create table %s ", SCHEMA + ".t11") + "(col1 timestamp, col2 int)");
            s.executeUpdate(String.format("create table %s ", SCHEMA + ".t2") + "(col1 timestamp, col2 int, primary key(col1, col2))");
            s.executeUpdate(String.format("create table %s ", SCHEMA + ".t3") + "(col1 timestamp, col2 int)");
            s.executeUpdate(String.format("create table %s ", SCHEMA + ".t3b") + "(col1 timestamp, col2 int)");
            s.executeUpdate(String.format("create index idx1 on %s ", SCHEMA + ".t3") + "(col1)");
            s.executeUpdate(String.format("create table %s ", SCHEMA + ".t4") + "(col1 timestamp, col2 timestamp)");

            ResultSet rs = s.executeQuery("CALL SYSCS_UTIL.SYSCS_GET_GLOBAL_DATABASE_PROPERTY('derby.database.createTablesWithVersion2Serializer')");
            if (rs.next()) {
                String aStr = rs.getString(2);
                if (!rs.wasNull()) {
                    if (aStr.equals("true"))
                        extendedTimestamps = false;
                }
            }

            s.execute(String.format("call SYSCS_UTIL.IMPORT_DATA(" +
            "'%s'," +  // schema name
            "'%s'," +  // table name
            "'col1, col2'," +  // insert column list
            "'%s'," +  // file path
            "','," +   // column delimiter
            "null," +  // character delimiter
            "'yyyy-MM-dd HH:mm:ss.S'," +  // timestamp format
            "null," +  // date format
            "null," +  // time format
            "0," +    // max bad records
            "'%s'," +  // bad record dir
            "'true'," +  // has one line records
            "null)",   // char set
            SCHEMA, "t1", SpliceUnitTest.getResourceDirectory() + "ts2.csv",
            BADDIR.getCanonicalPath()));

            s.execute(String.format("call SYSCS_UTIL.IMPORT_DATA(" +
            "'%s'," +  // schema name
            "'%s'," +  // table name
            "'col1, col2'," +  // insert column list
            "'%s'," +  // file path
            "','," +   // column delimiter
            "null," +  // character delimiter
            "'yyyy-MM-dd HH:mm:ss.S'," +  // timestamp format
            "null," +  // date format
            "null," +  // time format
            "0," +    // max bad records
            "'%s'," +  // bad record dir
            "'true'," +  // has one line records
            "null)",   // char set
            SCHEMA, "t2", SpliceUnitTest.getResourceDirectory() + "ts3.csv",
            BADDIR.getCanonicalPath()));

            s.execute(String.format("call SYSCS_UTIL.IMPORT_DATA(" +
            "'%s'," +  // schema name
            "'%s'," +  // table name
            "'col1, col2'," +  // insert column list
            "'%s'," +  // file path
            "','," +   // column delimiter
            "null," +  // character delimiter
            "'yyyy-MM-dd HH:mm:ss.SSSSSS'," +  // timestamp format
            "null," +  // date format
            "null," +  // time format
            "0," +    // max bad records
            "'%s'," +  // bad record dir
            "'true'," +  // has one line records
            "null)",   // char set
            SCHEMA, "t3", SpliceUnitTest.getResourceDirectory() + "ts4.csv",
            BADDIR.getCanonicalPath()));

            s.execute(String.format("call SYSCS_UTIL.IMPORT_DATA(" +
            "'%s'," +  // schema name
            "'%s'," +  // table name
            "'col1, col2'," +  // insert column list
            "'%s'," +  // file path
            "','," +   // column delimiter
            "null," +  // character delimiter
            "'yyyy-MM-dd HH:mm:ss.SSSSSS'," +  // timestamp format
            "null," +  // date format
            "null," +  // time format
            "0," +    // max bad records
            "'%s'," +  // bad record dir
            "'true'," +  // has one line records
            "null)",   // char set
            SCHEMA, "t3b", SpliceUnitTest.getResourceDirectory() + "ts4b.csv",
            BADDIR.getCanonicalPath()));

            try {
                s.execute(String.format("call SYSCS_UTIL.IMPORT_DATA(" +
                "'%s'," +  // schema name
                "'%s'," +  // table name
                "'col1, col2'," +  // insert column list
                "'%s'," +  // file path
                "','," +   // column delimiter
                "null," +  // character delimiter
                "'yyyy-MM-dd HH:mm:ss.SSSSSS'," +  // timestamp format
                "null," +  // date format
                "null," +  // time format
                "1," +    // max bad records
                "'%s'," +  // bad record dir
                "'true'," +  // has one line records
                "null)",   // char set
                SCHEMA, "t3", SpliceUnitTest.getResourceDirectory() + "ts5.csv",
                BADDIR.getCanonicalPath()));

                // Assert that convertOutOfRangeTimestamps does not convert timestamps on version 3.0 tables.
                if (extendedTimestamps)
                     assertTrue("Import should have errored out.", false);
            } catch (Exception e) {

            }
            s.execute(String.format("call SYSCS_UTIL.IMPORT_DATA(" +
            "'%s'," +  // schema name
            "'%s'," +  // table name
            "'col1, col2'," +  // insert column list
            "'%s'," +  // file path
            "','," +   // column delimiter
            "null," +  // character delimiter
            "'yyyy-MM-dd HH:mm:ss.SSSSSS'," +  // timestamp format
            "null," +  // date format
            "null," +  // time format
            "0," +    // max bad records
            "'%s'," +  // bad record dir
            "'true'," +  // has one line records
            "null)",   // char set
            SCHEMA, "t4", SpliceUnitTest.getResourceDirectory() + "ts6.csv",
            BADDIR.getCanonicalPath()));


            s.executeUpdate(String.format("insert into %s select * from %s", SCHEMA + ".t11", SCHEMA + ".t1"));

            rs.close();
        }

    }

    @Test
    public void testMultiProbeTableScanWithProbeVariables() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(format("select col2 from t1 --SPLICE-PROPERTIES useSpark = %s  \n" +
                                          "where col1 in (?,?,?,?)", useSpark));
        ps.setTimestamp(1, new Timestamp(1 - 1900 /*year*/, 0 /*month-1*/, 1 /*day*/, 0 /*hour*/, 0/*minute*/, 0 /*second*/, 0 /*nano*/));
        ps.setTimestamp(2, new Timestamp(2 - 1900/*year*/, 0 /*month-1*/, 1 /*day*/, 0 /*hour*/, 0/*minute*/, 0 /*second*/, 0 /*nano*/));
        ps.setTimestamp(3, new Timestamp(3 - 1900/*year*/, 0 /*month-1*/, 1 /*day*/, 0 /*hour*/, 0/*minute*/, 0 /*second*/, 0 /*nano*/));
        ps.setTimestamp(4, new Timestamp(4 - 1900/*year*/, 0 /*month-1*/, 1 /*day*/, 0 /*hour*/, 0/*minute*/, 0 /*second*/, 0 /*nano*/));

        try {
            ResultSet rs = ps.executeQuery();
            int i = 0;
            while (rs.next()) {
                i++;
            }
            Assert.assertEquals("Incorrect count returned!", 4, i);
        }
        catch (SQLException e) {
            if (extendedTimestamps)
                assertTrue("Import shouldn't have errored out.", false);
        }
    }


    @Test
    public void testIntersect() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select count(*), max(col1), min(col1) " +
        " from  (select col1 from t1 --SPLICE-PROPERTIES useSpark = %s  \n" +
        "intersect select col1 from t2) argh", useSpark));

        assertTrue("intersect incorrect", rs.next());
        if (extendedTimestamps) {
            Assert.assertEquals("Wrong Count", 3, rs.getInt(1));
            Assert.assertEquals("Wrong Max", new Timestamp(4 - 1900 /*year*/, 0 /*month-1*/, 1 /*day*/, 0 /*hour*/, 0/*minute*/, 0 /*second*/, 0 /*nano*/), rs.getTimestamp(2));
            Assert.assertEquals("Wrong Min", new Timestamp(1 - 1900 /*year*/, 0 /*month-1*/, 1 /*day*/, 0 /*hour*/, 0/*minute*/, 0 /*second*/, 0 /*nano*/), rs.getTimestamp(3));
        } else {
            Assert.assertEquals("Wrong Count", 1, rs.getInt(1));
            Assert.assertEquals("Wrong Max", new Timestamp(1677 - 1900 /*year*/, 8 /*month-1*/, 20 /*day*/, 16 /*hour*/, 12/*minute*/, 43 /*second*/, 147000000 /*nano*/), rs.getTimestamp(2));
            Assert.assertEquals("Wrong Min", new Timestamp(1677 - 1900 /*year*/, 8 /*month-1*/, 20 /*day*/, 16 /*hour*/, 12/*minute*/, 43 /*second*/, 147000000 /*nano*/), rs.getTimestamp(3));

        }

    }
    @Test
    public void testUnion() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select count(*), max(col1), min(col1) " +
        " from  (select col1 from t1 --SPLICE-PROPERTIES useSpark = %s  \n" +
        "union select col1 from t2) argh", useSpark));

        assertTrue("union incorrect", rs.next());
        if (extendedTimestamps) {
            Assert.assertEquals("Wrong Count", 5, rs.getInt(1));
            Assert.assertEquals("Wrong Max", new Timestamp(5 - 1900 /*year*/, 0 /*month-1*/, 1 /*day*/, 0 /*hour*/, 0/*minute*/, 0 /*second*/, 0 /*nano*/), rs.getTimestamp(2));
            Assert.assertEquals("Wrong Min", new Timestamp(1 - 1900 /*year*/, 0 /*month-1*/, 1 /*day*/, 0 /*hour*/, 0/*minute*/, 0 /*second*/, 0 /*nano*/), rs.getTimestamp(3));
        }
        else {
            Assert.assertEquals("Wrong Count", 1, rs.getInt(1));
            Assert.assertEquals("Wrong Max", new Timestamp(1677 - 1900 /*year*/, 8 /*month-1*/, 20 /*day*/, 16 /*hour*/, 12/*minute*/, 43 /*second*/, 147000000 /*nano*/), rs.getTimestamp(2));
            Assert.assertEquals("Wrong Min", new Timestamp(1677 - 1900 /*year*/, 8 /*month-1*/, 20 /*day*/, 16 /*hour*/, 12/*minute*/, 43 /*second*/, 147000000 /*nano*/), rs.getTimestamp(3));
        }
    }


    @Test
    public void testExcept() throws Exception {

        ResultSet rs;

        rs = methodWatcher.executeQuery(
        format("select count(*), max(col1), min(col1) from (" +
        "select col1 from t1  --SPLICE-PROPERTIES useSpark = %s  \n except select col1 from t2) argh", useSpark));
        assertTrue("minus incorrect", rs.next());

        if (!extendedTimestamps) {
            Assert.assertEquals("Wrong Count", 0, rs.getInt(1));
            Assert.assertNull("Wrong Max,", rs.getTimestamp(2));
            Assert.assertNull("Wrong Min,", rs.getTimestamp(3));
        }
        else {
            Assert.assertEquals("Wrong Count", 2, rs.getInt(1));
            Assert.assertEquals("Wrong Max", new Timestamp(5 - 1900 /*year*/, 0 /*month-1*/, 1 /*day*/, 0 /*hour*/, 0/*minute*/, 0 /*second*/, 0 /*nano*/), rs.getTimestamp(2));
            Assert.assertEquals("Wrong Min", new Timestamp(3 - 1900 /*year*/, 0 /*month-1*/, 1 /*day*/, 0 /*hour*/, 0/*minute*/, 0 /*second*/, 0 /*nano*/), rs.getTimestamp(3));
        }
    }


    @Test
    public void testExceptWithOrderBy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
        format("select col1 from t1 --SPLICE-PROPERTIES useSpark = %s\n except select col1 from t2 order by 1", useSpark));
        if (extendedTimestamps) {
            Assert.assertEquals(
            "COL1          |\n" +
            "-----------------------\n" +
            "0003-01-01 00:00:00.0 |\n" +
            "0005-01-01 00:00:00.0 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
        else {
            Assert.assertEquals(
            "", TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }


    /* positive test, top N is applied on the union all result */
    @Test
    public void testTopN1() throws Exception {
        String sqlText = "select top 2 * from t1 union all select * from t2 order by 1,2";
        String expected = extendedTimestamps ?
        "COL1          |COL2 |\n" +
        "-----------------------------\n" +
        "0001-01-01 00:00:00.0 |  1  |\n" +
        "0001-01-01 00:00:00.0 |  1  |" :
        "COL1           |COL2 |\n" +
        "-------------------------------\n" +
        "1677-09-20 16:12:43.147 |  1  |\n" +
        "1677-09-20 16:12:43.147 |  1  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    /* positive test, top N is applied on the union all result */
    @Test
    public void testTopN2() throws Exception {
        String sqlText = format("select count(*) from " +
        "(select top 2 * from t1 --SPLICE-PROPERTIES useSpark = %s  \n" +
        "union all select * from t2)dt", useSpark);
        String expected =
        "1 |\n" +
        "----\n" +
        " 2 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testGroupBy6Digits() throws Exception {
        String sqlText = "select count(*), col1 from t3 --SPLICE-PROPERTIES useSpark = %s  \n" +
                         "group by col1";
        String expected;
        expected = extendedTimestamps ?
        "1 |           COL1            |\n" +
        "--------------------------------\n" +
        " 1 |0001-01-01 00:00:00.123456 |\n" +
        " 1 |0001-01-01 00:00:00.123458 |\n" +
        " 1 |0001-01-01 00:00:00.123457 |" :
        "1 |         COL1           |\n" +
        "-----------------------------\n" +
        " 5 |1677-09-20 16:12:43.147 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        sqlText = "select count(*), col1 from t3 --SPLICE-PROPERTIES useSpark = %s,index=idx1  \n" +
        "group by col1";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "select count(*), col1 from t3b --SPLICE-PROPERTIES useSpark = %s  \n" +
        "group by col1";

        expected = extendedTimestamps ?
        "1 |           COL1            |\n" +
        "--------------------------------\n" +
        " 1 |9999-12-31 23:59:59.999998 |\n" +
        " 1 |9999-12-31 23:59:59.999999 |\n" +
        " 1 |9999-12-31 23:59:59.999997 |" :
        "1 |         COL1           |\n" +
        "-----------------------------\n" +
        " 3 |2262-04-11 16:47:16.853 |";

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));

        rs.close();
    }

    @Test
    public void testTimestampAdd() throws Exception {
        String sqlText = format("select TIMESTAMPADD(SQL_TSI_SECOND, -1, col1) from t3 --SPLICE-PROPERTIES useSpark = %s", useSpark);
        String expected;

        TestConnection connection = methodWatcher.getOrCreateConnection();
        // Resulting timestamp should be out of range.
        if (extendedTimestamps)
            assertFailed(connection, sqlText, "22003");

        sqlText = format("select TIMESTAMPADD(SQL_TSI_SECOND, 1, col1) from t3 --SPLICE-PROPERTIES useSpark = %s", useSpark);

        // These results are expected to change once SPLICE-2200 is fixed.
        expected = extendedTimestamps ?
        "1           |\n" +
        "-----------------------\n" +
        "0001-01-01 00:00:01.0 |\n" +
        "0001-01-01 00:00:01.0 |\n" +
        "0001-01-01 00:00:01.0 |" :
        "1           |\n" +
        "-----------------------\n" +
        "1677-09-20 16:12:44.0 |\n" +
        "1677-09-20 16:12:44.0 |\n" +
        "1677-09-20 16:12:44.0 |\n" +
        "1677-09-20 16:12:44.0 |\n" +
        "1677-09-20 16:12:44.0 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select TIMESTAMPADD(SQL_TSI_SECOND, 1, col1) from t3b --SPLICE-PROPERTIES useSpark = %s", useSpark);

        // Resulting timestamp should be out of range.
        if (extendedTimestamps)
            assertFailed(connection, sqlText, "22003");

        sqlText = format("select TIMESTAMPADD(SQL_TSI_SECOND, -1, col1) from t3b --SPLICE-PROPERTIES useSpark = %s", useSpark);

        // These results are expected to change once SPLICE-2200 is fixed.
        expected = extendedTimestamps ?
        "1           |\n" +
        "-----------------------\n" +
        "9999-12-31 23:59:58.0 |\n" +
        "9999-12-31 23:59:58.0 |\n" +
        "9999-12-31 23:59:58.0 |" :
        "1           |\n" +
        "-----------------------\n" +
        "2262-04-11 16:47:15.0 |\n" +
        "2262-04-11 16:47:15.0 |\n" +
        "2262-04-11 16:47:15.0 |";

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

    }

    @Test
    public void testTimestampDiff() throws Exception {
        String sqlText = format("select TIMESTAMPDIFF(SQL_TSI_FRAC_SECOND, col1, col2) from t4 --SPLICE-PROPERTIES useSpark = %s", useSpark);
        String expected;

        // These results are expected to change once SPLICE-2200 is fixed.
        expected = extendedTimestamps ?
        "1          |\n" +
        "---------------------\n" +
        "       1000         |\n" +
        "1911885146937623528 |" :
        "1    |\n" +
        "----------\n" +
        "    0    |\n" +
        "-3551616 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

    }

    @Test
    public void testJoins() throws Exception {
        String sqlText;
        String expected;

        // These results are expected to change once SPLICE-2200 is fixed.
        expected = extendedTimestamps ?
        "COL1          |\n" +
        "-----------------------\n" +
        "0001-01-01 00:00:00.0 |\n" +
        "0002-01-01 00:00:00.0 |\n" +
        "0004-01-01 00:00:00.0 |" :
        "COL1           |\n" +
        "-------------------------\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |\n" +
        "1677-09-20 16:12:43.147 |";

        ResultSet rs;
        List<String> jsList = Arrays.asList("NESTEDLOOP", "MERGE", "SORTMERGE", "BROADCAST");
        for (String js : jsList) {
            sqlText = format("select t1.col1 from t1,t2 --SPLICE-PROPERTIES useSpark = %s,joinStrategy=%s  \n" +
            "where t1.col1=t2.col1", useSpark, js);
            rs = methodWatcher.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
        }
    }

    @Test
    public void updateTest() throws Exception {
        int updated = methodWatcher.executeUpdate(format("update t1 set col1 = {ts '1999-01-01 00:00:00'}"));
        assertEquals("Incorrect number of records updated", 5, updated);

        ResultSet rs = methodWatcher.executeQuery("select col1,col2 from t1");
        assertEquals("COL1          |COL2 |\n" +
        "-----------------------------\n" +
        "1999-01-01 00:00:00.0 |  1  |\n" +
        "1999-01-01 00:00:00.0 |  2  |\n" +
        "1999-01-01 00:00:00.0 |  3  |\n" +
        "1999-01-01 00:00:00.0 |  4  |\n" +
        "1999-01-01 00:00:00.0 |  5  |",
        TestUtils.FormattedResult.ResultFactory.toString(rs));

        // Test batch once update...
        /* Enable this test once SPLICE-2214 is fixed...
        updated = methodWatcher.executeUpdate(format("update t11 set col1 = (select col1 from t1 where t11.col2 = t1.col2)"));

        rs = methodWatcher.executeQuery("select col1,col2 from t11");
        assertEquals("COL1          |COL2 |\n" +
        "-----------------------------\n" +
        "1999-01-01 00:00:00.0 |  1  |\n" +
        "1999-01-01 00:00:00.0 |  2  |\n" +
        "1999-01-01 00:00:00.0 |  3  |\n" +
        "1999-01-01 00:00:00.0 |  4  |\n" +
        "1999-01-01 00:00:00.0 |  5  |",
        TestUtils.FormattedResult.ResultFactory.toString(rs));  */
        methodWatcher.rollback();

        String match = extendedTimestamps ?
         "COL1          |COL2 |\n" +
         "-----------------------------\n" +
         "0001-01-01 00:00:00.0 |  1  |\n" +
         "0002-01-01 00:00:00.0 |  2  |\n" +
         "0003-01-01 00:00:00.0 |  3  |\n" +
         "0004-01-01 00:00:00.0 |  4  |\n" +
         "0005-01-01 00:00:00.0 |  5  |" :
        "COL1           |COL2 |\n" +
        "-------------------------------\n" +
        "1677-09-20 16:12:43.147 |  1  |\n" +
        "1677-09-20 16:12:43.147 |  2  |\n" +
        "1677-09-20 16:12:43.147 |  3  |\n" +
        "1677-09-20 16:12:43.147 |  4  |\n" +
        "1677-09-20 16:12:43.147 |  5  |";

        rs = methodWatcher.executeQuery("select col1,col2 from t1");
        assertEquals(match, TestUtils.FormattedResult.ResultFactory.toString(rs));

        rs = methodWatcher.executeQuery("select col1,col2 from t11");
        assertEquals(match, TestUtils.FormattedResult.ResultFactory.toString(rs));

        rs.close();
    }

    @AfterClass
    public static void resetConvertOutOfRangeTimeStamps() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();

        try (Statement s = conn.createStatement()) {
            s.execute("CALL SYSCS_UTIL.SYSCS_EMPTY_GLOBAL_STATEMENT_CACHE()");
            s.execute("CALL SYSCS_UTIL.INVALIDATE_GLOBAL_DICTIONARY_CACHE()");
            s.execute("call syscs_util.syscs_set_global_database_property('derby.database.convertOutOfRangeTimeStamps', 'false')");
        }
    }

}