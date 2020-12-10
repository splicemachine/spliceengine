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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.pipeline.ErrorState;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;

/**
 * @author Scott Fines
 *         Created on: 2/22/13
 */
public class FunctionIT extends SpliceUnitTest {
    protected static final String USER1 = "XIAYI";
    protected static final String PASSWORD1 = "xiayi";

    private static final Logger LOG = Logger.getLogger(FunctionIT.class);
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(FunctionIT.class.getSimpleName());
    private static SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(USER1, PASSWORD1);
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("A",FunctionIT.class.getSimpleName(),"(data double)");
    protected static SpliceFunctionWatcher spliceFunctionWatcher = new SpliceFunctionWatcher("SIN",FunctionIT.class.getSimpleName(),"( data double) returns double external name 'java.lang.Math.sin' language java parameter style java");
    protected static double [] roundVals = {1.2, 2.53, 3.225, 4.1352, 5.23412, 53.2315093704, 205.130295341296824,
            13.21958329568391029385, 12.132435242330192856728391029584, 1.9082847283940982746172849098273647589099};
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher("B",FunctionIT.class.getSimpleName(),"(col decimal(14,4))");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("TMM",FunctionIT.class.getSimpleName(),"(i int, db double, dc decimal(3,1), c char(4), vc varchar(4), ts timestamp, bl blob(1K), cl clob(1K))");  // XML column not implemented
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher("COA",FunctionIT.class.getSimpleName(),"(d date, i int)");
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher("TEST_FN_DATETIME_CHAR",FunctionIT.class.getSimpleName(),"(d date, t time, ts timestamp)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
        .around(spliceSchemaWatcher)
        .around(spliceUserWatcher1)
        .around(spliceTableWatcher)
        .around(spliceTableWatcher1)
        .around(spliceTableWatcher2)
        .around(spliceTableWatcher3)
        .around(spliceTableWatcher4)
        .around(spliceFunctionWatcher)
        .around(new SpliceDataWatcher(){
            @Override
            protected void starting(Description description) {
                try {
                    PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into "+ FunctionIT.class.getSimpleName() + ".A (data) values (?)");
                    ps.setDouble(1,1.23d);
                    ps.executeUpdate();
                    ps.close();
                    ps = spliceClassWatcher.prepareStatement("insert into "+ FunctionIT.class.getSimpleName() + ".B (col) values (?)");
                    ps.setInt(1,2);
                    ps.executeUpdate();
                    ps.setNull(1, Types.DECIMAL);
                    ps.executeUpdate();
                    ps.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                finally {
                    spliceClassWatcher.closeAll();
                }
            }

        });

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @BeforeClass
    public static void addData() throws Exception {
        String schemaName = FunctionIT.class.getSimpleName();
        String insertPrefix = "insert into "+ schemaName + ".TMM values ";
        spliceClassWatcher.executeUpdate("delete from " + schemaName + ".TMM");
        spliceClassWatcher.executeUpdate(insertPrefix + "(1,1.0,1.2,'aa','aa','1960-01-01 23:03:20',null,null)");
        spliceClassWatcher.executeUpdate(insertPrefix + "(2,2.0,2.2,'bb','bb','1960-01-02 23:03:20',null,null)");
        spliceClassWatcher.executeUpdate(insertPrefix + "(3,3.0,3.2,'cc','cc','1960-01-04 23:03:20',null,null)");
        spliceClassWatcher.executeUpdate("delete from " + schemaName + ".TEST_FN_DATETIME_CHAR");
        spliceClassWatcher.executeUpdate("insert into " + schemaName + ".TEST_FN_DATETIME_CHAR values " +
                "('1960-03-01', '23:03:20', '1960-01-01 23:03:20'), " +
                "('1960-03-01', '00:00:00', '1960-01-01 23:03:20'), " +
                "('1960-03-01', '00:01:00', '1960-01-01 23:03:20'), " +
                "('1960-03-01', '12:00:00', '1960-01-01 23:03:20'), " +
                "('1960-03-01', '12:01:00', '1960-01-01 23:03:20'), " +
                "('1960-03-01', '24:00:00', '1960-01-01 23:03:20')");
        spliceClassWatcher.commit();
    }

    @Test
    public void testSinFunction() throws Exception{
        ResultSet funcRs = methodWatcher.executeQuery("select SIN(data) from" + this.getPaddedTableReference("A"));
        int rows = 0;
        while(funcRs.next()){
            double sin = funcRs.getDouble(1);
            double correctSin = Math.sin(1.23d);
            Assert.assertEquals("incorrect sin!",correctSin,sin,1/100000d);
            LOG.info(funcRs.getDouble(1));
            rows++;
        }
        Assert.assertTrue("Incorrect rows returned!",rows>0);
    }

    /**
     * Tests the ROUND function which rounds the input value to the nearest whole LONG
     * <p>
     * If the input value is NULL, the result of this function is NULL. If the
     * input value is equal to a mathematical integer, the result of this
     * function is the same as the input number. If the input value is zero (0),
     * the result of this function is zero.
     * <p>
     * The returned value is the closest (closest to positive infinity) long
     * value to the input value. The
     * returned value is equal to a mathematical long. The data type of the
     *
     * @throws Exception
     */
    @Test
    public void testRound() throws Exception {
        ResultSet rs;
        //testing round to Long
         for(double val : roundVals){
            rs = methodWatcher.executeQuery("values ROUND("+val+")");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(Math.round(val),rs.getLong(1));
        }
    }

    @Test
    public void testRoundEdgeCase() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("values ROUND(null)");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(null,rs.getObject(1));

        rs = methodWatcher.executeQuery("values ROUND(0)");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(0, rs.getDouble(1), 0.0);
    }

    /**
     * This tests the ROUND function when the user inputs 2 parameters, where the second is the number of decimal places to round to
     * @returns double rounded to the given number of decimal places
     * @throws Exception
     */
    @Test
    public void testRoundVaryScale() throws Exception{
        ResultSet rs;
        double longNumber = 1347593487534897908346789398700763453456786.9082847283940982746172849098273647589099;
        //testing round to Long
        for(int i = -40; i < 40; i++){
            rs = methodWatcher.executeQuery("values ROUND("+longNumber+","+i+")");
            Assert.assertTrue(rs.next());
            double mult = i < 18 ? i : 18;
            mult = Math.pow(10,mult);
            double x = Math.round(longNumber*mult)/(mult*1.0);
            Assert.assertEquals(x,rs.getDouble(1),0.0);
        }
    }

        /**
          * If more than one of the arguments passed to COALESCE are untyped
          * parameter markers, compilation used to fail with a NullPointerException.
          * Fixed in DERBY-6273.
          */
        @Test
    public void testMultipleUntypedParametersAndNVL() throws Exception {
        // All parameters cannot be untyped. This should still fail.
        try {
            methodWatcher.prepareStatement("values coalesce(?,?,?)");
        } catch (SQLException se) {
            Assert.assertEquals("Invalid sql state!", ErrorState.LANG_DB2_COALESCE_FUNCTION_ALL_PARAMS.getSqlState(),se.getSQLState());
        }
        // But as long as we know the type of one parameter, it should be
        // possible to have multiple parameters whose types are determined
        // from the context. These queries used to raise NullPointerException
        // before DERBY-6273.
        vetThreeArgCoalesce("values coalesce(cast(? as char(1)), ?, ?)");
        vetThreeArgCoalesce("values coalesce(?, cast(? as char(1)), ?)");
        vetThreeArgCoalesce("values coalesce(?, ?, cast(? as char(1)))");
        vetThreeArgCoalesce("values nvl(cast(? as char(1)), ?, ?)");
        vetThreeArgCoalesce("values nvl(?, cast(? as char(1)), ?)");
        vetThreeArgCoalesce("values nvl(?, ?, cast(? as char(1)))");

    }

    // DB-10522
    @Test
    public void testCoalesceImplicitConversion() throws SQLException {
        try (ResultSet rs = methodWatcher.executeQuery(format("select coalesce(max(d),'0001-01-01') from %s", spliceTableWatcher3))) {
            rs.next();
            Assert.assertEquals("0001-01-01", rs.getString(1));
        }
        try (ResultSet rs = methodWatcher.executeQuery(format("select coalesce(max(d),'0001-01-01') from %s --splice-properties useSpark=true\n", spliceTableWatcher3))) {
            rs.next();
            Assert.assertEquals("0001-01-01", rs.getString(1));
        }

    }

    @Test
    public void testCallToSystemFunctionFromUserWithoutDefaultSchema() throws Exception {
        TestConnection user1Conn = spliceClassWatcher.connectionBuilder().user(USER1).password(PASSWORD1).build();

        PreparedStatement ps = user1Conn.prepareStatement("VALUES rand(10)");
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        rs.close();


        ps = user1Conn.prepareStatement("VALUES random()");
        rs = ps.executeQuery();
        count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
        rs.close();

    }

    @Test
    public void testNvlWithDecimalZero() throws Exception {
        String sqlText = format("select sum(val_no_null)\n" +
                "from (select nvl(col, 0) as val_no_null from %1$s.B) dt, %1$s.A --splice-properties useSpark=true", FunctionIT.class.getSimpleName());

        String expected = "1   |\n" +
                "--------\n" +
                "2.0000 |";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    private void vetThreeArgCoalesce(String sql) throws Exception {
    // First three values in each row are arguments to COALESCE. The
            // last value is the expected return value.
                    String[][] data = {
                {"a",  "b",  "c",  "a"},
                {null, "b",  "c",  "b"},
                {"a",  null, "c",  "a"},
                {"a",  "b",  null, "a"},
                {null, null, "c",  "c"},
                {"a",  null, null, "a"},
                {null, "b",  null, "b"},
                {null, null, null, null},
            };
        PreparedStatement ps = methodWatcher.prepareStatement(sql);
        for (int i = 0; i < data.length; i++) {
            ps.setString(1, data[i][0]);
            ps.setString(2, data[i][1]);
            ps.setString(3, data[i][2]);
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals("Values do not match",rs.getString(1),data[i][3]);
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testCurrentServer() throws Exception {
        String[] sqlTexts = {
                "values current server", "values current_server",
                "select current server", "select current_server",
                "values current database", "values current_database",
                "select current database", "select current_database",
                "select (select current_server)"};
        String expected = "1    |\n" +
                "----------\n" +
                "SPLICEDB |";

        for (String sql: sqlTexts) {
            try (ResultSet rs = methodWatcher.executeQuery(sql)) {
                assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
        }
    }

    @Test
    public void testScalarMinMaxNotAnAggregate() {
        try {
            methodWatcher.executeQuery("select min(i, 2) from " + spliceTableWatcher2 + " group by db");
            Assert.fail("expect failure since scalar min function is not an aggregate");
        } catch (SQLException e) {
            assertEquals("42Y36", e.getSQLState());
        }

        try {
            methodWatcher.executeQuery("select max(i, 2) from " + spliceTableWatcher2 + " group by db");
            Assert.fail("expect failure since scalar min function is not an aggregate");
        } catch (SQLException e) {
            assertEquals("42Y36", e.getSQLState());
        }
    }

    @Test
    public void testScalarMinMaxNull() throws Exception {
        String expected = "1  |\n" +
                "------\n" +
                "NULL |\n" +
                "NULL |\n" +
                "NULL |";
        try (ResultSet rs = methodWatcher.executeQuery("select min(cast(NULL as integer), 3) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
        try (ResultSet rs = methodWatcher.executeQuery("select max(2, cast(NULL as integer)) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testScalarMinMaxSynonym() throws Exception {
        String expected = "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";
        try (ResultSet rs = methodWatcher.executeQuery("select least(3, 5, 1, 4, 2) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        expected = "1 |\n" +
                "----\n" +
                " 5 |\n" +
                " 5 |\n" +
                " 5 |";
        try (ResultSet rs = methodWatcher.executeQuery("select greatest(3, 5, 1, 4, 2) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testScalarMinMaxInteger() throws Exception {
        String expected = "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";
        try (ResultSet rs = methodWatcher.executeQuery("select min(3, 5, 1, 4, 2) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        expected = "1 |\n" +
                "----\n" +
                " 5 |\n" +
                " 5 |\n" +
                " 5 |";
        try (ResultSet rs = methodWatcher.executeQuery("select max(3, 5, 1, 4, 2) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        expected = "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 2 |";
        try (ResultSet rs = methodWatcher.executeQuery("select min(i, 2) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        expected = "1 |\n" +
                "----\n" +
                " 2 |\n" +
                " 2 |\n" +
                " 3 |";
        try (ResultSet rs = methodWatcher.executeQuery("select max(i, 2) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testScalarMinMaxDouble() throws Exception {
        String expected = "1  |\n" +
                "-----\n" +
                "1.0 |\n" +
                "2.0 |\n" +
                "2.0 |";
        try (ResultSet rs = methodWatcher.executeQuery("select min(db, double('2.0')) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        expected = "1  |\n" +
                "-----\n" +
                "2.0 |\n" +
                "2.0 |\n" +
                "3.0 |";
        try (ResultSet rs = methodWatcher.executeQuery("select max(db, double('2.0')) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testScalarMinMaxDecimal() throws Exception {
        String expected = "1  |\n" +
                "------\n" +
                "1.20 |\n" +
                "2.20 |\n" +
                "2.25 |";

        // note that dc is DECIMAL(3,1) and 2.25 is DECIMAL(3,2), see result type has scale = 2 above
        try (ResultSet rs = methodWatcher.executeQuery("select min(dc, 2.25) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        expected = "1  |\n" +
                "------\n" +
                "2.25 |\n" +
                "2.25 |\n" +
                "3.20 |";

        // note that dc is DECIMAL(3,1) and 2.25 is DECIMAL(3,2), see result type has scale = 2 above
        try (ResultSet rs = methodWatcher.executeQuery("select max(dc, 2.25) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testScalarMinMaxChar() throws Exception {
        String expected = "1 |\n" +
                "----\n" +
                "aa |\n" +
                "bb |\n" +
                "bb |";

        try (ResultSet rs = methodWatcher.executeQuery("select min('bb', c) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        expected = "1 |\n" +
                "----\n" +
                "bb |\n" +
                "bb |\n" +
                "cc |";

        try (ResultSet rs = methodWatcher.executeQuery("select max('bb', c) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testScalarMinMaxVarchar() throws Exception {
        String expected = "1 |\n" +
                "----\n" +
                "aa |\n" +
                "bb |\n" +
                "bb |";

        try (ResultSet rs = methodWatcher.executeQuery("select min('bb', vc) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        expected = "1 |\n" +
                "----\n" +
                "bb |\n" +
                "bb |\n" +
                "cc |";

        try (ResultSet rs = methodWatcher.executeQuery("select max('bb', vc) from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testScalarMinMaxTimestamp() throws Exception {
        String expected = "1           |\n" +
                "-----------------------\n" +
                "1960-01-01 23:03:20.0 |\n" +
                "1960-01-02 20:00:00.0 |\n" +
                "1960-01-02 20:00:00.0 |";

        try (ResultSet rs = methodWatcher.executeQuery("select min(ts, '1960-01-02 20:00:00') from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        expected = "1           |\n" +
                "-----------------------\n" +
                "1960-01-02 20:00:00.0 |\n" +
                "1960-01-02 23:03:20.0 |\n" +
                "1960-01-04 23:03:20.0 |";

        try (ResultSet rs = methodWatcher.executeQuery("select max(ts, '1960-01-02 20:00:00') from " + spliceTableWatcher2)) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testScalarMinMaxUnsupportedTypes() {
        // blob
        try {
            methodWatcher.executeQuery("select min(bl, bl) from " + spliceTableWatcher2);
            Assert.fail("expect failure since scalar min function is not an aggregate");
        } catch (SQLException e) {
            assertEquals("42ZD6", e.getSQLState());
        }

        try {
            methodWatcher.executeQuery("select max(bl, bl) from " + spliceTableWatcher2);
            Assert.fail("expect failure since scalar min function is not an aggregate");
        } catch (SQLException e) {
            assertEquals("42ZD6", e.getSQLState());
        }

        // clob
        try {
            methodWatcher.executeQuery("select min(cl, cl) from " + spliceTableWatcher2);
            Assert.fail("expect failure since scalar min function is not an aggregate");
        } catch (SQLException e) {
            assertEquals("42ZD6", e.getSQLState());
        }

        try {
            methodWatcher.executeQuery("select max(cl, cl) from " + spliceTableWatcher2);
            Assert.fail("expect failure since scalar min function is not an aggregate");
        } catch (SQLException e) {
            assertEquals("42ZD6", e.getSQLState());
        }
    }

    private void testDateTimeToCharVarcharHelper(String funcName, String columnName, String format,
                                                 boolean isDistinct, boolean compareFirstRowOnly, String expected) throws Exception {
        String queryTemplate = "select %s %s(%s %s) from " + spliceTableWatcher4;
        String query = format(queryTemplate, (isDistinct ? "distinct" : ""), funcName, columnName, (format.isEmpty() ? "" : (", " + format)));

        if (compareFirstRowOnly) {
            firstRowContainsQuery(query, expected, methodWatcher);
        } else {
            try (ResultSet rs = methodWatcher.executeQuery(query)) {
                Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
        }
    }

    @Test
    public void testDateTimeToCharVarchar() throws Exception {
        // test date
        testDateTimeToCharVarcharHelper("char", "d", "", true, true, "1960-03-01");
        testDateTimeToCharVarcharHelper("char", "d", "iso", true, true, "1960-03-01");
        testDateTimeToCharVarcharHelper("char", "d", "jis", true, true, "1960-03-01");
        testDateTimeToCharVarcharHelper("char", "d", "eur", true, true, "01.03.1960");
        testDateTimeToCharVarcharHelper("char", "d", "usa", true, true, "03/01/1960");
        try {
            testDateTimeToCharVarcharHelper("char", "d", "local", true, true, "01.03.1960");
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }

        testDateTimeToCharVarcharHelper("varchar", "d", "", true, true, "1960-03-01");
        testDateTimeToCharVarcharHelper("varchar", "d", "iso", true, true, "1960-03-01");
        testDateTimeToCharVarcharHelper("varchar", "d", "jis", true, true, "1960-03-01");
        testDateTimeToCharVarcharHelper("varchar", "d", "eur", true, true, "01.03.1960");
        testDateTimeToCharVarcharHelper("varchar", "d", "usa", true, true, "03/01/1960");
        try {
            testDateTimeToCharVarcharHelper("varchar", "d", "local", true, true, "01.03.1960");
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }

        // test time
        String expectedJIS = "1    |\n" +
                "----------\n" +
                "00:00:00 |\n" +
                "00:00:00 |\n" +
                "00:01:00 |\n" +
                "12:00:00 |\n" +
                "12:01:00 |\n" +
                "23:03:20 |";
        testDateTimeToCharVarcharHelper("char", "t", "", false, false, expectedJIS);
        testDateTimeToCharVarcharHelper("char", "t", "jis", false, false, expectedJIS);

        testDateTimeToCharVarcharHelper("varchar", "t", "", false, false, expectedJIS);
        testDateTimeToCharVarcharHelper("varchar", "t", "jis", false, false, expectedJIS);

        String expectedEUR = "1    |\n" +
                "----------\n" +
                "00.00.00 |\n" +
                "00.00.00 |\n" +
                "00.01.00 |\n" +
                "12.00.00 |\n" +
                "12.01.00 |\n" +
                "23.03.20 |";
        testDateTimeToCharVarcharHelper("char", "t", "iso", false, false, expectedEUR);
        testDateTimeToCharVarcharHelper("char", "t", "eur", false, false, expectedEUR);

        testDateTimeToCharVarcharHelper("varchar", "t", "iso", false, false, expectedEUR);
        testDateTimeToCharVarcharHelper("varchar", "t", "eur", false, false, expectedEUR);

        String expectedUSA = "1    |\n" +
                "----------\n" +
                "11:03 PM |\n" +
                "12:00 AM |\n" +
                "12:00 AM |\n" +
                "12:00 PM |\n" +
                "12:01 AM |\n" +
                "12:01 PM |";

        testDateTimeToCharVarcharHelper("char", "t", "usa", false, false, expectedUSA);

        testDateTimeToCharVarcharHelper("varchar", "t", "usa", false, false, expectedUSA);

        try {
            testDateTimeToCharVarcharHelper("char", "t", "local", false, false, expectedEUR);
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }

        try {
            testDateTimeToCharVarcharHelper("varchar", "t", "local", false, false, expectedEUR);
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }

        // test timestamp, format should not be accepted
        String expected = "1960-01-01 23:03:20.0";
        testDateTimeToCharVarcharHelper("char", "ts", "", true, true, expected);
        testDateTimeToCharVarcharHelper("varchar", "ts", "", true, true, expected);

        try {
            testDateTimeToCharVarcharHelper("char", "ts", "jis", true, true, expected);
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }

        try {
            testDateTimeToCharVarcharHelper("varchar", "ts", "jis", true, true, expected);
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }
    }

    @Test
    public void testDateTimeToCharVarcharConstants() throws Exception {
        // we need to test constant values passed in CastNode because they are casted in binding time
        String date = "date('2020-11-06')";
        testDateTimeToCharVarcharHelper("char", date, "", false, true, "2020-11-06");
        testDateTimeToCharVarcharHelper("char", date, "iso", false, true, "2020-11-06");
        testDateTimeToCharVarcharHelper("char", date, "jis", false, true, "2020-11-06");
        testDateTimeToCharVarcharHelper("char", date, "eur", false, true, "06.11.2020");
        testDateTimeToCharVarcharHelper("char", date, "usa", false, true, "11/06/2020");
        try {
            testDateTimeToCharVarcharHelper("char", date, "local", false, true, "06.11.2020");
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }

        testDateTimeToCharVarcharHelper("varchar", date, "", false, true, "2020-11-06");
        testDateTimeToCharVarcharHelper("varchar", date, "iso", false, true, "2020-11-06");
        testDateTimeToCharVarcharHelper("varchar", date, "jis", false, true, "2020-11-06");
        testDateTimeToCharVarcharHelper("varchar", date, "eur", false, true, "06.11.2020");
        testDateTimeToCharVarcharHelper("varchar", date, "usa", false, true, "11/06/2020");
        try {
            testDateTimeToCharVarcharHelper("varchar", date, "local", false, true, "06.11.2020");
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }

        String time = "time('09:35:03')";
        testDateTimeToCharVarcharHelper("char", time, "", false, true, "09:35:03");
        testDateTimeToCharVarcharHelper("char", time, "iso", false, true, "09.35.03");
        testDateTimeToCharVarcharHelper("char", time, "jis", false, true, "09:35:03");
        testDateTimeToCharVarcharHelper("char", time, "eur", false, true, "09.35.03");
        testDateTimeToCharVarcharHelper("char", time, "usa", false, true, "09:35 AM");
        try {
            testDateTimeToCharVarcharHelper("char", time, "local", false, true, "09.35.03");
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }

        testDateTimeToCharVarcharHelper("varchar", time, "", false, true, "09:35:03");
        testDateTimeToCharVarcharHelper("varchar", time, "iso", false, true, "09.35.03");
        testDateTimeToCharVarcharHelper("varchar", time, "jis", false, true, "09:35:03");
        testDateTimeToCharVarcharHelper("varchar", time, "eur", false, true, "09.35.03");
        testDateTimeToCharVarcharHelper("varchar", time, "usa", false, true, "09:35 AM");
        try {
            testDateTimeToCharVarcharHelper("varchar", time, "local", false, true, "09.35.03");
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }

        String timestamp = "timestamp('2020-11-06 09:35:03')";
        testDateTimeToCharVarcharHelper("char", timestamp, "", false, true, "2020-11-06 09:35:03.0");
        testDateTimeToCharVarcharHelper("varchar", timestamp, "", false, true, "2020-11-06 09:35:03.0");

        try {
            testDateTimeToCharVarcharHelper("char", timestamp, "jis", false, true, "2020-11-06 09:35:03.0");
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }

        try {
            testDateTimeToCharVarcharHelper("varchar", timestamp, "jis", false, true, "2020-11-06 09:35:03.0");
            Assert.fail("expect failure because LOCAL is not supported");
        } catch (SQLException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }
    }

    @Test
    public void testCastToCharWithLength() throws Exception {
        try (ResultSet ignored = methodWatcher.executeQuery("select char(current date, 20)")) {
            Assert.fail("expect failure since we cannot specify a length with something which is not a char/varchar");
        } catch (SQLException e) {
            assertEquals("42846", e.getSQLState());
            Assert.assertThat(e.getMessage(), containsString("Cannot set a length"));
        }
        try (ResultSet ignored = methodWatcher.executeQuery("select char(42, 20)")) {
            Assert.fail("expect failure since we cannot specify a length with something which is not a char/varchar");
        } catch (SQLException e) {
            assertEquals("42846", e.getSQLState());
            Assert.assertThat(e.getMessage(), containsString("Cannot set a length"));
        }
        try (ResultSet rs = methodWatcher.executeQuery("select char('abcd', 2)")) {
            rs.next();
            Assert.assertEquals("ab", rs.getString(1));
        }
        try (ResultSet rs = methodWatcher.executeQuery("select char('abcd', 6)")) {
            rs.next();
            Assert.assertEquals("abcd  ", rs.getString(1));
        }
    }

    @Test
    public void testCastNotDateToCharWithDateFormatFail() throws Exception {
        try (ResultSet ignored = methodWatcher.executeQuery("select char(1, ISO)")) {
            Assert.fail("expect failure since we cannot specify a date format if the first param isn't a datelike");
        } catch (SQLException e) {
            assertEquals("42846", e.getSQLState());
            Assert.assertThat(e.getMessage(), containsString("Date format is only applicable"));
        }
        try (ResultSet ignored = methodWatcher.executeQuery("select char('bonjour', EUR)")) {
            Assert.fail("expect failure since we cannot specify a date format if the first param isn't a datelike");
        } catch (SQLException e) {
            assertEquals("42846", e.getSQLState());
            Assert.assertThat(e.getMessage(), containsString("Date format is only applicable"));
        }

    }

    @Test
    public void testLengthOfCastToVarchar() throws Exception {
        // See DB-10618
        methodWatcher.executeUpdate("create table testLengthOfCastToVarchar (v varchar(10))");
        String sql = "select length(coalesce(max(substr(v,1,1)),char('',2))) from testLengthOfCastToVarchar --splice-properties useSpark=%s\n";
        try (ResultSet rs = methodWatcher.executeQuery(format(sql, true))) {
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
        try (ResultSet rs = methodWatcher.executeQuery(format(sql, false))) {
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    public void testCastToCharCcsidAscii() throws SQLException {
        try (ResultSet rs = methodWatcher.executeQuery("select cast('abc' as char(5)), cast('abc' as char(5) ccsid ascii)," +
                "cast('abc' as varchar(5)), cast('abc' as varchar(5) ccsid ascii)")) {
            rs.next();
            assertEquals(rs.getString(1), rs.getString(2));
            assertEquals(rs.getString(3), rs.getString(4));
        }
    }

    @Test
    public void testCastToCharForSbcsData() throws Exception
    {
        methodWatcher.executeUpdate("drop table sbcs if exists");
        methodWatcher.executeUpdate("create table sbcs(a varchar(30) for bit data, b char(30) for bit data)");
        methodWatcher.executeUpdate("insert into sbcs values (cast('a' as varchar(30) for bit data), cast('b' as char(30) for bit data))");
        methodWatcher.executeUpdate("insert into sbcs values (null, null)");
        String sql = "select cast(a as varchar(30)), cast(a as varchar(30) for sbcs data)," +
                "cast(b as char(30)), cast(b as char(30) for sbcs data) from sbcs --splice-properties useSpark=%s\n order by a";
            String expected = "1  |  2  |               3               |  4  |\n" +
                "--------------------------------------------------\n" +
                " 61  |  a  |622020202020202020202020202020 |  b  |\n" +
                "NULL |NULL |             NULL              |NULL |";
        try (ResultSet rs = methodWatcher.executeQuery(format(sql, false))) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
        try (ResultSet rs = methodWatcher.executeQuery(format(sql, true))) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    private String scalarFunctionSql(String column, boolean var, String extraParam, boolean useSpark) {
        String functionCall = format("%schar(%s%s)",
                var ? "var" : "",
                column,
                extraParam.isEmpty() ? "" : ", " + extraParam);
        return format("select %s as value, typeof(%s) as type from scalar_function --splice-properties useSpark=%s\n",
                functionCall, functionCall,
                useSpark);
    }

    private void scalarFunctionExpectSuccess(String column, boolean varcharElseChar, String extraParam, String expectedType, String expectedValue) throws SQLException {
        for (boolean useSpark : new boolean[]{false, true}) {
            try (ResultSet rs = methodWatcher.executeQuery(scalarFunctionSql(column, varcharElseChar, extraParam, useSpark))) {
                rs.next();
                assertEquals(expectedValue, rs.getString("value"));
                assertEquals(expectedType, rs.getString("type"));
            }
        }
    }

    private void scalarFunctionExpectFailure(String column, Boolean varcharElseChar, String extraParam, String expectedErrorCode) {
        boolean[] varIter;
        if (varcharElseChar == null) {
            varIter = new boolean[] {false, true};
        } else {
            varIter = new boolean[] {varcharElseChar};
        }
        for (boolean var : varIter) {
            for (boolean useSpark : new boolean[]{false, true}) {
                try (ResultSet rs = methodWatcher.executeQuery(scalarFunctionSql(column, var, extraParam, useSpark))) {
                    rs.next();
                    Assert.fail("should have failed but did not");
                } catch (SQLException e) {
                    Assert.assertEquals(expectedErrorCode, e.getSQLState());
                }
            }
        }
    }

    @Test
    public void testVarcharCharFunction() throws Exception
    {
        methodWatcher.executeUpdate("drop table scalar_function if exists");
        methodWatcher.executeUpdate("create table scalar_function (" +
                "num1 tinyint, num2 smallint, num3 int, num4 bigint," +
                "dec1 decimal(10, 4), dec2 decimal(18, 8), decfl1 decfloat, floating1 double, floating2 real," +
                "char1 char(30), char2 varchar(30)," +
                "datetime1 date, datetime2 time, datetime3 timestamp," +
                "bool1 boolean, bool2 boolean," +
                "null1 integer, notnull1 integer not null," +
                "bitchar1 char(30) for bit data, bitchar2 varchar(30) for bit data)");
                methodWatcher.executeUpdate("insert into scalar_function values (" +
                "1, 2, 3, 4," +
                "1, 2, 1, 1, 2," +
                "1111, 2222," +
                "'2020-01-02', '16:30:30', '2020-01-01 16:30:30.123456'," +
                "false, true," +
                "null, 1," +
                "'1111', '2222')");

                // Binary integer
        scalarFunctionExpectSuccess("num1", true, "", "VARCHAR(4)", "1");
        scalarFunctionExpectSuccess("num2", true, "", "VARCHAR(6)", "2");
        scalarFunctionExpectSuccess("num3", true, "", "VARCHAR(11)", "3");
        scalarFunctionExpectSuccess("num4", true, "", "VARCHAR(20)", "4");
        scalarFunctionExpectSuccess("num1", false, "", "CHAR(4)", "1   ");
        scalarFunctionExpectSuccess("num2", false, "", "CHAR(6)", "2     ");
        scalarFunctionExpectSuccess("num3", false, "", "CHAR(11)", "3          ");
        scalarFunctionExpectSuccess("num4", false, "", "CHAR(20)", "4                   ");

        scalarFunctionExpectFailure("num1", null, "1", "42846");
        scalarFunctionExpectFailure("num2", null, "1", "42846");
        scalarFunctionExpectFailure("num3", null, "1", "42846");
        scalarFunctionExpectFailure("num4", null, "1", "42846");
        scalarFunctionExpectFailure("num1", null, "ISO", "42846");
        scalarFunctionExpectFailure("num2", null, "ISO", "42846");
        scalarFunctionExpectFailure("num3", null, "ISO", "42846");
        scalarFunctionExpectFailure("num4", null, "ISO", "42846");

        // Decimal, decfloat and double
        scalarFunctionExpectSuccess("dec1", true, "", "VARCHAR(12)", "1");
        scalarFunctionExpectSuccess("dec2", true, "", "VARCHAR(20)", "2");
        // FIXME(DB-10938) Should be VARCHAR(42) instead of VARCHAR(35)
        scalarFunctionExpectSuccess("decfl1", true, "", "VARCHAR(35)", "1");
        scalarFunctionExpectSuccess("floating1", true, "", "VARCHAR(24)", "1.0E0");
        scalarFunctionExpectSuccess("floating2", true, "", "VARCHAR(24)", "2.0E0");
        scalarFunctionExpectSuccess("dec1", false, "", "CHAR(12)", "1           ");
        scalarFunctionExpectSuccess("dec2", false, "", "CHAR(20)", "2                   ");
        // FIXME(DB-10938) Should be CHAR(42) instead of CHAR(35)
        scalarFunctionExpectSuccess("decfl1", false, "", "CHAR(35)", "1                                  ");
        scalarFunctionExpectSuccess("floating1", false, "", "CHAR(24)", "1.0E0                   ");
        scalarFunctionExpectSuccess("floating2", false, "", "CHAR(24)", "2.0E0                   ");

        scalarFunctionExpectFailure("dec1", null, "1", "42846");
        scalarFunctionExpectFailure("dec2", null, "1", "42846");
        scalarFunctionExpectFailure("decfl1", null, "1", "42846");
        scalarFunctionExpectFailure("floating1", null, "1", "42846");
        scalarFunctionExpectFailure("floating2", null, "1", "42846");
        scalarFunctionExpectFailure("dec1", null, "ISO", "42846");
        scalarFunctionExpectFailure("dec2", null, "ISO", "42846");
        scalarFunctionExpectFailure("decfl1", null, "ISO", "42846");
        scalarFunctionExpectFailure("floating1", null, "ISO", "42846");
        scalarFunctionExpectFailure("floating2", null, "ISO", "42846");
        // Following three throw a syntax error because we do not support setting a decimal character yet
        scalarFunctionExpectFailure("dec1", null, "','", "42X01");
        scalarFunctionExpectFailure("dec2", null, "','", "42X01");
        scalarFunctionExpectFailure("decfl1", null, "','", "42X01");
        scalarFunctionExpectFailure("floating1", null, "','", "42X01");
        scalarFunctionExpectFailure("floating2", null, "','", "42X01");

        // Character string
        scalarFunctionExpectSuccess("char1", true, "", "VARCHAR(30)", "1111                          ");
        scalarFunctionExpectSuccess("char2", true, "", "VARCHAR(30)", "2222");
        scalarFunctionExpectSuccess("char1", true, "2", "VARCHAR(2)", "11");
        scalarFunctionExpectSuccess("char2", true, "2", "VARCHAR(2)", "22");
        scalarFunctionExpectSuccess("char1", true, "5", "VARCHAR(5)", "1111 ");
        scalarFunctionExpectSuccess("char2", true, "5", "VARCHAR(5)", "2222");
        scalarFunctionExpectSuccess("char1", true, "40", "VARCHAR(40)", "1111                          ");
        scalarFunctionExpectSuccess("char2", true, "40", "VARCHAR(40)", "2222");

        scalarFunctionExpectSuccess("char1", false, "", "CHAR(30)", "1111                          ");
        scalarFunctionExpectSuccess("char2", false, "", "CHAR(30)", "2222                          ");
        scalarFunctionExpectSuccess("char1", false, "2", "CHAR(2)", "11");
        scalarFunctionExpectSuccess("char2", false, "2", "CHAR(2)", "22");
        scalarFunctionExpectSuccess("char1", false, "5", "CHAR(5)", "1111 ");
        scalarFunctionExpectSuccess("char2", false, "5", "CHAR(5)", "2222 ");
        scalarFunctionExpectSuccess("char1", false, "40", "CHAR(40)", "1111                                    ");
        scalarFunctionExpectSuccess("char2", false, "40", "CHAR(40)", "2222                                    ");

        scalarFunctionExpectFailure("char1", null, "ISO", "42846");
        scalarFunctionExpectFailure("char2", null, "ISO", "42846");
        scalarFunctionExpectFailure("char1", true, "32673", "42611");
        scalarFunctionExpectFailure("char1", false, "256", "42611");

        // Datetime
        scalarFunctionExpectSuccess("datetime1", true, "", "VARCHAR(10)", "2020-01-02");
        scalarFunctionExpectSuccess("datetime2", true, "", "VARCHAR(8)", "16:30:30");
        scalarFunctionExpectSuccess("datetime3", true, "", "VARCHAR(29)", "2020-01-01 16:30:30.123456000");
        scalarFunctionExpectSuccess("datetime1", true, "ISO", "VARCHAR(10)", "2020-01-02");
        scalarFunctionExpectSuccess("datetime2", true, "ISO", "VARCHAR(8)", "16.30.30");
        scalarFunctionExpectSuccess("datetime1", true, "JIS", "VARCHAR(10)", "2020-01-02");
        scalarFunctionExpectSuccess("datetime2", true, "JIS", "VARCHAR(8)", "16:30:30");
        scalarFunctionExpectSuccess("datetime1", true, "EUR", "VARCHAR(10)", "02.01.2020");
        scalarFunctionExpectSuccess("datetime2", true, "EUR", "VARCHAR(8)", "16.30.30");
        scalarFunctionExpectSuccess("datetime1", true, "USA", "VARCHAR(10)", "01/02/2020");
        scalarFunctionExpectSuccess("datetime2", true, "USA", "VARCHAR(8)", "04:30 PM");

        scalarFunctionExpectSuccess("datetime1", false, "", "CHAR(10)", "2020-01-02");
        scalarFunctionExpectSuccess("datetime2", false, "", "CHAR(8)", "16:30:30");
        scalarFunctionExpectSuccess("datetime3", false, "", "CHAR(29)", "2020-01-01 16:30:30.123456000");
        scalarFunctionExpectSuccess("datetime1", false, "ISO", "CHAR(10)", "2020-01-02");
        scalarFunctionExpectSuccess("datetime2", false, "ISO", "CHAR(8)", "16.30.30");
        scalarFunctionExpectSuccess("datetime1", false, "JIS", "CHAR(10)", "2020-01-02");
        scalarFunctionExpectSuccess("datetime2", false, "JIS", "CHAR(8)", "16:30:30");
        scalarFunctionExpectSuccess("datetime1", false, "EUR", "CHAR(10)", "02.01.2020");
        scalarFunctionExpectSuccess("datetime2", false, "EUR", "CHAR(8)", "16.30.30");
        scalarFunctionExpectSuccess("datetime1", false, "USA", "CHAR(10)", "01/02/2020");
        scalarFunctionExpectSuccess("datetime2", false, "USA", "CHAR(8)", "04:30 PM");

        scalarFunctionExpectFailure("datetime1", null, "1", "42846");
        scalarFunctionExpectFailure("datetime2", null, "1", "42846");
        scalarFunctionExpectFailure("datetime3", null, "1", "42846");
        scalarFunctionExpectFailure("datetime3", null, "ISO", "22018");

        // Boolean
        scalarFunctionExpectSuccess("bool1", true, "", "VARCHAR(5)", "false");
        scalarFunctionExpectSuccess("bool2", true, "", "VARCHAR(5)", "true");
        scalarFunctionExpectSuccess("bool1", false, "", "CHAR(5)", "false");
        scalarFunctionExpectSuccess("bool2", false, "", "CHAR(5)", "true ");

        scalarFunctionExpectFailure("bool1", null, "1", "42846");
        scalarFunctionExpectFailure("bool1", null, "ISO", "42846");

        // Nullables
        scalarFunctionExpectSuccess("null1", true, "", "VARCHAR(11)", null);
        scalarFunctionExpectSuccess("notnull1", true, "", "VARCHAR(11) NOT NULL", "1");

        // Binary string
        scalarFunctionExpectSuccess("bitchar1", true, "", "VARCHAR (30) FOR BIT DATA", "313131312020202020202020202020202020202020202020202020202020");
        scalarFunctionExpectSuccess("bitchar2", true, "", "VARCHAR (30) FOR BIT DATA", "32323232");
        scalarFunctionExpectSuccess("bitchar1", true, "1", "VARCHAR (1) FOR BIT DATA", "31");
        scalarFunctionExpectSuccess("bitchar2", true, "1", "VARCHAR (1) FOR BIT DATA", "32");
        scalarFunctionExpectSuccess("bitchar1", true, "2", "VARCHAR (2) FOR BIT DATA", "3131");
        scalarFunctionExpectSuccess("bitchar2", true, "2", "VARCHAR (2) FOR BIT DATA", "3232");
        scalarFunctionExpectSuccess("bitchar1", true, "5", "VARCHAR (5) FOR BIT DATA", "3131313120");
        scalarFunctionExpectSuccess("bitchar2", true, "5", "VARCHAR (5) FOR BIT DATA", "32323232");
        scalarFunctionExpectSuccess("bitchar1", true, "40", "VARCHAR (40) FOR BIT DATA", "313131312020202020202020202020202020202020202020202020202020");
        scalarFunctionExpectSuccess("bitchar2", true, "40", "VARCHAR (40) FOR BIT DATA", "32323232");

        scalarFunctionExpectSuccess("bitchar1", false, "", "CHAR (30) FOR BIT DATA", "313131312020202020202020202020202020202020202020202020202020");
        scalarFunctionExpectSuccess("bitchar2", false, "", "CHAR (30) FOR BIT DATA", "323232322020202020202020202020202020202020202020202020202020");
        scalarFunctionExpectSuccess("bitchar1", false, "1", "CHAR (1) FOR BIT DATA", "31");
        scalarFunctionExpectSuccess("bitchar2", false, "1", "CHAR (1) FOR BIT DATA", "32");
        scalarFunctionExpectSuccess("bitchar1", false, "2", "CHAR (2) FOR BIT DATA", "3131");
        scalarFunctionExpectSuccess("bitchar2", false, "2", "CHAR (2) FOR BIT DATA", "3232");
        scalarFunctionExpectSuccess("bitchar1", false, "5", "CHAR (5) FOR BIT DATA", "3131313120");
        scalarFunctionExpectSuccess("bitchar2", false, "5", "CHAR (5) FOR BIT DATA", "3232323220");
        scalarFunctionExpectSuccess("bitchar1", false, "40", "CHAR (40) FOR BIT DATA", "31313131202020202020202020202020202020202020202020202020202020202020202020202020");
        scalarFunctionExpectSuccess("bitchar2", false, "40", "CHAR (40) FOR BIT DATA", "32323232202020202020202020202020202020202020202020202020202020202020202020202020");

        scalarFunctionExpectFailure("bitchar1", true, "32673", "42611");
        scalarFunctionExpectFailure("bitchar1", false, "256", "42611");
        scalarFunctionExpectFailure("bitchar2", true, "32673", "42611");
        scalarFunctionExpectFailure("bitchar2", false, "256", "42611");
        scalarFunctionExpectFailure("bitchar1", null, "ISO", "42846");
        scalarFunctionExpectFailure("bitchar2", null, "ISO", "42846");
    }
}

