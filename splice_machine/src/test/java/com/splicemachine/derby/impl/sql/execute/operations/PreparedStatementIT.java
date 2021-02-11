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
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for Prepared Statements
 *
 * @author Jeff Cunningham
 *         Date: 6/19/13
 */
public class PreparedStatementIT extends SpliceUnitTest {
    private static final String CLASS_NAME = PreparedStatementIT.class.getSimpleName().toUpperCase();

    private static final List<String> customerVals = Arrays.asList(
            "(1, 'Smith', 'Will', 'Berkeley','CA')",
            "(2, 'Smith', 'Jones', 'San Francisco','CA')",
            "(3, 'Smith', 'Jane', 'Los Angeles','CA')",
            "(4, 'Smith', 'Lauren', 'Sacramento','CA')",
            "(5, 'Doe', 'Jane', 'Baltimore','MD')");

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher tableSchema = new SpliceSchemaWatcher(CLASS_NAME);

    private static final String CUST_TABLE_NAME = "customers";
    private static final String CUST_TABLE_DEF = "(customerid integer not null primary key, lastname varchar(30), firstname varchar(10), city varchar(19), state char(2))";
    protected static SpliceTableWatcher custTable = new SpliceTableWatcher(CUST_TABLE_NAME,CLASS_NAME, CUST_TABLE_DEF);
    public static final String SELECT_STAR_QUERY = String.format("select * from %s.%s", tableSchema.schemaName, CUST_TABLE_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
        .around(tableSchema)
        .around(custTable)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                   for(String rowVal:customerVals){
                       try {
                           spliceClassWatcher.getStatement().executeUpdate("insert into "+ custTable.toString()+" values "+ rowVal);
                       } catch (Exception e) {
                           throw new RuntimeException(e);
                       }
                   }
                }
            });

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    private TestConnection conn;

    @Before
    public void fillTable() throws Exception {
        conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
//        //  load customer table
//        for (String rowVal : customerVals) {
//            spliceClassWatcher.getStatement().executeUpdate("insert into " + custTable.toString() + " values " + rowVal);
//        }
    }
//
    @After
    public void emptyTable() throws Exception {
        conn.rollback();
        conn.reset();
//        //  empty customer table
//        spliceClassWatcher.getStatement().execute(String.format("delete from %s where customerid > 0", custTable.toString()));
    }

    /**
     * Bug 534 - delete PS accumulating modified row results
     *
     * @throws Exception fail
     */
    @Test
    public void testPrepStatementDeleteResultCount() throws Exception {
        // should see all 5 rows
        ResultSet rs = conn.prepareStatement(SELECT_STAR_QUERY).executeQuery();
        Assert.assertEquals(5, SpliceUnitTest.resultSetSize(rs));

        String rowsToDeleteQuery = String.format("select * from %s.%s where customerid >= 4", tableSchema.schemaName, CUST_TABLE_NAME);
        // should see 2 rows that will be deleted by prepared statement
        rs = conn.query(rowsToDeleteQuery);
        Assert.assertEquals(2, SpliceUnitTest.resultSetSize(rs));

        PreparedStatement psc1 = conn.prepareStatement(String.format("delete from %s.%s where customerid >= 4",
                tableSchema.schemaName, CUST_TABLE_NAME));
        // should delete 2 rows
        int effected = psc1.executeUpdate();
        Assert.assertEquals(2, effected);

        // expecting 0 rows after delete
        rs = conn.query(rowsToDeleteQuery);
        Assert.assertEquals(0, SpliceUnitTest.resultSetSize(rs));

        // expecting 3 rows remaining after delete
        rs = conn.query(SELECT_STAR_QUERY);
        Assert.assertEquals(3, SpliceUnitTest.resultSetSize(rs));

        // should delete nothing
        effected = psc1.executeUpdate();
        Assert.assertEquals(0, effected);
    }

    /**
     * Bug 534 - update PS accumulating modified row results
     *
     * @throws Exception fail
     */
    @Test
    public void testPrepStatementUpdateResultCount() throws Exception {

        // should see all 5 rows
        ResultSet rs = conn.query(SELECT_STAR_QUERY);
        Assert.assertEquals(5, SpliceUnitTest.resultSetSize(rs));

        PreparedStatement psc1 = conn.prepareStatement(String.format("delete from %s.%s where customerid >= 3",
                tableSchema.schemaName, CUST_TABLE_NAME));
        int effected = psc1.executeUpdate();
        // should delete 3 rows
        Assert.assertEquals(3, effected);

        rs = conn.query(SELECT_STAR_QUERY);
        // should see 2 rows left
        Assert.assertEquals(2, SpliceUnitTest.resultSetSize(rs));

        PreparedStatement psc2 = conn.prepareStatement(String.format("update %s.%s set customerid = customerid + 10 where customerid > 1",
                tableSchema.schemaName, CUST_TABLE_NAME));
        effected = psc2.executeUpdate();
        // should update 1 row
        Assert.assertEquals(1, effected);

        effected = psc1.executeUpdate();
        // should delete 1 row
        Assert.assertEquals(1, effected);

        rs = conn.query(SELECT_STAR_QUERY);
        // 1 row remains
        Assert.assertEquals(1, SpliceUnitTest.resultSetSize(rs));

        effected = psc2.executeUpdate();
        // no rows updated
        Assert.assertEquals(0, effected);
    }

    /**
     * Bug 534 - update PS accumulating modified row results
     *
     * @throws Exception fail
     */
    @Test
    public void testPrepStatementMultipleUpdateResultCount() throws Exception {
        // should see all 5 rows
        ResultSet rs = conn.query(SELECT_STAR_QUERY);
        Assert.assertEquals(5, SpliceUnitTest.resultSetSize(rs));

        PreparedStatement ps1 = conn.prepareStatement(String.format("update %s.%s set city = ? where lastname = 'Smith' and state = 'CA'",
                tableSchema.schemaName, CUST_TABLE_NAME));
        ps1.setString(1, "Sacto");
        int effected = ps1.executeUpdate();
        // should update 1 row
        Assert.assertEquals(4, effected);

        int total = 0;
        for (int i=0; i<10; i++) {
            ps1.setString(1, "Sacto"+i);
            effected = ps1.executeUpdate();
            // 4 rows updated
            Assert.assertEquals(4, effected);
            total += effected;
        }
        Assert.assertEquals(40, total);
    }


    /**
     * Bug 534 - insert PS accumulating modified row results
     *
     * @throws Exception fail
     */
    @Test
    public void testPrepStatementInsertResultCount() throws Exception {
        // should see all 5 rows
        ResultSet rs = conn.query(SELECT_STAR_QUERY);
        Assert.assertEquals(5, SpliceUnitTest.resultSetSize(rs));

        PreparedStatement psc = conn.prepareStatement(String.format("insert into %s.%s values (?,?,?,?,?)",
                tableSchema.schemaName, CUST_TABLE_NAME));
        for (int i =6; i<16; i++) {
            psc.setInt(1, i);
            psc.setString(2, "fname_" + i);
            psc.setString(3, "lname_" + i);
            psc.setString(4, "St. Louis");
            psc.setString(5, "MO");
            int effected = psc.executeUpdate();
            // should see 1 row
            Assert.assertEquals(1, effected);
        }
    }

    @Test
    public void testPrepStatementGroupedAggregate() throws Exception {
        // should see all 5 rows
        ResultSet rs = conn.query(SELECT_STAR_QUERY);
        Assert.assertEquals(5, SpliceUnitTest.resultSetSize(rs));

        PreparedStatement psc = conn.prepareStatement(String.format("select lastname, count(*) from %s.%s where lastname=? group by lastname",
                tableSchema.schemaName, CUST_TABLE_NAME));
        psc.setString(1, "Smith");
        rs = psc.executeQuery();
        rs.next();
        Assert.assertEquals(rs.getInt(2), 4);
        rs.close();

        psc.setString(1, "Doe");
        rs = psc.executeQuery();
        rs.next();
        Assert.assertEquals(rs.getInt(2), 1);
        rs.close();
    }

    @Test
    public void testPrepStatementGroupedAggregateViaSpark() throws Exception {
        // should see all 5 rows
        ResultSet rs = conn.query(SELECT_STAR_QUERY);
        Assert.assertEquals(5, SpliceUnitTest.resultSetSize(rs));

        PreparedStatement psc = conn.prepareStatement(String.format("select lastname, count(*) from %s.%s --splice-properties useSpark=true\n where lastname=? group by lastname",
                tableSchema.schemaName, CUST_TABLE_NAME));
        psc.setString(1, "Smith");
        rs = psc.executeQuery();
        rs.next();
        Assert.assertEquals(rs.getInt(2), 4);
        rs.close();

        psc.setString(1, "Doe");
        rs = psc.executeQuery();
        rs.next();
        Assert.assertEquals(rs.getInt(2), 1);
        rs.close();
    }

    @Test
    public void testPrepStatementMultiProbeScanOnEqualParameter() throws Exception {
        String tableName = "TEST_PK_MULTI_PROBE";
        methodWatcher.executeUpdate(String.format("create table if not exists %s.%s (col1 int, col2 int, primary key(col1, col2))",
                tableSchema.schemaName, tableName));
        methodWatcher.executeUpdate(String.format("delete from %s.%s",
                tableSchema.schemaName, tableName));
        methodWatcher.executeUpdate(String.format("insert into %s.%s values (1,1), (2,2), (3,3), (4,4), (5,5)",
                tableSchema.schemaName, tableName));

        String query = String.format("select * from %s.%s where col1=? and col2 in (?,?)", tableSchema.schemaName, tableName);
        PreparedStatement ps = conn.prepareStatement("explain " + query);
        ps.setInt(1, 1);
        ps.setInt(2, 1);
        ps.setInt(3, 4);

        try(ResultSet rs = ps.executeQuery()) {
            String plan = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(plan.contains("MultiProbeTableScan"));
            Assert.assertFalse(plan.contains("->  TableScan"));
        }

        ps = conn.prepareStatement(query);
        ps.setInt(1, 1);
        ps.setInt(2, 1);
        ps.setInt(3, 4);

        String expected =
                "COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |  1  |";

        try(ResultSet rs = ps.executeQuery()) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    private void testReturnRowCount(PreparedStatement ps, int expectedCount) throws Exception {
        try(ResultSet rs = ps.executeQuery()) {
            Assert.assertEquals(expectedCount, SpliceUnitTest.resultSetSize(rs));
        }
    }

    @Test
    public void testPrepStatementParameterInConstantRestriction() throws Exception {
        PreparedStatement ps = conn.prepareStatement(SELECT_STAR_QUERY + " where 1 = ? {limit 1}");

        ps.setInt(1, 1);
        testReturnRowCount(ps, 1);

        ps.setInt(1, 2);
        testReturnRowCount(ps, 0);

        ps.setDouble(1, 1.0);
        testReturnRowCount(ps, 1);

        ps.setString(1, "2");
        testReturnRowCount(ps, 0);

        try {
            ps.setDate(1, Date.valueOf("2020-01-01"));
            Assert.fail("Expect failure in converting date to integer");
        } catch (Exception e) {
            Assert.assertEquals("An attempt was made to get a data value of type 'INTEGER' from a data value of type 'DATE'.", e.getMessage());
        }
    }

    @Test
    public void testPrepStatementParameterOnBothSidesOfBinaryComparison() throws Exception {
        PreparedStatement ps = conn.prepareStatement(SELECT_STAR_QUERY + " where ? = ? {limit 1}");

        ps.setInt(1, 1);
        ps.setInt(2, 1);
        testReturnRowCount(ps, 1);

        ps.setInt(1, 1);
        ps.setInt(2, 0);
        testReturnRowCount(ps, 0);

        ps.setDouble(1, 1.0);
        ps.setDouble(2, 1.0);
        testReturnRowCount(ps, 1);

        ps.setDouble(1, 1.0);
        ps.setDouble(2, 1.5);
        testReturnRowCount(ps, 0);

        ps.setString(1, "aa");
        ps.setString(2, "aa");
        testReturnRowCount(ps, 1);

        ps.setString(1, "aa");
        ps.setString(2, "ab");
        testReturnRowCount(ps, 0);

        ps.setDate(1, Date.valueOf("2020-01-01"));
        ps.setDate(2, Date.valueOf("2020-01-01"));
        testReturnRowCount(ps, 1);

        ps.setDate(1, Date.valueOf("2020-01-01"));
        ps.setDate(2, Date.valueOf("2020-02-01"));
        testReturnRowCount(ps, 0);

        ps.setInt(1, 12);
        ps.setString(2, "12");
        testReturnRowCount(ps, 1);

        // DB2 returns 0 row in this case. But
        // '1.0' = cast(1.0 as varchar(254))
        // does evaluated to true in DB2.
        ps.setDouble(1, 1.0);
        ps.setString(2, "1.0");
        testReturnRowCount(ps, 1);

        ps.setString(1, "2020-01-01");
        ps.setDate(2, Date.valueOf("2020-01-01"));
        testReturnRowCount(ps, 1);

        ps.setInt(1, 1);
        ps.setDouble(2, 1.0);
        testReturnRowCount(ps, 0);

        ps.setString(1, "2020-02-01");
        ps.setDate(2, Date.valueOf("2020-01-01"));
        testReturnRowCount(ps, 0);

        ps.setInt(1, 1577836800);
        ps.setDate(2, Date.valueOf("2020-01-01"));
        testReturnRowCount(ps, 0);
    }

    void testParamsHelper(String sqlText, Object[] args, String expected) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(sqlText)) {
            for (int i = 0; i < args.length; ++i) {
                Object arg = args[i];
                if (arg instanceof String) {
                    ps.setString(i + 1, (String) arg);
                } else if (arg instanceof Integer) {
                    ps.setInt(i + 1, (Integer) arg);
                } else if (arg instanceof Double) {
                    ps.setDouble(i + 1, (Double) arg);
                } else if (arg instanceof BigDecimal) {
                    ps.setBigDecimal(i + 1, (BigDecimal) arg);
                }
            }

            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            }
        }
    }

    void testOneParamHelper(String sqlText, Object arg, String expected) throws Exception {
        testParamsHelper(sqlText, new Object[]{arg}, expected);
    }

    @Test
    public void testParameterOperandInUnaryOperator() throws Exception {
        /* unary date/time operator */

        String expected = "1     |\n" +
                "------------\n" +
                "2010-04-16 |";
        testOneParamHelper("select date(?) + 105 days from sysibm.sysdummy1", "2010-01-01", expected);

        expected = "1    |\n" +
                "----------\n" +
                "01:02:03 |";
        testOneParamHelper("select time(?) from sysibm.sysdummy1", "01:02:03", expected);

        expected = "1           |\n" +
                "-----------------------\n" +
                "2010-01-02 01:02:03.0 |";
        testOneParamHelper("select timestamp(?) + 1 days from sysibm.sysdummy1", "2010-01-01 01:02:03", expected);

        /* extract operator */

        expected = "1  |\n" +
                "------\n" +
                "2010 |";
        testOneParamHelper("select year(?) from sysibm.sysdummy1", "2010-01-02", expected);

        expected = "1 |\n" +
                "----\n" +
                " 1 |";
        testOneParamHelper("select quarter(?) from sysibm.sysdummy1", "2010-01-02", expected);

        expected = "1 |\n" +
                "----\n" +
                " 1 |";
        testOneParamHelper("select month(?) from sysibm.sysdummy1", "2010-01-02", expected);

        expected = "1 |\n" +
                "----\n" +
                " 2 |";
        testOneParamHelper("select day(?) from sysibm.sysdummy1", "2010-01-02", expected);

        expected = "1 |\n" +
                "----\n" +
                " 1 |";
        testOneParamHelper("select hour(?) from sysibm.sysdummy1", "01:02:03", expected);

        expected = "1 |\n" +
                "----\n" +
                " 2 |";
        testOneParamHelper("select minute(?) from sysibm.sysdummy1", "01:02:03", expected);

        expected = "1 |\n" +
                "----\n" +
                " 3 |";
        testOneParamHelper("select second(?) from sysibm.sysdummy1", "01:02:03", expected);

        expected = "1 |\n" +
                "----\n" +
                " 1 |";
        testOneParamHelper("select days(?) from sysibm.sysdummy1", "0001-01-01", expected);
    }

    @Test
    public void testCastOnParameter() throws Exception {
        String expected = "1 |\n" +
                "----\n" +
                "20 |";
        testOneParamHelper("select int(?) from sysibm.sysdummy1", "20", expected);

        expected = "1 |\n" +
                "----\n" +
                "15 |";
        testOneParamHelper("select char(?) from sysibm.sysdummy1", 15, expected);

        expected = "1  |\n" +
                "------\n" +
                "0.02 |";
        testOneParamHelper("select varchar(?) from sysibm.sysdummy1", 0.02, expected);

        expected = "1   |\n" +
                "-------\n" +
                "12.50 |";
        testOneParamHelper("select cast(? as decimal(4,2)) from sysibm.sysdummy1", BigDecimal.valueOf(12.50), expected);
    }

    @Test
    public void testParameterOperandInDateTimeExpr() throws Exception {
        try {
            String expected = "1     |\n" +
                    "------------\n" +
                    "2010-04-16 |";
            testOneParamHelper("select ? + 105 days from sysibm.sysdummy1", "2010-01-01", expected);
            Assert.fail("Expect failure due to parameter in date/time expression.");
        } catch (SQLException e) {
            Assert.assertEquals("42816", e.getSQLState());
            Assert.assertTrue(e.getMessage().contains("A datetime value or duration in an expression is invalid"));
        }
    }

    @Test
    public void testParameterInSelectOfUnion() throws Exception {
        testOneParamHelper("select ? from sysibm.sysdummy1 union select 1 from sysibm.sysdummy1 order by 1", 4,
                "1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 4 |");
        testOneParamHelper("select 1 from sysibm.sysdummy1 union select ? from sysibm.sysdummy1 order by 1", 2,
                "1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |");

        testOneParamHelper("select ? from sysibm.sysdummy1 union select 2 from sysibm.sysdummy1 union select 3 from sysibm.sysdummy1 order by 1", 4,
                "1 |\n" +
                        "----\n" +
                        " 2 |\n" +
                        " 3 |\n" +
                        " 4 |");
        testOneParamHelper("select 1 from sysibm.sysdummy1 union select ? from sysibm.sysdummy1 union select 3 from sysibm.sysdummy1 order by 1", 4,
                "1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 3 |\n" +
                        " 4 |");
        testOneParamHelper("select 1 from sysibm.sysdummy1 union select 2 from sysibm.sysdummy1 union select ? from sysibm.sysdummy1 order by 1", 4,
                "1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 4 |");

        try (PreparedStatement ignored = conn.prepareStatement("select ? from sysibm.sysdummy1 union select ? from sysibm.sysdummy1")) {
            Assert.fail("Expect failure due to parameter in date/time expression.");
        } catch (SQLException e) {
            Assert.assertEquals("42Y10", e.getSQLState());
        }

        testParamsHelper("select ? from sysibm.sysdummy1 union select ? from sysibm.sysdummy1 union select 3 from sysibm.sysdummy1 order by 1",
                new Object[] {1, 2},
                "1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |\n" +
                        " 3 |");

        testOneParamHelper("select 1, 'a' from sysibm.sysdummy1 union select 4, ? from sysibm.sysdummy1 order by 1", "a",
                "1 | 2 |\n" +
                        "--------\n" +
                        " 1 | a |\n" +
                        " 4 | a |");

        testParamsHelper("select ?, 'a' from sysibm.sysdummy1 union select 4, ? from sysibm.sysdummy1 order by 1", new Object[]{5, "b"},
                "1 | 2 |\n" +
                        "--------\n" +
                        " 4 | b |\n" +
                        " 5 | a |");

        testParamsHelper("select 1, 'a' from sysibm.sysdummy1 union select ?, ? from sysibm.sysdummy1 order by 1", new Object[]{5, "b"},
                "1 | 2 |\n" +
                        "--------\n" +
                        " 1 | a |\n" +
                        " 5 | b |");

    }
}
