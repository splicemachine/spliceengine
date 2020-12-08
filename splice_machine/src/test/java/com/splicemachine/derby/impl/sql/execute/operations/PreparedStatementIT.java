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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for Prepared Statements
 *
 * @author Jeff Cunningham
 *         Date: 6/19/13
 */
public class PreparedStatementIT { 
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

        ps.setDouble(1, 1.0);
        ps.setString(2, "1.0");
        testReturnRowCount(ps, 0);

        ps.setDouble(1, 1.0);
        ps.setString(2, "1.0E0");
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
}
