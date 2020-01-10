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

package com.splicemachine.derby.test;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.subquery.SubqueryITUtil.*;
import static org.junit.Assert.assertEquals;

public class TPCHIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = "TPCH1X";
    private static final String LINEITEM = "LINEITEM";
    private static final String ORDERS = "ORDERS";
    private static final String CUSTOMERS = "CUSTOMER";
    private static final String PARTSUPP = "PARTSUPP";
    private static final String SUPPLIER = "SUPPLIER";
    private static final String PART = "PART";
    private static final String NATION = "NATION";
    private static final String REGION = "REGION";

    @ClassRule
    public static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @BeforeClass
    public static void loadData() throws Exception {
        TestUtils.executeSqlFile(spliceClassWatcher, "tcph/TPCHIT.sql", SCHEMA_NAME);
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null)", SCHEMA_NAME, LINEITEM, getResource("lineitem.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null)", SCHEMA_NAME, ORDERS, getResource("orders.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null)", SCHEMA_NAME, CUSTOMERS, getResource("customer.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null)", SCHEMA_NAME, PARTSUPP, getResource("partsupp.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null)", SCHEMA_NAME, SUPPLIER, getResource("supplier.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null)", SCHEMA_NAME, PART, getResource("part.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null)", SCHEMA_NAME, NATION, getResource("nation.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null)", SCHEMA_NAME, REGION, getResource("region.tbl"))).execute();

        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s', false)", SCHEMA_NAME)).execute();

        // validate
        assertEquals(9958L, (long)spliceClassWatcher.query("select count(*) from " + LINEITEM));
        assertEquals(2500L, (long)spliceClassWatcher.query("select count(*) from " + ORDERS));
        assertEquals(250L, (long)spliceClassWatcher.query("select count(*) from " + CUSTOMERS));
        assertEquals(1332L, (long)spliceClassWatcher.query("select count(*) from " + PARTSUPP));
        assertEquals(16L, (long)spliceClassWatcher.query("select count(*) from " + SUPPLIER));
        assertEquals(333L, (long)spliceClassWatcher.query("select count(*) from " + PART));
        assertEquals(25L, (long)spliceClassWatcher.query("select count(*) from " + NATION));
        assertEquals(5L, (long)spliceClassWatcher.query("select count(*) from " + REGION));
    }


    @Test
    public void sql1() throws Exception {
        executeQuery(getContent("1.sql"), getContent("1.expected.txt"), true);
    }

    @Test
    public void sql2() throws Exception {
        String sql = getContent("2.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql3() throws Exception {
        executeQuery(getContent("3.sql"), "", true);
    }

    @Test
    public void sql4() throws Exception {
        String sql = getContent("4.sql");
        executeQuery(sql, getContent("4.expected.txt"), true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql5() throws Exception {
        executeQuery(getContent("5.sql"), "", true);
    }

    @Test
    public void sql6() throws Exception {
        executeQuery(getContent("6.sql"), getContent("6.expected.txt"), false);
    }

    @Test
    public void sql7() throws Exception {
        String sql = getContent("7.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql8() throws Exception {
        String sql = getContent("8.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql8InvalidMergeJoin() throws Exception {
        try {
            executeQuery(getContent("8-invalid-merge.sql"), "", true);
        } catch (SQLException e) {
            // Error expected due to invalid MERGE join:
            // ERROR 42Y69: No valid execution plan was found for this statement.
            assertEquals("42Y69", e.getSQLState());
        }
    }

    @Test
    public void sql9() throws Exception {
        String sql = getContent("9.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql9Repeated() throws Exception {
        for (int i = 0; i < 3; i++) {
            sql9();
        }
    }

    @Test
    public void sql10() throws Exception {
        executeQuery(getContent("10.sql"), "", true);
    }

    @Test
    public void sql11() throws Exception {
        String sql = getContent("11.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ONE_SUBQUERY_NODE);
    }

    @Test
    public void sql12() throws Exception {
        executeQuery(getContent("12.sql"), getContent("12.expected.txt"), true);
    }

    @Test
    public void sql13() throws Exception {
        String sql = getContent("13.sql");
        executeQuery(sql, getContent("13.expected.txt"), true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql14() throws Exception {
        executeQuery(getContent("14.sql"), getContent("14.expected.txt"), false);
    }

    @Test
    public void sql15() throws Exception {
        String sql15a = getContent("15a.sql");
        String sql15b = getContent("15b.sql");

        executeUpdate(sql15a);
        executeQuery(sql15b, "", false);

        assertSubqueryNodeCount(conn(), sql15b, ONE_SUBQUERY_NODE);
    }

    @Test
    public void sql16() throws Exception {
        String sql = getContent("16.sql");
        executeQuery(sql, getContent("16.expected.txt"), true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql17() throws Exception {
        String sql = getContent("17.sql");
        executeQuery(sql, getContent("17.expected.txt"), false);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test(timeout = 30000)
    public void sql18() throws Exception {
        String sql = getContent("18.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql19() throws Exception {
        executeQuery(getContent("19.sql"), getContent("19.expected.txt"), false);
    }

    @Test
    public void sql20() throws Exception {
        String sql = getContent("20.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql21() throws Exception {
        String sql = getContent("21.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql22() throws Exception {
        String sql = getContent("22.sql");
        executeQuery(sql, getContent("22.expected.txt"), true);
        assertSubqueryNodeCount(conn(), sql, ONE_SUBQUERY_NODE);
    }

    @Test
    public void testPredicatePushdownOnRightSideOfJoin() throws Exception {
        rowContainsQuery(7,"explain select count(*) from --splice-properties joinOrder=fixed\n" +
                " ORDERS, LINEITEM --splice-properties joinStrategy=BROADCAST\n" +
                " where l_orderkey = o_orderkey and l_shipdate > date('1995-03-15') and o_orderdate > date('1995-03-15')","preds=[(L_SHIPDATE[2:2] > 1995-03-15)]",methodWatcher);
    }

    @Test(expected = SQLException.class)
    public void noMergeOverMergeSort() throws Exception {
        String mergeOverMergeSort = "select s_name from  --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "%s.supplier, " +
                "%s.nation, " +
                "%s.lineitem l3 --SPLICE-PROPERTIES joinStrategy=SORTMERGE\n " +
                " ,%s.orders --SPLICE-PROPERTIES joinStrategy=MERGE\n" +
                " where " +
                "s_suppkey = l3.l_suppkey " +
                "and o_orderkey = l3.l_orderkey";
        methodWatcher.executeQuery(format(mergeOverMergeSort,SCHEMA_NAME,SCHEMA_NAME,SCHEMA_NAME,SCHEMA_NAME));
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void executeQuery(String query, String expected, boolean isResultSetOrdered) throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(query);

        // If the ResultSet is NOT ordered (no order by clause in query) then sort it before comparing to expected result.
        // When we don't sort we are assuming the order by clause gives the ResultSet a unique order-- seems to be
        // the case for this data set (no duplicates in result set order by columns).
        boolean sort = !isResultSetOrdered;

        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.convert("", resultSet, sort).toString().trim());
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    public static String getResource(String name) {
        return SpliceUnitTest.getResourceDirectory() + "tcph/data/" + name;
    }

    private static String getContent(String fileName) throws IOException {
        String fullFileName = SpliceUnitTest.getResourceDirectory() + "tcph/query/" + fileName;
        return IOUtils.toString(new FileInputStream(new File(fullFileName)));
    }

    private void executeUpdate(String query) throws Exception {
        methodWatcher.executeUpdate(query);
    }

    private TestConnection conn() {
        return methodWatcher.getOrCreateConnection();
    }

}
