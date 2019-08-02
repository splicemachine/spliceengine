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

package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.*;
import org.spark_project.guava.base.Throwables;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.subquery.SubqueryITUtil.*;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by jyuan on 10/9/17.
 */
public class HBaseBulkLoadIndexIT extends SpliceUnitTest {
    private static final String SCHEMA_NAME = HBaseBulkLoadIndexIT.class.getSimpleName().toUpperCase();
    private static final String LINEITEM = "LINEITEM";
    private static final String LINEITEM2 = "LINEITEM2";
    private static final String ORDERS = "ORDERS";
    private static final String CUSTOMERS = "CUSTOMER";
    private static final String PARTSUPP = "PARTSUPP";
    private static final String SUPPLIER = "SUPPLIER";
    private static final String PART = "PART";
    private static final String NATION = "NATION";
    private static final String REGION = "REGION";
    private static boolean notSupported;

    @ClassRule
    public static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @BeforeClass
    public static void loaddata() throws Exception {
        try {
            TestUtils.executeSqlFile(spliceClassWatcher, "tcph/createTable.sql", SCHEMA_NAME);
            spliceClassWatcher.prepareStatement("create table lineitem2 as select * from lineitem with no data").execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, LINEITEM, getResource("lineitem.tbl"), getResource("data"))).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, LINEITEM2, getResource("lineitem.tbl"), getResource("data"))).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, ORDERS, getResource("orders.tbl"), getResource("data"))).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, CUSTOMERS, getResource("customer.tbl"), getResource("data"))).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, PARTSUPP, getResource("partsupp.tbl"), getResource("data"))).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, SUPPLIER, getResource("supplier.tbl"), getResource("data"))).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, PART, getResource("part.tbl"), getResource("data"))).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, NATION, getResource("nation.tbl"), getResource("data"))).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, REGION, getResource("region.tbl"), getResource("data"))).execute();

            spliceClassWatcher.prepareStatement(format("create index O_CUST_IDX on ORDERS(\n" +
                    " O_CUSTKEY,\n" +
                    " O_ORDERKEY\n" +
                    " ) auto splitkeys sample fraction 0.1 hfile location '%s'", getResource("data"))).execute();

            spliceClassWatcher.prepareStatement(format("create index O_DATE_PRI_KEY_IDX on ORDERS(\n" +
                    " O_ORDERDATE,\n" +
                    " O_ORDERPRIORITY,\n" +
                    " O_ORDERKEY\n" +
                    " ) auto splitkeys hfile location '%s'", getResource("data"))).execute();

            spliceClassWatcher.prepareStatement(format("create index L_SHIPDATE_IDX on LINEITEM(\n" +
                    " L_SHIPDATE,\n" +
                    " L_PARTKEY,\n" +
                    " L_EXTENDEDPRICE,\n" +
                    " L_DISCOUNT\n" +
                    " ) splitkeys location '%s' columnDelimiter '|' hfile location '%s'", getResource("shipDateIndex.csv"),  getResource("data"))).execute();

            spliceClassWatcher.prepareStatement(format(" create index L_PART_IDX on LINEITEM(\n" +
                    " L_PARTKEY,\n" +
                    " L_ORDERKEY,\n" +
                    " L_SUPPKEY,\n" +
                    " L_SHIPDATE,\n" +
                    " L_EXTENDEDPRICE,\n" +
                    " L_DISCOUNT,\n" +
                    " L_QUANTITY,\n" +
                    " L_SHIPMODE,\n" +
                    " L_SHIPINSTRUCT\n" +
                    " ) physical splitkeys location '%s' hfile location '%s'", getResource("l_part_idx.txt"), getResource("data"))).execute();

            spliceClassWatcher.prepareStatement(format("create index L_SHIPDATE_IDX2 on LINEITEM2(\n" +
                    " L_SHIPDATE,\n" +
                    " L_PARTKEY,\n" +
                    " L_EXTENDEDPRICE,\n" +
                    " L_DISCOUNT\n" +
                    " ) splitkeys location '%s' columnDelimiter '|'", getResource("shipDateIndex.csv"))).execute();

            spliceClassWatcher.prepareStatement(format(" create index L_PART_IDX2 on LINEITEM2(\n" +
                    " L_PARTKEY,\n" +
                    " L_ORDERKEY,\n" +
                    " L_SUPPKEY,\n" +
                    " L_SHIPDATE,\n" +
                    " L_EXTENDEDPRICE,\n" +
                    " L_DISCOUNT,\n" +
                    " L_QUANTITY,\n" +
                    " L_SHIPMODE,\n" +
                    " L_SHIPINSTRUCT\n" +
                    " ) physical splitkeys location '%s'", getResource("l_part_idx.txt"))).execute();

            spliceClassWatcher.prepareStatement(format(" create index L_PART_IDX3 on LINEITEM2(\n" +
                    " L_PARTKEY,\n" +
                    " L_ORDERKEY,\n" +
                    " L_SUPPKEY,\n" +
                    " L_SHIPDATE,\n" +
                    " L_EXTENDEDPRICE,\n" +
                    " L_DISCOUNT,\n" +
                    " L_SHIPINSTRUCT\n" +
                    " ) auto splitkeys sample fraction 0.1")).execute();

            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s', false)", SCHEMA_NAME)).execute();
            spliceClassWatcher.prepareStatement(format("create table A(c varchar(200))"));
            // validate
            assertEquals(9958L, (long) spliceClassWatcher.query("select count(*) from " + LINEITEM));
            assertEquals(2500L, (long) spliceClassWatcher.query("select count(*) from " + ORDERS));
            assertEquals(250L, (long) spliceClassWatcher.query("select count(*) from " + CUSTOMERS));
            assertEquals(1332L, (long) spliceClassWatcher.query("select count(*) from " + PARTSUPP));
            assertEquals(16L, (long) spliceClassWatcher.query("select count(*) from " + SUPPLIER));
            assertEquals(333L, (long) spliceClassWatcher.query("select count(*) from " + PART));
            assertEquals(25L, (long) spliceClassWatcher.query("select count(*) from " + NATION));
            assertEquals(5L, (long) spliceClassWatcher.query("select count(*) from " + REGION));
            assertEquals(9958L, (long) spliceClassWatcher.query("select count(*) from lineitem2 --splice-properties index=L_PART_IDX2"));
            assertEquals(9958L, (long) spliceClassWatcher.query("select count(*) from lineitem2 --splice-properties index=L_SHIPDATE_IDX2"));
            spliceClassWatcher.execute("create table t(COL1 varchar(1000),COL2 varchar(1000), primary key(col1))");
            String sql = String.format("call syscs_util.SYSCS_SPLIT_TABLE_OR_INDEX('%s', 'T', null, 'COL1', '%s', null,null,null,null,null,0,'/BAD',true,null)",
                    SCHEMA_NAME, SpliceUnitTest.getResourceDirectory()+"largeKey.csv");
            spliceClassWatcher.execute(sql);
            TestConnection conn = spliceClassWatcher.getOrCreateConnection();
            new TableCreator(conn)
                    .withCreate("create table b(i int)")
                    .withInsert("insert into b values(?)")
                    .withRows(rows(
                            row(1),
                            row(1)))
                    .create();
        }
        catch (Exception e) {
            java.lang.Throwable ex = Throwables.getRootCause(e);
            if (ex.getMessage().contains("bulk load not supported")) {
                notSupported = true;
            }
            else
                throw e;
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        testBulkDelete();
        String dir = getResource("data");
        FileUtils.deleteDirectory(new File(dir));
    }

    private static void testBulkDelete() throws Exception {
        if (notSupported)
            return;
        bulkDelete(LINEITEM);
        bulkDelete(LINEITEM2);
        bulkDelete(ORDERS);
        bulkDelete(CUSTOMERS);
        bulkDelete(PART);
        bulkDelete(PARTSUPP);
        bulkDelete(NATION);
        bulkDelete(REGION);
        bulkDelete(SUPPLIER);
        countUsingIndex(LINEITEM, "L_PART_IDX");
        countUsingIndex(LINEITEM, "L_SHIPDATE_IDX");
        countUsingIndex(ORDERS, "O_CUST_IDX");
        countUsingIndex(ORDERS, "O_DATE_PRI_KEY_IDX");
        countUsingIndex(LINEITEM2, "L_PART_IDX2");
        countUsingIndex(LINEITEM2, "L_PART_IDX3");
        countUsingIndex(LINEITEM2, "L_SHIPDATE_IDX2");
    }

    private static void bulkDelete(String tableName) throws Exception {
        String sql = String.format("delete from %s --splice-properties bulkDeleteDirectory='%s'", tableName, getResource("data"));
        spliceClassWatcher.execute(sql);
        ResultSet rs = spliceClassWatcher.executeQuery(String.format("select count(*) from %s", tableName));
        rs.next();
        int count = rs.getInt(1);
        Assert.assertTrue(count==0);
    }

    private static void countUsingIndex(String tableName, String indexName) throws Exception {
        ResultSet rs = spliceClassWatcher.executeQuery(
                String.format("select count(*) from %s --splice-properties index=%s", tableName, indexName));
        rs.next();
        int count = rs.getInt(1);
        Assert.assertTrue(count==0);
    }
    @Test
    public void sql1() throws Exception {
        if (notSupported)
            return;
        executeQuery(getContent("1.sql"), getContent("1.expected.txt"), true);
    }

    @Test
    public void sql2() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("2.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql3() throws Exception {
        if (notSupported)
            return;
        executeQuery(getContent("3.sql"), "", true);
    }

    @Test
    public void sql4() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("4.sql");
        executeQuery(sql, getContent("4.expected.txt"), true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql5() throws Exception {
        if (notSupported)
            return;
        executeQuery(getContent("5.sql"), "", true);
    }

    @Test
    public void sql6() throws Exception {
        if (notSupported)
            return;
        executeQuery(getContent("6.sql"), getContent("6.expected.txt"), false);
    }

    @Test
    public void sql7() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("7.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql8() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("8.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql8InvalidMergeJoin() throws Exception {
        if (notSupported)
            return;
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
        if (notSupported)
            return;
        String sql = getContent("9.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql9Repeated() throws Exception {
        if (notSupported)
            return;
        for (int i = 0; i < 3; i++) {
            sql9();
        }
    }

    @Test
    public void sql10() throws Exception {
        if (notSupported)
            return;
        executeQuery(getContent("10.sql"), "", true);
    }

    @Test
    public void sql11() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("11.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ONE_SUBQUERY_NODE);
    }

    @Test
    public void sql12() throws Exception {
        if (notSupported)
            return;
        executeQuery(getContent("12.sql"), getContent("12.expected.txt"), true);
    }

    @Test
    public void sql13() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("13.sql");
        executeQuery(sql, getContent("13.expected.txt"), true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql14() throws Exception {
        if (notSupported)
            return;
        executeQuery(getContent("14.sql"), getContent("14.expected.txt"), false);
    }

    @Test
    public void sql15() throws Exception {
        if (notSupported)
            return;
        String sql15a = getContent("15a.sql");
        String sql15b = getContent("15b.sql");

        executeUpdate(sql15a);
        executeQuery(sql15b, "", false);

        assertSubqueryNodeCount(conn(), sql15b, ONE_SUBQUERY_NODE);
    }

    @Test
    public void sql16() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("16.sql");
        executeQuery(sql, getContent("16.expected.txt"), true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql17() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("17.sql");
        executeQuery(sql, getContent("17.expected.txt"), false);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test(timeout = 30000)
    public void sql18() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("18.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql19() throws Exception {
        if (notSupported)
            return;
        executeQuery(getContent("19.sql"), getContent("19.expected.txt"), false);
    }

    @Test
    public void sql20() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("20.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql21() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("21.sql");
        executeQuery(sql, "", true);
        assertSubqueryNodeCount(conn(), sql, ZERO_SUBQUERY_NODES);
    }

    @Test
    public void sql22() throws Exception {
        if (notSupported)
            return;
        String sql = getContent("22.sql");
        executeQuery(sql, getContent("22.expected.txt"), true);
        assertSubqueryNodeCount(conn(), sql, ONE_SUBQUERY_NODE);
    }

    @Test
    public void testPredicatePushdownOnRightSideOfJoin() throws Exception {
        if (notSupported)
            return;
        rowContainsQuery(7,"explain select count(*) from --splice-properties joinOrder=fixed\n" +
                " ORDERS, LINEITEM --splice-properties joinStrategy=BROADCAST\n" +
                " where l_orderkey = o_orderkey and l_shipdate > date('1995-03-15') and o_orderdate > date('1995-03-15')","preds=[(L_SHIPDATE[2:2] > 1995-03-15)]",methodWatcher);
    }

    @Test
    public void testLargeKeys() throws Exception {
        if (notSupported)
            return;

        String bulkImport = String.format("call syscs_util.bulk_import_hfile('%s','T','COL1', '%s', null,null,null,null,null,-1,'/BAD',true,null, '%s', true)",
                SCHEMA_NAME, SpliceUnitTest.getResourceDirectory()+"largeKey.csv", SpliceUnitTest.getResourceDirectory());
        spliceClassWatcher.execute(bulkImport);

        String createIndex = String.format("create index ti on t(col2) SPLITKEYS LOCATION '%s' HFILE LOCATION '%s'",
                SpliceUnitTest.getResourceDirectory()+"largeKey.csv", SpliceUnitTest.getResourceDirectory());
        spliceClassWatcher.execute(createIndex);

        ResultSet rs = spliceClassWatcher.executeQuery("select count(*) from t --splice-properties index=null");
        rs.next();
        int count = rs.getInt(1);
        Assert.assertEquals(1, count);

        rs = spliceClassWatcher.executeQuery("select count(*) from t --splice-properties index=ti");
        rs.next();
        count = rs.getInt(1);
        Assert.assertEquals(1, count);

    }

    @Test
    public void verifySplitKeys() throws Exception {
        if (notSupported)
            return;
        ResultSet rs = methodWatcher.executeQuery("call syscs_util.get_regions('HBASEBULKLOADINDEXIT', 'LINEITEM', " +
                "'L_SHIPDATE_IDX',null, null,null,null,null,null,null)");
        String[] startKeys = {"{ NULL, NULL, NULL, NULL }", "{ 1993-05-30, 58476, 5737.88, 0.04 }",
        "{ 1995-02-01, 133234, 24077.37, 0.03 }", "{ 1996-04-20, NULL, 48881, 0.06 }", "{ 1996-04-20, 94751, 48881, 0.06 }"};
        int i = 0;
        while(rs.next()) {
            String k = rs.getString(2);
            Assert.assertEquals(k, startKeys[i++]);
        }

    }

    @Test
    public void testUniqueConstraintsViolation() throws Exception {
        if (notSupported)
            return;
        try {
            methodWatcher.execute(String.format("create unique index bi on b(i) auto splitkeys hfile location '%s'", getResource("data")));
            fail();
        }
        catch (Exception e) {
            String errorMessage = e.getMessage();
            String expected = "The statement was aborted because it would have caused a duplicate key value in a unique or primary key constraint or unique index identified by 'BI' defined on 'B'.";
            Assert.assertEquals(expected, errorMessage);
        }
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


    private static String getResource(String name) {
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
