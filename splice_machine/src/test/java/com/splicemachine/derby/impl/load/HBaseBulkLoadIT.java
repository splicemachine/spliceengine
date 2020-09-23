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


package com.splicemachine.derby.impl.load;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import splice.com.google.common.base.Throwables;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.subquery.SubqueryITUtil.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by jyuan on 3/27/17.
 */
public class HBaseBulkLoadIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = HBaseBulkLoadIT.class.getSimpleName().toUpperCase();
    private static final String LINEITEM = "LINEITEM";
    private static final String LINEITEM4 = "LINEITEM4";
    private static final String ORDERS = "ORDERS";
    private static final String CUSTOMERS = "CUSTOMER";
    private static final String PARTSUPP = "PARTSUPP";
    private static final String SUPPLIER = "SUPPLIER";
    private static final String PART = "PART";
    private static final String NATION = "NATION";
    private static final String REGION = "REGION";
    private static final String ROWS_COUNT_WITH_SAMPLE = "ROWS_COUNT_WITH_SAMPLE";
    private static final String ROWS_COUNT_WITHOUT_SAMPLE = "ROWS_COUNT_WITHOUT_SAMPLE";
    private static boolean notSupported;
    private static String BADDIR;
    private static String BULKLOADDIR;

    public static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);

    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

    public static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(ROWS_COUNT_WITH_SAMPLE,
            spliceSchemaWatcher.schemaName,
            "(i varchar(10), j varchar(10) not null, constraint a_pk1 primary key (i))");

    public static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(ROWS_COUNT_WITHOUT_SAMPLE,
            spliceSchemaWatcher.schemaName,
            "(i varchar(10), j varchar(10) not null, constraint a_pk2 primary key (i))");
    private static String startKeys[] = {
            "{ NULL, NULL }",
            "{ 1424004, 7 }",
            "{ 2384419, 4 }",
            "{ 3244416, 6 }",
            "{ 5295747, 4 }"};

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @BeforeClass
    public static void loaddata() throws Exception {
        try {
            BADDIR = SpliceUnitTest.createBadLogDirectory(SCHEMA_NAME).getCanonicalPath();
            BULKLOADDIR = SpliceUnitTest.createBulkLoadDirectory(SCHEMA_NAME).getCanonicalPath();

            TestUtils.executeSqlFile(spliceClassWatcher, "tcph/TPCHIT.sql", SCHEMA_NAME);
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, LINEITEM, getResource("lineitem.tbl"), BULKLOADDIR)).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, ORDERS, getResource("orders.tbl"), BULKLOADDIR)).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, CUSTOMERS, getResource("customer.tbl"), BULKLOADDIR)).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, PARTSUPP, getResource("partsupp.tbl"), BULKLOADDIR)).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, SUPPLIER, getResource("supplier.tbl"), BULKLOADDIR)).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, PART, getResource("part.tbl"), BULKLOADDIR)).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, NATION, getResource("nation.tbl"), BULKLOADDIR)).execute();
            spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,0,null,true,null, '%s', false)", SCHEMA_NAME, REGION, getResource("region.tbl"), BULKLOADDIR)).execute();

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
            spliceClassWatcher.prepareStatement("CREATE TABLE LINEITEM4 (\n" +
                    "  L_ORDERKEY      BIGINT NOT NULL,\n" +
                    "  L_PARTKEY       INTEGER NOT NULL,\n" +
                    "  L_SUPPKEY       INTEGER NOT NULL,\n" +
                    "  L_LINENUMBER    INTEGER NOT NULL,\n" +
                    "  L_QUANTITY      DECIMAL(15, 2),\n" +
                    "  L_EXTENDEDPRICE DECIMAL(15, 2),\n" +
                    "  L_DISCOUNT      DECIMAL(15, 2),\n" +
                    "  L_TAX           DECIMAL(15, 2),\n" +
                    "  L_RETURNFLAG    CHAR(1),\n" +
                    "  L_LINESTATUS    CHAR(1),\n" +
                    "  L_SHIPDATE      DATE,\n" +
                    "  L_COMMITDATE    DATE,\n" +
                    "  L_RECEIPTDATE   DATE,\n" +
                    "  L_SHIPINSTRUCT  CHAR(25),\n" +
                    "  L_SHIPMODE      CHAR(10),\n" +
                    "  L_COMMENT       VARCHAR(44),\n" +
                    "  PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)\n" +
                    ")").execute();
        }
        catch (Exception e) {
            java.lang.Throwable ex = Throwables.getRootCause(e);
             if (ex.getMessage().contains("bulk load not supported"))
                 notSupported = true;
            else
                 throw e;
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        testBulkDelete();
        FileUtils.deleteDirectory(new File(BULKLOADDIR));
    }

    private static void testBulkDelete() throws Exception {
        if (notSupported)
            return;
        bulkDelete(LINEITEM, "L_PART_IDX");
        bulkDelete(ORDERS, "O_CUST_IDX");
        bulkDelete(CUSTOMERS, null);
        bulkDelete(PART,null);
        bulkDelete(PARTSUPP,null);
        bulkDelete(NATION,null);
        bulkDelete(REGION,null);
        bulkDelete(SUPPLIER,null);
        countUsingIndex(LINEITEM, "L_PART_IDX");
        countUsingIndex(LINEITEM, "L_SHIPDATE_IDX");
        countUsingIndex(ORDERS, "O_CUST_IDX");
        countUsingIndex(ORDERS, "O_DATE_PRI_KEY_IDX");
    }

    private static void bulkDelete(String tableName, String indexName) throws Exception {
        String sql = String.format("delete from %s --splice-properties bulkDeleteDirectory='%s',index=%s", tableName, BULKLOADDIR,
                indexName!=null?indexName:"null");
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
    public void testRowsCountWithSample() throws Exception {
        if (notSupported)
            return;
        ResultSet result = spliceClassWatcher.prepareStatement(format(
                "call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s',null,null,null,null,null,-1,'%s',true,null, '%s', false)",
                SCHEMA_NAME, ROWS_COUNT_WITH_SAMPLE, getResourceDirectory() + "rows_count.csv", BADDIR, BULKLOADDIR))
                .executeQuery();
        result.next();
        int rowsCount = result.getInt(1);
        int badRecords = result.getInt(2);
        assertEquals(rowsCount, 1);
        assertEquals(badRecords, 2);
    }

    @Test
    public void testRowsCountWithoutSample() throws Exception {
        if (notSupported)
            return;
        ResultSet result = spliceClassWatcher.prepareStatement(format(
                "call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s',null,null,null,null,null,-1,'%s',true,null, '%s', true)",
                SCHEMA_NAME, ROWS_COUNT_WITHOUT_SAMPLE, getResourceDirectory() + "rows_count.csv", BADDIR, BULKLOADDIR))
                .executeQuery();
        result.next();
        int rowsCount = result.getInt(1);
        int badRecords = result.getInt(2);
        assertEquals(rowsCount, 1);
        assertEquals(badRecords, 2);
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
    public void testComputeTableSplitKeys() throws Exception {
        if (notSupported)
            return;
       String sql =
               " select conglomeratenumber from sys.systables t, sys.sysconglomerates c, sys.sysschemas s " +
               "where c.tableid=t.tableid and t.tablename='LINEITEM' and s.schemaid=c.schemaid and s.schemaname='%s' order by 1";

        ResultSet rs = methodWatcher.executeQuery(format(sql, SCHEMA_NAME));
        rs.next();
        long conglomId = rs.getLong(1);
        methodWatcher.execute(format("call SYSCS_UTIL.COMPUTE_SPLIT_KEY('%s','%s',null,'L_ORDERKEY,L_LINENUMBER'," +
                        "'%s','|',null,null,null,null,-1,'%s',true,null,'%s')",SCHEMA_NAME,LINEITEM,
                getResource("lineitemKey.csv"), BADDIR, BULKLOADDIR));
        rs.close();

        String select =
                "SELECT \"KEY\" " +
                "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS splitKey (\"KEY\" varchar(200))";
        rs = methodWatcher.executeQuery(format(select, BULKLOADDIR + "/" + conglomId + "/keys"));
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        String expected =
                "KEY     |\n" +
                "--------------\n" +
                "  \\x81\\x00   |\n" +
                "\\x81\\x00\\x82 |\n" +
                "\\x81\\x00\\x84 |\n" +
                "\\x81\\x00\\x86 |\n" +
                "\\x81\\x00\\x88 |\n" +
                "  \\x82\\x00   |\n" +
                "\\x82\\x00\\x82 |";


        Assert.assertEquals(expected, s);
    }

    @Test
    public void testComputeIndexSplitKeys() throws Exception {
        if (notSupported)
            return;
        String sql =
                " select conglomeratenumber from sys.systables t, sys.sysconglomerates c, sys.sysschemas s " +
                        "where c.tableid=t.tableid and t.tablename='ORDERS' and s.schemaid=c.schemaid and " +
                        "s.schemaname='%s' and conglomeratename='O_CUST_IDX' order by 1";

        ResultSet rs = methodWatcher.executeQuery(format(sql, SCHEMA_NAME));
        rs.next();
        long conglomId = rs.getLong(1);
        methodWatcher.execute(format("call SYSCS_UTIL.COMPUTE_SPLIT_KEY('%s','%s','O_CUST_IDX'," +
                        "'O_CUSTKEY,O_ORDERKEY'," +
                        "'%s','|',null,null,null,null,-1,'%s',true,null,'%s')",SCHEMA_NAME,ORDERS,
                getResource("custIndex.csv"), BADDIR, BULKLOADDIR));
        rs.close();

        String select =
                "SELECT \"KEY\" " +
                        "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS splitKey (\"KEY\" varchar(200))";
        rs = methodWatcher.executeQuery(format(select, BULKLOADDIR + "/" + conglomId + "/keys"));
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        String expected ="KEY           |\n" +
                "-------------------------\n" +
                "\\xE1(Q\\x00\\xE4<\\x8F\\x84 |";

        Assert.assertEquals(expected, s);
    }
    @Test
    public void testCreateTableWithLogicalSplitKeyes() throws Exception {
        if (notSupported)
            return;
        String sql = String.format("CREATE TABLE LINEITEM2 (\n" +
                "  L_ORDERKEY      INTEGER NOT NULL,\n" +
                "  L_PARTKEY       INTEGER NOT NULL,\n" +
                "  L_SUPPKEY       INTEGER NOT NULL,\n" +
                "  L_LINENUMBER    INTEGER NOT NULL,\n" +
                "  L_QUANTITY      DECIMAL(15, 2),\n" +
                "  L_EXTENDEDPRICE DECIMAL(15, 2),\n" +
                "  L_DISCOUNT      DECIMAL(15, 2),\n" +
                "  L_TAX           DECIMAL(15, 2),\n" +
                "  L_RETURNFLAG    CHAR(1),\n" +
                "  L_LINESTATUS    CHAR(1),\n" +
                "  L_SHIPDATE      DATE,\n" +
                "  L_COMMITDATE    DATE,\n" +
                "  L_RECEIPTDATE   DATE,\n" +
                "  L_SHIPINSTRUCT  CHAR(25),\n" +
                "  L_SHIPMODE      CHAR(10),\n" +
                "  L_COMMENT       VARCHAR(44),\n" +
                "  PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)\n" +
                ") logical splitkeys location '%s'", SpliceUnitTest.getResourceDirectory()+ "lineitemKeys.csv");
        methodWatcher.execute(sql);
        ResultSet rs = methodWatcher.executeQuery(String.format("call syscs_util.get_regions('%s','LINEITEM2',null,null,null,null,null,null,null,null)", SCHEMA_NAME));
        int i = 0;
        while (rs.next()) {
            String startKey = rs.getString("SPLICE_START_KEY");
            Assert.assertEquals(startKey, startKey, startKeys[i]);
            i++;
        }

    }

    @Test
    public void testCreateTableWithPhysicalSplitKeyes() throws Exception {
        if (notSupported)
            return;
        String sql = String.format("CREATE TABLE LINEITEM3 (\n" +
                "  L_ORDERKEY      INTEGER NOT NULL,\n" +
                "  L_PARTKEY       INTEGER NOT NULL,\n" +
                "  L_SUPPKEY       INTEGER NOT NULL,\n" +
                "  L_LINENUMBER    INTEGER NOT NULL,\n" +
                "  L_QUANTITY      DECIMAL(15, 2),\n" +
                "  L_EXTENDEDPRICE DECIMAL(15, 2),\n" +
                "  L_DISCOUNT      DECIMAL(15, 2),\n" +
                "  L_TAX           DECIMAL(15, 2),\n" +
                "  L_RETURNFLAG    CHAR(1),\n" +
                "  L_LINESTATUS    CHAR(1),\n" +
                "  L_SHIPDATE      DATE,\n" +
                "  L_COMMITDATE    DATE,\n" +
                "  L_RECEIPTDATE   DATE,\n" +
                "  L_SHIPINSTRUCT  CHAR(25),\n" +
                "  L_SHIPMODE      CHAR(10),\n" +
                "  L_COMMENT       VARCHAR(44),\n" +
                "  PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)\n" +
                ") physical splitkeys location '%s'", SpliceUnitTest.getResourceDirectory()+ "lineitemKeys.txt");
        methodWatcher.execute(sql);
        ResultSet rs = methodWatcher.executeQuery(String.format("call syscs_util.get_regions('%s','LINEITEM3',null,null,null,null,null,null,null,null)", SCHEMA_NAME));
        int i = 0;
        while (rs.next()) {
            String startKey = rs.getString("SPLICE_START_KEY");
            Assert.assertEquals(startKey, startKey, startKeys[i]);
            i++;
        }

    }

    @Test
    public void negativeTests() throws Exception {
        if (notSupported)
            return;

        try {
            methodWatcher.execute("call SYSCS_UTIL.SYSCS_SPLIT_TABLE_OR_INDEX_AT_POINTS(null,null,null,null)");
        }catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.TABLE_NAME_CANNOT_BE_NULL.substring(0,5), sqlcode);
        }

        try {
            methodWatcher.execute("call SYSCS_UTIL.SYSCS_SPLIT_TABLE_OR_INDEX(null,null,null, 'L_ORDERKEY,L_LINENUMBER','hdfs:///tmp/test_hfile_import/lineitemKey.csv','|', null, null, null,null, -1, null, true, null)");
        } catch (SQLException e) {
            String sqlcode = e.getSQLState();
            Assert.assertEquals(sqlcode, SQLState.TABLE_NAME_CANNOT_BE_NULL.substring(0,5), sqlcode);
        }

    }

    @Test
    public void testDuplicateKeys() throws Exception {
        if (notSupported)
            return;

        methodWatcher.prepareStatement(format("call SYSCS_UTIL.BULK_IMPORT_HFILE('%s','%s',null,'%s','|','\"',null,null,null,-1, '%s',true,null, '%s', false)", SCHEMA_NAME, LINEITEM4, getResource("lineitem_dup.tbl"), BADDIR, BULKLOADDIR)).execute();
        File f = new File(BADDIR);
        File[] files = f.listFiles();
        for (File file:files) {
            if (file.getName().startsWith("lineitem_dup")) {
                String select =
                        "SELECT \"message\" " +
                                "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                                "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                                "AS errormessage (\"message\" varchar(400)) order by 1";
                ResultSet rs = methodWatcher.executeQuery(format(select, file.getAbsolutePath()));
                String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
                String expected = "message                                                                                                |\n" +
                        "--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                        "     Unique constraint violation, row: { 1, 155190, 7706, 1, 17, 21168.23, 0.04, 0.02, N, O, 1996-03-13, 1996-02-12, 1996-03-22, DELIVER IN PERSON        , TRUCK     , egular courts above the }      |\n" +
                        "    Unique constraint violation, row: { 1, 63700, 3701, 3, 8, 13309.6, 0.1, 0.02, N, O, 1996-01-29, 1996-03-05, 1996-01-31, TAKE BACK RETURN         , REG AIR   , riously. regular, express dep }     |\n" +
                        "Unique constraint violation, row: { 1, 67310, 7311, 2, 36, 45983.16, 0.09, 0.06, N, O, 1996-04-12, 1996-02-28, 1996-04-20, TAKE BACK RETURN         , MAIL      , ly final dependencies: slyly bold  } |";
                Assert.assertEquals(s, expected, s);
            }
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
