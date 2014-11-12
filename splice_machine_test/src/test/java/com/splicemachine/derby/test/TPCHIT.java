package com.splicemachine.derby.test;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.format;
import static com.splicemachine.derby.test.framework.SpliceUnitTest.getResourceDirectory;
import static org.junit.Assert.assertEquals;

public class TPCHIT {

    private static final String SCHEMA_NAME = "TPCH1X";
    private static final String LINEITEM = "LINEITEM";
    private static final String ORDERS = "ORDERS";
    private static final String CUSTOMERS = "CUSTOMER";
    private static final String PARTSUPP = "PARTSUPP";
    private static final String SUPPLIER = "SUPPLIER";
    private static final String PART = "PART";
    private static final String NATION = "NATION";
    private static final String REGION = "REGION";

    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(new SpliceSchemaWatcher(SCHEMA_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "tcph/TPCHIT.sql", SCHEMA_NAME));

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @BeforeClass
    public static void loadData() throws Exception {
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", SCHEMA_NAME, LINEITEM, getResource("lineitem.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", SCHEMA_NAME, ORDERS, getResource("orders.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", SCHEMA_NAME, CUSTOMERS, getResource("customer.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", SCHEMA_NAME, PARTSUPP, getResource("partsupp.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", SCHEMA_NAME, SUPPLIER, getResource("supplier.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", SCHEMA_NAME, PART, getResource("part.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", SCHEMA_NAME, NATION, getResource("nation.tbl"))).execute();
        spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", SCHEMA_NAME, REGION, getResource("region.tbl"))).execute();
    }

    @Test
    public void validateDataLoad() throws Exception {
        assertEquals(9958L, methodWatcher.query(format("select count(*) from %s.%s", SCHEMA_NAME, LINEITEM)));
        assertEquals(2500L, methodWatcher.query(format("select count(*) from %s.%s", SCHEMA_NAME, ORDERS)));
        assertEquals(250L, methodWatcher.query(format("select count(*) from %s.%s", SCHEMA_NAME, CUSTOMERS)));
        assertEquals(1332L, methodWatcher.query(format("select count(*) from %s.%s", SCHEMA_NAME, PARTSUPP)));
        assertEquals(16L, methodWatcher.query(format("select count(*) from %s.%s", SCHEMA_NAME, SUPPLIER)));
        assertEquals(333L, methodWatcher.query(format("select count(*) from %s.%s", SCHEMA_NAME, PART)));
        assertEquals(25L, methodWatcher.query(format("select count(*) from %s.%s", SCHEMA_NAME, NATION)));
        assertEquals(5L, methodWatcher.query(format("select count(*) from %s.%s", SCHEMA_NAME, REGION)));
    }

    @Test
    public void sql1() throws Exception {
        executeQuery(getContent("1.sql"), getContent("1.expected.txt"), true);
    }

    @Test
    public void sql2() throws Exception {
        executeQuery(getContent("2.sql"), "", true);
    }

    @Test
    public void sql3() throws Exception {
        executeQuery(getContent("3.sql"), "", true);
    }

    @Test
    public void sql4() throws Exception {
        executeQuery(getContent("4.sql"), getContent("4.expected.txt"), true);
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
        executeQuery(getContent("7.sql"), "", true);
    }

    @Test
    public void sql8() throws Exception {
        executeQuery(getContent("8.sql"), "", true);
    }

    @Test
    public void sql9() throws Exception {
        executeQuery(getContent("9.sql"), "", true);
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
        executeQuery(getContent("11.sql"), "", true);
    }

    @Test
    public void sql12() throws Exception {
        executeQuery(getContent("12.sql"), getContent("12.expected.txt"), true);
    }

    @Test
    public void sql13() throws Exception {
        executeQuery(getContent("13.sql"), getContent("13.expected.txt"), true);
    }

    @Test
    public void sql14() throws Exception {
        executeQuery(getContent("14.sql"), getContent("14.expected.txt"), false);
    }

    @Test
    public void sql15() throws Exception {
        executeUpdate(getContent("15a.sql"));
        executeQuery(getContent("15b.sql"), "", false);
    }

    @Test
    public void sql16() throws Exception {
        executeQuery(getContent("16.sql"), getContent("16.expected.txt"), true);
    }

    @Test
    public void sql17() throws Exception {
        executeQuery(getContent("17.sql"), getContent("17.expected.txt"), false);
    }

    @Test
    public void sql18() throws Exception {
        executeQuery(getContent("18.sql"), "", true);
    }

    @Test
    public void sql19() throws Exception {
        executeQuery(getContent("19.sql"), getContent("19.expected.txt"), false);
    }

    @Test
    public void sql20() throws Exception {
        executeQuery(getContent("20.sql"), "", true);
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
        methodWatcher.executeQuery(String.format(mergeOverMergeSort, SCHEMA_NAME, SCHEMA_NAME, SCHEMA_NAME, SCHEMA_NAME));
    }

    private static String getResource(String name) {
        return getResourceDirectory() + "tcph/data/" + name;
    }

    private static String getContent(String fileName) throws IOException {
        String fullFileName = getResourceDirectory() + "tcph/query/" + fileName;
        return IOUtils.toString(new FileInputStream(new File(fullFileName)));
    }

    private void executeQuery(String query, String expected, boolean isResultSetOrdered) throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(query);

        // If the ResultSet is NOT ordered (no order by clause in query) then sort it before comparing to expected result.
        // When we don't sort we are assuming the order by clause gives the ResultSet a unique order-- seems to be
        // the case for this data set (no duplicates in result set order by columns).
        boolean sort = !isResultSetOrdered;

        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.convert("", resultSet, sort).toString().trim());
    }

    private void executeUpdate(String query) throws Exception {
        methodWatcher.executeUpdate(query);
    }
}
