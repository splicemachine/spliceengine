package com.splicemachine.derby.test;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SlowTest;
import org.apache.commons.io.IOUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.FileInputStream;
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
        executeQuery(getSQLFile("1.sql"));
    }

    @Test
    public void sql2() throws Exception {
        executeQuery(getSQLFile("2.sql"));
    }

    @Test
    public void sql3() throws Exception {
        executeQuery(getSQLFile("3.sql"));
    }

    @Test
    public void sql4() throws Exception {
        executeQuery(getSQLFile("4.sql"));
    }

    @Test
    public void sql5() throws Exception {
        executeQuery(getSQLFile("5.sql"));
    }

    @Test
    public void sql6() throws Exception {
        executeQuery(getSQLFile("6.sql"));
    }

    @Test
    public void sql7() throws Exception {
        executeQuery(getSQLFile("7.sql"));
    }

    @Test
    public void sql8() throws Exception {
        executeQuery(getSQLFile("8.sql"));
    }

    @Test
    @Ignore
    public void sql9() throws Exception {
        executeQuery(getSQLFile("9.sql"));
    }

    @Test
    @Ignore
    @Category(SlowTest.class)
    public void testRepeatedSql9() throws Exception {
        for (int i = 0; i < 100; i++) {
            sql9();
            System.out.printf("Iteration %d succeeded%n", i);
        }
    }

    @Test
    public void sql10() throws Exception {
        executeQuery(getSQLFile("10.sql"));
    }

    @Test
    public void sql11() throws Exception {
        executeQuery(getSQLFile("11.sql"));
    }

    @Test
    public void sql12() throws Exception {
        executeQuery(getSQLFile("12.sql"));
    }

    @Test
    public void sql13() throws Exception {
        executeQuery(getSQLFile("13.sql"));
    }

    @Test
    public void sql14() throws Exception {
        executeQuery(getSQLFile("14.sql"));
    }

    @Test
    public void sql15() throws Exception {
        executeUpdate(getSQLFile("15a.sql"));
        executeQuery(getSQLFile("15b.sql"));
    }

    @Test
    public void sql16() throws Exception {
        executeQuery(getSQLFile("16.sql"));
    }

    @Test
    @Ignore
    public void sql17() throws Exception {
        executeQuery(getSQLFile("17.sql"));
    }

    @Test
    public void sql18() throws Exception {
        executeQuery(getSQLFile("18.sql"));
    }

    @Test
    public void sql19() throws Exception {
        executeQuery(getSQLFile("19.sql"));
    }

    @Test
    public void sql20() throws Exception {
        executeQuery(getSQLFile("20.sql"));
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

    private static String getSQLFile(String name) {
        return getResourceDirectory() + "tcph/query/" + name;
    }

    private void executeQuery(String queryFileName) throws Exception {
        String query = IOUtils.toString(new FileInputStream(new File(queryFileName)));
        methodWatcher.executeQuery(query);
    }

    private void executeUpdate(String queryFileName) throws Exception {
        String query = IOUtils.toString(new FileInputStream(new File(queryFileName)));
        methodWatcher.executeUpdate(query);
    }
}
