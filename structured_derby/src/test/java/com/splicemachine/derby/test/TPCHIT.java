package com.splicemachine.derby.test;

import com.google.common.io.Closeables;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SlowTest;
import org.apache.derby.tools.ij;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.format;
import static com.splicemachine.derby.test.framework.SpliceUnitTest.getResourceDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        assertTrue(runScript(new File(getSQLFile("1.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql2() throws Exception {
        assertTrue(runScript(new File(getSQLFile("2.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql3() throws Exception {
        assertTrue(runScript(new File(getSQLFile("3.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql4() throws Exception {
        assertTrue(runScript(new File(getSQLFile("4.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql5() throws Exception {
        assertTrue(runScript(new File(getSQLFile("5.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql6() throws Exception {
        assertTrue(runScript(new File(getSQLFile("6.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql7() throws Exception {
        assertTrue(runScript(new File(getSQLFile("7.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql8() throws Exception {
        assertTrue(runScript(new File(getSQLFile("8.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    @Ignore
    public void sql9() throws Exception {
        assertTrue(runScript(new File(getSQLFile("9.sql")), methodWatcher.getOrCreateConnection()));
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
        assertTrue(runScript(new File(getSQLFile("10.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql11() throws Exception {
        assertTrue(runScript(new File(getSQLFile("11.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql12() throws Exception {
        assertTrue(runScript(new File(getSQLFile("12.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql13() throws Exception {
        assertTrue(runScript(new File(getSQLFile("13.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql14() throws Exception {
        assertTrue(runScript(new File(getSQLFile("14.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql15() throws Exception {
        assertTrue(runScript(new File(getSQLFile("15.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql16() throws Exception {
        assertTrue(runScript(new File(getSQLFile("16.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    @Ignore
    public void sql17() throws Exception {
        assertTrue(runScript(new File(getSQLFile("17.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql18() throws Exception {
        assertTrue(runScript(new File(getSQLFile("18.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql19() throws Exception {
        assertTrue(runScript(new File(getSQLFile("19.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql20() throws Exception {
        assertTrue(runScript(new File(getSQLFile("20.sql")), methodWatcher.getOrCreateConnection()));
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

    public static String getResource(String name) {
        return getResourceDirectory() + "tcph/data/" + name;
    }

    protected static String getSQLFile(String name) {
        return getResourceDirectory() + "tcph/query/" + name;
    }


    protected static boolean runScript(File scriptFile, Connection connection) throws Exception {
        FileInputStream fileStream = null;
        try {
            fileStream = new FileInputStream(scriptFile);
            int result = ij.runScript(connection, fileStream, "UTF-8", System.out, "UTF-8");
            return (result == 0);
        } finally {
            Closeables.closeQuietly(fileStream);
        }
    }

}
