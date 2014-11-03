package com.splicemachine.derby.test;

import com.google.common.io.Closeables;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SlowTest;
import org.apache.derby.tools.ij;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.format;
import static com.splicemachine.derby.test.framework.SpliceUnitTest.getResourceDirectory;

public class TPCHIT {
    public static final String CLASS_NAME = "TPCH1X";
    protected static final String LINEITEM = "LINEITEM";
    protected static final String ORDERS = "ORDERS";
    protected static final String CUSTOMERS = "CUSTOMER";
    protected static final String PARTSUPP = "PARTSUPP";
    protected static final String SUPPLIER = "SUPPLIER";
    protected static final String PART = "PART";
    protected static final String NATION = "NATION";
    protected static final String REGION = "REGION";

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "tcph/TPCHIT.sql", CLASS_NAME))
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", CLASS_NAME, LINEITEM, getResource("lineitem.tbl")));
                        ps.execute();
                        ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", CLASS_NAME, ORDERS, getResource("orders.tbl")));
                        ps.execute();
                        ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", CLASS_NAME, CUSTOMERS, getResource("customer.tbl")));
                        ps.execute();
                        ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", CLASS_NAME, PARTSUPP, getResource("partsupp.tbl")));
                        ps.execute();
                        ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", CLASS_NAME, SUPPLIER, getResource("supplier.tbl")));
                        ps.execute();
                        ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", CLASS_NAME, PART, getResource("part.tbl")));
                        ps.execute();
                        ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", CLASS_NAME, NATION, getResource("nation.tbl")));
                        ps.execute();
                        ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s',null,null,'%s','|','\"',null,null,null)", CLASS_NAME, REGION, getResource("region.tbl")));
                        ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void validateDataLoad() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s.%s", CLASS_NAME, LINEITEM));
        rs.next();
        Assert.assertEquals(9958, rs.getLong(1));
        rs = methodWatcher.executeQuery(format("select count(*) from %s.%s", CLASS_NAME, ORDERS));
        rs.next();
        Assert.assertEquals(2500, rs.getLong(1));
        rs = methodWatcher.executeQuery(format("select count(*) from %s.%s", CLASS_NAME, CUSTOMERS));
        rs.next();
        Assert.assertEquals(250, rs.getLong(1));
        rs = methodWatcher.executeQuery(format("select count(*) from %s.%s", CLASS_NAME, PARTSUPP));
        rs.next();
        Assert.assertEquals(1332, rs.getLong(1));
        rs = methodWatcher.executeQuery(format("select count(*) from %s.%s", CLASS_NAME, SUPPLIER));
        rs.next();
        Assert.assertEquals(16, rs.getLong(1));
        rs = methodWatcher.executeQuery(format("select count(*) from %s.%s", CLASS_NAME, PART));
        rs.next();
        Assert.assertEquals(333, rs.getLong(1));
        rs = methodWatcher.executeQuery(format("select count(*) from %s.%s", CLASS_NAME, NATION));
        rs.next();
        Assert.assertEquals(25, rs.getLong(1));
        rs = methodWatcher.executeQuery(format("select count(*) from %s.%s", CLASS_NAME, REGION));
        rs.next();
        Assert.assertEquals(5, rs.getLong(1));
    }

    @Test
    public void sql1() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("1.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql2() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("2.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql3() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("3.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql4() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("4.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql5() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("5.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql6() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("6.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql7() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("7.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql8() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("8.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    @Ignore
    public void sql9() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("9.sql")), methodWatcher.getOrCreateConnection()));
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
        Assert.assertTrue(runScript(new File(getSQLFile("10.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql11() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("11.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql12() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("12.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql13() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("13.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql14() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("14.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql15() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("15.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql16() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("16.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    @Ignore
    public void sql17() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("17.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql18() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("18.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql19() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("19.sql")), methodWatcher.getOrCreateConnection()));
    }

    @Test
    public void sql20() throws Exception {
        Assert.assertTrue(runScript(new File(getSQLFile("20.sql")), methodWatcher.getOrCreateConnection()));
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
        methodWatcher.executeQuery(String.format(mergeOverMergeSort, CLASS_NAME, CLASS_NAME, CLASS_NAME, CLASS_NAME));
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
