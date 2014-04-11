package org.apache.derby.impl.sql.execute.operations.joins;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.derby.test.TPCHIT;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.splicemachine.homeless.TestUtils.o;

/**
 * @author P Trolard
 *         Date: 26/11/2013
 */
public class MergeJoinIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(MergeJoinIT.class);

    public static final String CLASS_NAME = MergeJoinIT.class.getSimpleName();

    protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    protected static final String LINEITEM = "LINEITEM";
    protected static final String ORDERS = "ORDERS";

    protected static SpliceTableWatcher lineItemTable = new SpliceTableWatcher(LINEITEM, CLASS_NAME,
            "( L_ORDERKEY INTEGER NOT NULL,L_PARTKEY INTEGER NOT NULL, L_SUPPKEY INTEGER NOT NULL, " +
                    "L_LINENUMBER  INTEGER NOT NULL, L_QUANTITY DECIMAL(15,2), L_EXTENDEDPRICE DECIMAL(15,2)," +
                    "L_DISCOUNT DECIMAL(15,2), L_TAX DECIMAL(15,2), L_RETURNFLAG  CHAR(1), L_LINESTATUS CHAR(1), " +
                    "L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT CHAR(25)," +
                    "L_SHIPMODE CHAR(10),L_COMMENT VARCHAR(44),PRIMARY KEY(L_ORDERKEY,L_LINENUMBER))");
    protected static SpliceTableWatcher orderTable = new SpliceTableWatcher(ORDERS, CLASS_NAME,
            "( O_ORDERKEY INTEGER NOT NULL PRIMARY KEY,O_CUSTKEY INTEGER,O_ORDERSTATUS CHAR(1)," +
                    "O_TOTALPRICE DECIMAL(15,2),O_ORDERDATE DATE, O_ORDERPRIORITY  CHAR(15), " +
                    "O_CLERK CHAR(15), O_SHIPPRIORITY INTEGER, O_COMMENT VARCHAR(79))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceSchemaWatcher)
            .around(spliceClassWatcher)
            .around(lineItemTable)
            .around(orderTable)
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/employee.sql", CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/basic_join_dataset.sql", CLASS_NAME))
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(
                                format("call SYSCS_UTIL.SYSCS_IMPORT_DATA(" +
                                        "'%s','%s',null,null,'%s','|','\"',null,null,null)",
                                        CLASS_NAME, LINEITEM, TPCHIT.getResource("lineitem.tbl")));
                        ps.execute();
                        ps = spliceClassWatcher.prepareStatement(
                                format("call SYSCS_UTIL.SYSCS_IMPORT_DATA(" +
                                        "'%s','%s',null,null,'%s','|','\"',null,null,null)",
                                        CLASS_NAME, ORDERS, TPCHIT.getResource("orders.tbl")));
                        ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            })
        .around(TestUtils.createStringDataWatcher(spliceClassWatcher,
                                                     "create table people " +
                                                         "  (fname varchar(25)," +
                                                         "  lname varchar(25), " +
                                                         "  age int, " +
                                                         "  primary key(fname,lname));" +
                                                         "create table purchase " +
                                                         "  (fname varchar(25)," +
                                                         "  lname varchar(25), num int," +
                                                         "  primary key (fname,lname,num));" +
                                                         "insert into people values " +
                                                         "  ('adam', 'scott', 22)," +
                                                         "  ('scott', 'anchorman', 23)," +
                                                         "  ('tori', 'spelling', 9);" +
                                                         "insert into purchase values" +
                                                         "  ('adam', 'scott', 1)," +
                                                         "  ('scott', 'anchorman', 1)," +
                                                         "  ('scott', 'anchorman', 2)," +
                                                         "  ('tori', 'spelling', 1)," +
                                                         "  ('adam', 'scott', 10)," +
                                                         "  ('scott', 'anchorman', 5);",
                                                     CLASS_NAME));


    @Rule
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    public static final List<String> STRATEGIES = Arrays.asList("SORTMERGE", "NESTEDLOOP", "BROADCAST", "MERGE");

    @Test
    public void testSimpleJoinOverAllStrategies() throws Exception {
        String query = "select count(*) from orders, lineitem --splice-properties joinStrategy=%s \n" +
                "where o_orderkey = l_orderkey";
        List<Integer> counts = Lists.newArrayList();

        for (String strategy : STRATEGIES) {
            ResultSet rs = methodWatcher.executeQuery(String.format(query, strategy));
            rs.next();
            counts.add(rs.getInt(1));
        }

        Assert.assertTrue("Each strategy returns the same number of join results",
                counts.size() == STRATEGIES.size()
                        && Sets.newHashSet(counts).size() == 1);
    }

    @Test
    public void testSimpleJoinOn2Columns() throws Exception {
        List<Object[]> expected = Arrays.asList(
                                                   o("adam",1),
                                                   o("adam",10),
                                                   o("scott",1),
                                                   o("scott",2),
                                                   o("scott",5),
                                                   o("tori",1));
        String query = "select p.fname, t.num from people p, purchase t --splice-properties joinStrategy=%s \n" +
                           "where p.fname = t.fname and p.lname = t.lname order by p.fname, t.num";
        List<List<Object[]>> results = Lists.newArrayList();

        for (String strategy : STRATEGIES) {
            ResultSet rs = methodWatcher.executeQuery(String.format(query, strategy));
            results.add(TestUtils.resultSetToArrays(rs));
        }

        Assert.assertTrue("Each strategy returns the same join results",
                             results.size() == STRATEGIES.size());
        for (List<Object[]> result: results) {
            Assert.assertArrayEquals("The join results match expected results",
                                        expected.toArray(), result.toArray());
        }
    }

    @Test
    public void testSmallMergeJoinOverIndex() throws Exception {
        methodWatcher.executeUpdate("create index staff_ordered on staff (empnum)");
        methodWatcher.executeUpdate("create index works_ordered on works (empnum)");

        try {

            ResultSet rs = methodWatcher.executeQuery("select s.empnum, count(*) " +
                    "from staff s, works w where s.empnum = w.empnum " +
                    "group by s.empnum " +
                    "order by s.empnum");
            List<Object[]> msj = TestUtils.resultSetToArrays(rs);

            rs = methodWatcher.executeQuery("select s.empnum, count(*) " +
                    "from staff s --splice-properties index=staff_ordered \n" +
                    ", works w --splice-properties index=works_ordered, " +
                    "joinStrategy=merge \n" +
                    "where s.empnum = w.empnum " +
                    "group by s.empnum " +
                    "order by s.empnum");
            List<Object[]> merge = TestUtils.resultSetToArrays(rs);

            Assert.assertArrayEquals(msj.toArray(), merge.toArray());


        } finally {
            methodWatcher.executeUpdate("drop index staff_ordered");
            methodWatcher.executeUpdate("drop index works_ordered");
        }
    }

    @Test
    public void testBiggerMergeJoinOverIndex() throws Exception {
        methodWatcher.executeUpdate("create index lineitem_ordered on lineitem (l_orderkey)");

        try {

            ResultSet rs = methodWatcher.executeQuery("select count(*) " +
                    "from orders, lineitem where o_orderkey = l_orderkey");
            List<Object[]> msj = TestUtils.resultSetToArrays(rs);

            rs = methodWatcher.executeQuery("select count(*) " +
                    "from orders, lineitem " +
                    "--splice-properties index=lineitem_ordered, joinStrategy=merge\n" +
                    "where o_orderkey = l_orderkey");
            List<Object[]> merge = TestUtils.resultSetToArrays(rs);

            Assert.assertArrayEquals(msj.toArray(), merge.toArray());


        } finally {
            methodWatcher.executeUpdate("drop index lineitem_ordered");
        }
    }

    @Test
    public void testThrowIfNonCoveringIndex() throws Exception {
        methodWatcher.executeUpdate("create index staff_ordered on staff (empnum)");
        methodWatcher.executeUpdate("create index works_ordered on works (empnum)");

        try {
            ResultSet rs = methodWatcher.executeQuery("select s.empnum, s.empname " +
                    "from works w --splice-properties index=works_ordered \n" +
                    ", staff s --splice-properties index=staff_ordered, " +
                    "joinStrategy=merge \n" +
                    "where s.empnum = w.empnum " +
                    "order by s.empnum");
            List<Object[]> merge = TestUtils.resultSetToArrays(rs);
        } catch (SQLException e){
            Assert.assertTrue("Non-covering index not allowed on right",
                    e.getNextException().getMessage().contains("non-covering index"));

        } finally {
            methodWatcher.executeUpdate("drop index staff_ordered");
            methodWatcher.executeUpdate("drop index works_ordered");
        }
    }
}
