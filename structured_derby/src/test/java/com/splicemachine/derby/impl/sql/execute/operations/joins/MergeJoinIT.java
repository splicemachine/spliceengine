package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.derby.test.TPCHIT;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    protected static final String LINEITEM = "LINEITEM";
    protected static final String ORDERS = "ORDERS";
    protected static final String FOO = "FOO";
    protected static final String FOO2 = "FOO2";
    protected static final String FOO2_IDX = "FOO2_IDX";
    protected static final String TEST = "TEST";
    protected static final String TEST2 = "TEST2";
    
    

    protected static SpliceTableWatcher lineItemTable = new SpliceTableWatcher(LINEITEM, CLASS_NAME,
            "( L_ORDERKEY INTEGER NOT NULL,L_PARTKEY INTEGER NOT NULL, L_SUPPKEY INTEGER NOT NULL, " +
                    "L_LINENUMBER  INTEGER NOT NULL, L_QUANTITY DECIMAL(15,2), L_EXTENDEDPRICE DECIMAL(15,2)," +
                    "L_DISCOUNT DECIMAL(15,2), L_TAX DECIMAL(15,2), L_RETURNFLAG  CHAR(1), L_LINESTATUS CHAR(1), " +
                    "L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT CHAR(25)," +
                    "L_SHIPMODE VARCHAR(10),L_COMMENT VARCHAR(44),PRIMARY KEY(L_ORDERKEY,L_LINENUMBER))");
    protected static SpliceTableWatcher orderTable = new SpliceTableWatcher(ORDERS, CLASS_NAME,
            "( O_ORDERKEY INTEGER NOT NULL PRIMARY KEY,O_CUSTKEY INTEGER,O_ORDERSTATUS CHAR(1)," +
                    "O_TOTALPRICE DECIMAL(15,2),O_ORDERDATE DATE, O_ORDERPRIORITY  CHAR(15), " +
                    "O_CLERK CHAR(15), O_SHIPPRIORITY INTEGER, O_COMMENT VARCHAR(79))");


    protected static SpliceTableWatcher fooTable = new SpliceTableWatcher(FOO, CLASS_NAME,
            "(col1 int, col2 int, primary key (col1))");

    protected static SpliceTableWatcher foo2Table = new SpliceTableWatcher(FOO2, CLASS_NAME,
            "(col1 int, col2 int, col3 int)");

    protected static SpliceTableWatcher testTable = new SpliceTableWatcher(TEST, CLASS_NAME,
            "(col1 int, col2 int, col3 int, col4 int, col5 int, col6 int, col7 int, col8 int, primary key (col5, col7))");
    
    protected static SpliceTableWatcher test2Table = new SpliceTableWatcher(TEST2, CLASS_NAME,
            "(col1 int, col2 int, col3 int, col4 int, primary key (col1, col2))");

    protected static SpliceIndexWatcher foo2Index = new SpliceIndexWatcher(FOO2,CLASS_NAME,FOO2_IDX,CLASS_NAME,"(col3, col2, col1)");

    protected static String MERGE_INDEX_RIGHT_SIDE_TEST = format("select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
    						" %s.%s inner join %s.%s --SPLICE-PROPERTIES index=%s, joinStrategy=MERGE\n" + 
    						" on foo.col1 = foo2.col3",CLASS_NAME,FOO,CLASS_NAME,FOO2,FOO2_IDX);

    protected static String MERGE_WITH_UNORDERED = format("select test.col1, test2.col4 from --SPLICE-PROPERTIES joinOrder=fixed\n" +
			" %s.%s inner join %s.%s --SPLICE-PROPERTIES joinStrategy=MERGE\n" + 
			" on test.col7 = test2.col2 and" +
			" test.col5 = test2.col1",CLASS_NAME,TEST,CLASS_NAME,TEST2);

    
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceSchemaWatcher)
        .around(spliceClassWatcher)
        .around(lineItemTable)
        .around(orderTable)
        .around(fooTable)
        .around(foo2Table)
        .around(foo2Index)
        .around(testTable)
        .around(test2Table)
        .around(new SpliceDataWatcher() {
        	  @Override
              protected void starting(Description description) {
                  try {
                	  spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,2)",CLASS_NAME,FOO));
                	  spliceClassWatcher.executeUpdate(format("insert into %s.%s values (3,2,1)",CLASS_NAME,FOO2));
                	  spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,2,3,4,1,6,2,8)",CLASS_NAME,TEST));
                	  spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,2,3,4)",CLASS_NAME,TEST2));

                  } catch (Exception e) {
                      throw new RuntimeException(e);
                  } finally {
                      spliceClassWatcher.closeAll();
                  }
              }
        })
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
                                                 CLASS_NAME))
    .around(TestUtils.createStringDataWatcher(spliceClassWatcher,
                                                     "create table shipmode " +
                                                         "  (mode varchar(10)," +
                                                         "  id int," +
                                                         "  primary key (id)); " +
                                                         "insert into shipmode values " +
                                                         "  ('AIR', 1)," +
                                                         "  ('FOB', 2)," +
                                                         "  ('MAIL', 3)," +
                                                         "  ('RAIL', 4)," +
                                                         "  ('REG AIR', 5)," +
                                                         "  ('SHIP', 6)," +
                                                         "  ('TRUCK', 7);",
                                                     CLASS_NAME));


    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static final List<String> STRATEGIES = Arrays.asList("SORTMERGE", "NESTEDLOOP", "BROADCAST", "MERGE");

    @Test
    public void testSimpleJoinOverAllStrategies() throws Exception {
        String query = "select count(*) from --SPLICE-PROPERTIES joinOrder=FIXED \n" +
                "orders, lineitem --splice-properties joinStrategy=%s \n" +
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
    @Ignore
    public void test3WayJoinOverAllStrategyPairs() throws Exception {
        methodWatcher.executeUpdate("create index proj_pnum on proj (pnum)");

        List<Pair<String,String>> strategyPairs = Lists.newLinkedList();
        for (String s1 : STRATEGIES){
            for (String s2: STRATEGIES){
                strategyPairs.add(Pair.newPair(s1, s2));
            }
        }

        final List<Object[]> expected = Arrays.asList(
                                                   o("P1", "E1", "P1"),
                                                   o("P1", "E2", "P1"),
                                                   o("P2", "E1", "P2"),
                                                   o("P2", "E2", "P2"),
                                                   o("P2", "E3", "P2"),
                                                   o("P2", "E4", "P2"),
                                                   o("P3", "E1", "P3"),
                                                   o("P4", "E1", "P4"),
                                                   o("P4", "E4", "P4"),
                                                   o("P5", "E1", "P5"),
                                                   o("P5", "E4", "P5"),
                                                   o("P6", "E1", "P6"));

        try {
            String query = "select w.pnum, w.empnum, p2.pnum " +
                               "from --splice-properties joinOrder=fixed\n" +
                               "         (select empnum, pnum from works order by pnum) w " +
                               "            inner join proj p --splice-properties index=proj_pnum, joinStrategy=%s\n" +
                               "               on w.pnum = p.pnum" +
                               "     inner join proj p2  --splice-properties index=proj_pnum, joinStrategy=%s\n" +
                               "            on p.pnum = p2.pnum " +
                               " order by w.pnum, w.empnum, p2.pnum";

            for (Pair<String,String> pair : strategyPairs){
                try {
                    ResultSet rs = methodWatcher.executeQuery(String.format(query,
                                                                               pair.getFirst(),
                                                                               pair.getSecond()));
                    List<Object[]> results = TestUtils.resultSetToArrays(rs);
                    Assert.assertArrayEquals(String.format("%s over %s produces incorrect results",
                                                              pair.getSecond(), pair.getFirst()),
                                                expected.toArray(),
                                                results.toArray());
                } catch (Exception e) {
                    Assert.fail(String.format("%s failed with exception %s", pair, e));
                }
            }


        } finally {
            methodWatcher.executeUpdate("drop index proj_pnum");
        }
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
    @Ignore("No longer valid since plan is not feasible")
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

    @Test
    public void testBCastOverMerge() throws Exception {
        String query = "select count(*) " +
                           "from --splice-properties joinOrder=fixed\n" +
                           "    (select * from orders where o_orderkey > 1000) o, " +
                           "    lineitem l --splice-properties joinStrategy=merge\n" +
                           "     , shipmode s --splice-properties joinStrategy=broadcast\n " +
                           "where o_orderkey = l_orderkey" +
                               "  and l_shipmode = s.mode";
        Assert.assertEquals(8954L, TestUtils.resultSetToArrays(methodWatcher.executeQuery(query)).get(0)[0]);
    }
    
    @Test
    public void testMergeWithRightCoveringIndex() throws Exception {
    	List<Object[]> data = TestUtils.resultSetToArrays(methodWatcher.executeQuery(MERGE_INDEX_RIGHT_SIDE_TEST));
    	Assert.assertTrue("does not return 1 row for merge, position problems in MergeSortJoinStrategy/Operation?",data.size()==1);
    }

    @Test
    public void testMergeWithUnorderedPredicates() throws Exception {
    	List<Object[]> data = TestUtils.resultSetToArrays(methodWatcher.executeQuery(MERGE_WITH_UNORDERED));
    	Assert.assertTrue("does not return 1 row for merge, position problems in MergeSortJoinStrategy/Operation?",data.size()==1);
    }

}
