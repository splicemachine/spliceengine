/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.db.shared.common.reference.SQLState;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Sets;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static com.splicemachine.homeless.TestUtils.o;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
    protected static final String A = "A";
    protected static final String B = "B";
    protected static final String A_IDX = "A_IDX";
    protected static final String ORDERLINE = "ORDER_LINE";
    protected static final String STOCK = "STOCK";
    protected static final String ORDERLINE_IDX = "ORDER_LINE_IX";

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


    protected static SpliceTableWatcher orderLineTable = new SpliceTableWatcher(ORDERLINE, CLASS_NAME,
            "(ol_w_id int NOT NULL,\n" +
                    "  ol_d_id int NOT NULL,\n" +
                    "  ol_o_id int NOT NULL,\n" +
                    "  ol_number int NOT NULL,\n" +
                    "  ol_i_id int NOT NULL,\n" +
                    "  ol_delivery_d timestamp,\n" +
                    "  ol_amount decimal(6,2) NOT NULL,\n" +
                    "  ol_supply_w_id int NOT NULL,\n" +
                    "  ol_quantity decimal(2,0) NOT NULL,\n" +
                    "  ol_dist_info char(24) NOT NULL,\n" +
                    "  PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number))");

    protected static SpliceTableWatcher stockTable = new SpliceTableWatcher(STOCK, CLASS_NAME,
            "(s_w_id int NOT NULL,\n" +
                    "  s_i_id int NOT NULL,\n" +
                    "  s_quantity decimal(4,0) NOT NULL,\n" +
                    "  s_ytd decimal(8,2) NOT NULL,\n" +
                    "  s_order_cnt int NOT NULL,\n" +
                    "  s_remote_cnt int NOT NULL,\n" +
                    "  s_data varchar(50) NOT NULL,\n" +
                    "  s_dist_01 varchar(24) NOT NULL,\n" +
                    "  s_dist_02 varchar(24) NOT NULL,\n" +
                    "  s_dist_03 varchar(24) NOT NULL,\n" +
                    "  s_dist_04 varchar(24) NOT NULL,\n" +
                    "  s_dist_05 varchar(24) NOT NULL,\n" +
                    "  s_dist_06 varchar(24) NOT NULL,\n" +
                    "  s_dist_07 varchar(24) NOT NULL,\n" +
                    "  s_dist_08 varchar(24) NOT NULL,\n" +
                    "  s_dist_09 varchar(24) NOT NULL,\n" +
                    "  s_dist_10 varchar(24) NOT NULL,\n" +
                    "  PRIMARY KEY (s_w_id,s_i_id)\n" +
                    ")");

    protected static SpliceIndexWatcher orderLineIndex = new SpliceIndexWatcher(ORDERLINE,CLASS_NAME,ORDERLINE_IDX,CLASS_NAME,"(ol_w_id, ol_d_id, ol_i_id, ol_o_id)");

    protected static SpliceTableWatcher fooTable = new SpliceTableWatcher(FOO, CLASS_NAME,
            "(col1 int, col2 int, primary key (col1))");

    protected static SpliceTableWatcher foo2Table = new SpliceTableWatcher(FOO2, CLASS_NAME,
            "(col1 int, col2 int, col3 int)");

    protected static SpliceTableWatcher testTable = new SpliceTableWatcher(TEST, CLASS_NAME,
            "(col1 int, col2 int, col3 int, col4 int, col5 int, col6 int, col7 int, col8 int, primary key (col5, col7))");

    protected static SpliceTableWatcher test2Table = new SpliceTableWatcher(TEST2, CLASS_NAME,
            "(col1 int, col2 int, col3 int, col4 int, primary key (col1, col2))");

    protected static SpliceIndexWatcher foo2Index = new SpliceIndexWatcher(FOO2,CLASS_NAME,FOO2_IDX,CLASS_NAME,"(col3, col2, col1)");

    protected static SpliceTableWatcher aTable = new SpliceTableWatcher(A, CLASS_NAME,
            "(c1 int, c2 int, c3 int, c4 int)");

    protected static SpliceIndexWatcher aIndex = new SpliceIndexWatcher(A, CLASS_NAME, A_IDX,CLASS_NAME,"(c1, c2, c3)");

    protected static SpliceTableWatcher bTable = new SpliceTableWatcher(B, CLASS_NAME,
            "(c1 int, c2 int, c3 int, primary key(c1, c2))");

    protected static String MERGE_INDEX_RIGHT_SIDE_NEGATIVE_TEST = format("select sum(a.c4) from --SPLICE-PROPERTIES joinOrder=fixed\n" +
            " %s.%s b inner join %s.%s a --SPLICE-PROPERTIES index=%s, joinStrategy=MERGE\n" +
            " on b.c1 = a.c3 and a.c1=1",CLASS_NAME,B,CLASS_NAME,A,A_IDX);

    protected static String MERGE_INDEX_RIGHT_SIDE_TEST = format("select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
            " %s.%s inner join %s.%s --SPLICE-PROPERTIES index=%s, joinStrategy=MERGE\n" +
            " on foo.col1 = foo2.col3",CLASS_NAME,FOO,CLASS_NAME,FOO2,FOO2_IDX);

    protected static String MERGE_WITH_UNORDERED = format("select test.col1, test2.col4 from --SPLICE-PROPERTIES joinOrder=fixed\n" +
            " %s.%s inner join %s.%s --SPLICE-PROPERTIES joinStrategy=MERGE\n" +
            " on test.col7 = test2.col2 and" +
            " test.col5 = test2.col1",CLASS_NAME,TEST,CLASS_NAME,TEST2);

    protected static String MERGE_WITH_OUTER_TABLE_PREDICATE = format(
            "explain SELECT COUNT(DISTINCT (s_i_id)) AS stock_count\n" +
            "FROM --splice-properties joinOrder=fixed\n" +
            "%s.%s --splice-properties index=%s\n" +
            ",%s.%s --splice-properties joinStrategy=MERGE\n" +
            "WHERE\n" +
            "ol_w_id = 1\n" +
            "AND ol_d_id = 1\n" +
            "AND ol_i_id = s_i_id\n" +
            "AND ol_w_id = s_w_id\n" +
            "AND s_quantity < 25\n" +
            "AND ol_o_id < 50\n" +
            "AND ol_o_id >= 50 - 20",CLASS_NAME,ORDERLINE,ORDERLINE_IDX,CLASS_NAME,STOCK);
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceSchemaWatcher)
            .around(spliceClassWatcher)
            .around(aTable)
            .around(bTable)
            .around(aIndex)
            .around(lineItemTable)
            .around(orderTable)
            .around(fooTable)
            .around(foo2Table)
            .around(foo2Index)
            .around(testTable)
            .around(test2Table)
            .around(orderLineTable)
            .around(stockTable)
            .around(orderLineIndex)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,2)", CLASS_NAME, FOO));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (3,2,1)", CLASS_NAME, FOO2));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,2,3,4,1,6,2,8)", CLASS_NAME, TEST));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,2,3,4)", CLASS_NAME, TEST2));

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
                                format("call SYSCS_UTIL.IMPORT_DATA(" +
                                                "'%s','%s',null,'%s','|','\"',null,null,null,1,null,true,null)",
                                        CLASS_NAME, LINEITEM, getResource("lineitem.tbl")));
                        ps.execute();
                        ps = spliceClassWatcher.prepareStatement(
                                format("call SYSCS_UTIL.IMPORT_DATA(" +
                                                "'%s','%s',null,'%s','|','\"',null,null,null,1,null,true,null)",
                                        CLASS_NAME, ORDERS, getResource("orders.tbl")));
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
                    CLASS_NAME)).around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(CallableStatement cs = spliceClassWatcher.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
                       cs.setString(1,spliceSchemaWatcher.schemaName);
                        cs.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            });


    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    // TODO Add HalfSortMerge when re-enabled DB-4913
    public static final List<String> STRATEGIES = Arrays.asList("SORTMERGE", "NESTEDLOOP", "BROADCAST", "MERGE");

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }
    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a int, b int, c int, d int)")
                .withInsert("insert into t1 values(?,?,?,?)")
                .withIndex("create index ti on t1(a, b, c)")
                .withRows(rows(
                        row(1, 1, 1, 10),
                        row(1, 1, 2, 20),
                        row(1, 2, 1, 30),
                        row(1, 2, 2, 40),
                        row(2, 1, 1, 50),
                        row(2, 1, 2, 60),
                        row(2, 2, 1, 70),
                        row(2, 2, 2, 80)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a int, b int, c int, d int, primary key(a, b, c))")
                .withInsert("insert into t2 values(?,?,?,?)")
                .withRows(rows(
                        row(1, 1, 1, 100),
                        row(1, 1, 2, 200),
                        row(1, 2, 1, 300),
                        row(1, 2, 2, 400)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, d3 int, primary key(a3))")
                .withInsert("insert into t3 values(?,?,?,?)")
                .withIndex("create index ix_t3 on t3(b3, c3, d3)")
                .withRows(rows(
                        row(1, 1, 1, 1),
                        row(2, 2, 2, 2),
                        row(3, 3, 3, 3),
                        row(4, 4, 4, 4),
                        row(5, 5, 5, 5)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t4 (a4 int, b4 int, c4 int, d4 int, primary key(a4))")
                .withInsert("insert into t4 values(?,?,?,?)")
                .withIndex("create index ix_t4 on t4(b4, c4, d4)")
                .withRows(rows(
                        row(3, 3, 3, 3),
                        row(5, 5, 5, 5),
                        row(6, 6, 6, 6)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tab1 (a int, b int, c int, d int)")
                .withInsert("insert into tab1 values(?,?,?,?)")
                .withIndex("create index tabi on tab1(a, b, c)")
                .withRows(rows(
                        row(1, 1, 0, 10)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tab2 (a int, b int, c int, d int, primary key(a, b, c))")
                .withInsert("insert into tab2 values(?,?,?,?)")
                .withIndex("create index tab2i on tab2(a, c, d)")
                .withRows(rows(
                        row(1, 0, 1, 200)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tab3 (a int, b varchar(10), c int, d int, primary key(a, b, c))")
                .withInsert("insert into tab3 values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", 0, 200)))
                .create();
        
        new TableCreator(conn)
                .withCreate("create table tt1 (a1 int, b1 int, c1 int, d1 int, e1 int, primary key(e1, d1, c1))")
                .withInsert("insert into tt1 values(?,?,?,?,?)")
                .withIndex("create index idx_tt1 on tt1 (d1, b1)")
                .withIndex("create index idx2_tt1 on tt1 (a1, c1)")
                .withRows(rows(
                        row(3, 30, 300, 3000, 30000), row(5, 50, 500, 5000, 50000)))
                .create();
        new TableCreator(conn)
                .withCreate("create table tt2 (a2 int, b2 int, c2 int, d2 int, e2 int, primary key(e2, d2, c2))")
                .withInsert("insert into tt2 values(?,?,?,?,?)")
                .withIndex("create index idx_tt2 on tt2 (d2, b2)")
                .withIndex("create index idx2_tt2 on tt2 (a2, c2)")
                .withRows(rows(
                        row(3, 30, 300, 3000, 30000), row(5, 50, 500, 5000, 50000)))
                .create();
    }
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
        Assert.assertTrue("does not return 1 row for merge, position problems in MergeSortJoinStrategy/Operation?", data.size() == 1);
    }

    @Test
    public void testMergeWithUnorderedPredicates() throws Exception {
        List<Object[]> data = TestUtils.resultSetToArrays(methodWatcher.executeQuery(MERGE_WITH_UNORDERED));
        Assert.assertTrue("does not return 1 row for merge, position problems in MergeSortJoinStrategy/Operation?",data.size()==1);
    }

    @Test
    public void testMergeWithRightIndexNegative() throws Exception {
        try {
            TestUtils.resultSetToArrays(methodWatcher.executeQuery(MERGE_INDEX_RIGHT_SIDE_NEGATIVE_TEST));
            fail("Expected infeasible join strategy exception");
        }
        catch (Exception e) {
            Assert.assertTrue(e.getMessage().compareTo("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.")==0);
        }
    }

    @Test
    public void testMergeWithPredicateFromOuterTable() throws Exception {
        rowContainsQuery(6, MERGE_WITH_OUTER_TABLE_PREDICATE, "MergeJoin", methodWatcher);
    }

    @Test
    public void GTNotFeasible() throws Exception {
        String sql = "explain select * \n" +
                "from --splice-properties joinOrder=fixed\n" +
                "t1 --splice-properties index=ti\n" +
                ",t2 --splice-properties joinStrategy=MERGE\n" +
                "where t1.a>=1 and t2.a=1 and t1.b=t2.b";
        try {
            TestUtils.resultSetToArrays(methodWatcher.executeQuery(sql));
            fail("Expected infeasible join strategy exception");
        }
        catch (Exception e) {
            Assert.assertTrue(e.getMessage().compareTo("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.")==0);
        }
    }

    @Test
    public void innerGapNotFeasible() throws Exception {
        String sql = "explain select * \n" +
                "from --splice-properties joinOrder=fixed\n" +
                "t1 --splice-properties index=ti\n" +
                ",t2 --splice-properties joinStrategy=MERGE\n" +
                "where t1.a=t2.a and t1.b=t2.c";
        try {
            TestUtils.resultSetToArrays(methodWatcher.executeQuery(sql));
            fail("Expected infeasible join strategy exception");
        }
        catch (Exception e) {
            Assert.assertTrue(e.getMessage().compareTo("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.")==0);
        }
    }

    @Test
    public void outerGapNotFeasible() throws Exception {
        String sql = "select * \n" +
                "from --splice-properties joinOrder=fixed\n" +
                "t1 --splice-properties index=ti\n" +
                ",t2 --splice-properties joinStrategy=MERGE\n" +
                "where t1.a = 1 and t2.a=1 and t1.c=t2.b";
        try {
            TestUtils.resultSetToArrays(methodWatcher.executeQuery(sql));
            fail("Expected infeasible join strategy exception");
        }
        catch (Exception e) {
            Assert.assertTrue(e.getMessage().compareTo("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.")==0);
        }
    }

    @Test
    public void testRightTableScanStartKey() throws Exception {
        String sql ="select *\n" +
                "from --splice-properties joinOrder=fixed\n" +
                "tab1 --splice-properties index=tabi\n" +
                ", tab2 --splice-properties joinStrategy=MERGE\n" +
                "where tab1.a=1 and tab1.b=1 and tab1.c=tab2.b and tab2.a=1";
        ResultSet rs = methodWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testRightTableScanStartKey2() throws Exception {
        String sql ="select *\n" +
                "from --splice-properties joinOrder=fixed\n" +
                "tab1 --splice-properties index=tabi\n" +
                ", tab2 --splice-properties joinStrategy=MERGE\n" +
                "where tab1.a=tab2.a and tab2.b=0 and tab1.b=tab2.c";
        ResultSet rs = methodWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testRightTableScanStartKey3() throws Exception {
        String sql = "select *\n" +
                "from --splice-properties joinOrder=fixed\n" +
                "tab1 --splice-properties index=tabi\n" +
                ", tab3 --splice-properties joinStrategy=MERGE\n" +
                "where tab1.a=1 and tab3.a=1 and tab1.a=tab3.a";
        ResultSet rs = methodWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testRightTableScanKeyWithGEPredidateOnLeadingIndexColumn() throws Exception {
        /* test query with inequality scan key on right table's leading pk column (e2>20000) */
        /* Q1 : e2 >20000 */
        String sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n where e1=e2 and e2 > 20000";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "3000 |30000 |3000 |30000 |\n" +
                "5000 |50000 |5000 |50000 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n where e1=e2 and e2 > 20000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q2: e2 >=30000 where 30000 is the first row in right table */
        sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n where e1=e2 and e2 >= 30000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n where e1=e2 and e2 >= 30000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q3: e2 > 30000 */
        sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n where e1=e2 and e2 > 30000";
        expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "5000 |50000 |5000 |50000 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n where e1=e2 and e2 > 30000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }

    @Test
    public void testRightTableScanKeyWithLEPredicateOnLeadingIndexColumn() throws Exception {
        /* test query with inequality scan key on right table's leading pk column (e2<50000) */
        String sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n where e1=e2 and e2 < 50000";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "3000 |30000 |3000 |30000 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n where e1=e2 and e2 < 50000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTableScanKeyWithEqualityPredicateOnLeadingIndexColumn() throws Exception {
        /* test query with equality scan key on right table's leading pk column (e2=50000) */
        String sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n where e1=e2 and e2 = 50000";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "5000 |50000 |5000 |50000 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n where e1=e2 and e2 = 50000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTableScanKeyCombiningGEStartKeyAndHashColumnValue() throws Exception {
        String sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n where e2 = 30000 and e1 =30000 and d2 > 2000 and d1=d2 and c1=c2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "3000 |30000 |3000 |30000 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n where e2 = 30000 and e1 =30000 and d2 > 2000 and d1=d2 and c1=c2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTableScanKeyCombiningEqualStartKeyAndHashColumnValue() throws Exception {
        String sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n where e2 = 30000 and e1 =30000 and d2 = 3000 and d1=d2 and c1=c2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "3000 |30000 |3000 |30000 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, e1, d2, e2 from tt1, tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n where e2 = 30000 and e1 =30000 and d2 = 3000 and d1=d2 and c1=c2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTableScanKeyCombiningGEStartKeyAndHashColumnValueCoveringIndex() throws Exception {
        /* idx_tt2 (d2, b2) is used, idx_tt2 has index columns whose positions in the base table are reversed, so there is a projectrestrict operation
           on top of the IndexScan, we cannot take advantage of the first qualified left on the join column to further restrict the scan of right table
          */
        String sql = "select d1, b1, d2, b2 from tt1, tt2 --splice-properties index=idx_tt2, joinStrategy=MERGE, useSpark=false\n where d2 > 2000 and d1=d2 and b1=b2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  |B1 | D2  |B2 |\n" +
                "--------------------\n" +
                "3000 |30 |3000 |30 |\n" +
                "5000 |50 |5000 |50 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, b1, d2, b2 from tt1, tt2 --splice-properties index=idx_tt2, joinStrategy=MERGE, useSpark=true\n where d2 > 2000 and d1=d2 and b1=b2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTableScanKeyCombiningEqualStartKeyAndHashColumnValueCoveringIndex() throws Exception {
         /* idx_tt2 (d2, b2) is used, idx_tt2 has index columns whose positions in the base table are reversed, so there is a projectrestrict operation
           on top of the IndexScan, we cannot take advantage of the first qualified left on the join column to further restrict the scan of right table
          */
        String sql = "select d1, b1, d2, b2 from tt1, tt2 --splice-properties index=idx_tt2, joinStrategy=MERGE, useSpark=false\n where d2 = 3000 and d1=d2 and b1=b2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  |B1 | D2  |B2 |\n" +
                "--------------------\n" +
                "3000 |30 |3000 |30 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = sql = "select d1, b1, d2, b2 from tt1, tt2 --splice-properties index=idx_tt2, joinStrategy=MERGE, useSpark=true\n where d2 = 3000 and d1=d2 and b1=b2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTaleScanKeyCombiningGEStartKeyAndHashColumnValueCoveringIndex2() throws Exception {
         /* idx2_tt2 (a2, c2) is used, idx2_tt2 has index columns whose positions in the base table are in the same order, so there is no projectrestrict operation
           on top of the IndexScan, we can take advantage of the first qualified left on the join column to further restrict the scan of right table
          */
        String sql = "select a1, c1, a2, c2 from tt1, tt2 --splice-properties index=idx2_tt2, joinStrategy=MERGE, useSpark=false\n where a2 > 2 and a1=a2 and c1=c2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "A1 |C1  |A2 |C2  |\n" +
                "------------------\n" +
                " 3 |300 | 3 |300 |\n" +
                " 5 |500 | 5 |500 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select a1, c1, a2, c2 from tt1, tt2 --splice-properties index=idx2_tt2, joinStrategy=MERGE, useSpark=true\n where a2 > 2 and a1=a2 and c1=c2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTaleScanKeyCombiningEqualStartKeyAndHashColumnValueCoveringIndex2() throws Exception {
         /* idx2_tt2 (a2, c2) is used, idx2_tt2 has index columns whose positions in the base table are in the same order, so there is no projectrestrict operation
           on top of the IndexScan, we can take advantage of the first qualified left on the join column to further restrict the scan of right table
          */
        String sql = "select a1, c1, a2, c2 from tt1, tt2 --splice-properties index=idx2_tt2, joinStrategy=MERGE, useSpark=false\n where a2 = 3 and a1=a2 and c1=c2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "A1 |C1  |A2 |C2  |\n" +
                "------------------\n" +
                " 3 |300 | 3 |300 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = sql = "select a1, c1, a2, c2 from tt1, tt2 --splice-properties index=idx2_tt2, joinStrategy=MERGE, useSpark=true\n where a2 = 3 and a1=a2 and c1=c2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTableScanKeyWithGEPredidateOnLeadingIndexColumnOuterJoin() throws Exception {
        /* test query with inequality scan key on right table's leading pk column (e2>20000) */
        /* Q1 : e2 >20000 */
        String sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n on e1=e2 and e2 > 20000";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "3000 |30000 |3000 |30000 |\n" +
                "5000 |50000 |5000 |50000 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n on e1=e2 and e2 > 20000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q2: e2 >=30000 where 30000 is the first row in right table */
        sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n on e1=e2 and e2 >= 30000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n on e1=e2 and e2 >= 30000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q3: e2 > 30000 */
        sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n on e1=e2 and e2 > 30000";
        expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "3000 |30000 |NULL |NULL  |\n" +
                "5000 |50000 |5000 |50000 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n on e1=e2 and e2 > 30000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }

    @Test
    public void testRightTableScanKeyWithLEPredicateOnLeadingIndexColumnLeftJoin() throws Exception {
        /* test query with inequality scan key on right table's leading pk column (e2<50000) */
        String sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n on e1=e2 and e2 < 50000";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "3000 |30000 |3000 |30000 |\n" +
                "5000 |50000 |NULL |NULL  |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n on e1=e2 and e2 < 50000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTableScanKeyWithEqualityPredicateOnLeadingIndexColumnLeftJoin() throws Exception {
        /* test query with equality scan key on right table's leading pk column (e2=50000) */
        String sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n on e1=e2 and e2 = 50000";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String  expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "3000 |30000 |NULL |NULL  |\n" +
                "5000 |50000 |5000 |50000 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n on e1=e2 and e2 = 50000";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTableScanKeyCombiningGEStartKeyAndHashColumnValueLeftJoin() throws Exception {
        String sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n on e2 = 30000 and e1 =30000 and d2 > 2000 and d1=d2 and c1=c2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "3000 |30000 |3000 |30000 |\n" +
                "5000 |50000 |NULL |NULL  |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n on e2 = 30000 and e1 =30000 and d2 > 2000 and d1=d2 and c1=c2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTableScanKeyCombiningEqualStartKeyAndHashColumnValueLeftJoin() throws Exception {
        String sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=false\n on e2 = 30000 and e1 =30000 and d2 = 3000 and d1=d2 and c1=c2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  | E1   | D2  | E2   |\n" +
                "--------------------------\n" +
                "3000 |30000 |3000 |30000 |\n" +
                "5000 |50000 |NULL |NULL  |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, e1, d2, e2 from tt1 left join tt2 --splice-properties joinStrategy=MERGE, useSpark=true\n on e2 = 30000 and e1 =30000 and d2 = 3000 and d1=d2 and c1=c2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTableScanKeyCombiningGEStartKeyAndHashColumnValueCoveringIndexLeftJoin() throws Exception {
        /* idx_tt2 (d2, b2) is used, idx_tt2 has index columns whose positions in the base table are reversed, so there is a projectrestrict operation
           on top of the IndexScan, we cannot take advantage of the first qualified left on the join column to further restrict the scan of right table
          */
        String sql = "select d1, b1, d2, b2 from tt1 left join tt2 --splice-properties index=idx_tt2, joinStrategy=MERGE, useSpark=false\n on d2 > 2000 and d1=d2 and b1=b2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  |B1 | D2  |B2 |\n" +
                "--------------------\n" +
                "3000 |30 |3000 |30 |\n" +
                "5000 |50 |5000 |50 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select d1, b1, d2, b2 from tt1 left join tt2 --splice-properties index=idx_tt2, joinStrategy=MERGE, useSpark=true\n on d2 > 2000 and d1=d2 and b1=b2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTableScanKeyCombiningEqualStartKeyAndHashColumnValueCoveringIndexLeftJoin() throws Exception {
         /* idx_tt2 (d2, b2) is used, idx_tt2 has index columns whose positions in the base table are reversed, so there is a projectrestrict operation
           on top of the IndexScan, we cannot take advantage of the first qualified left on the join column to further restrict the scan of right table
          */
        String sql = "select d1, b1, d2, b2 from tt1 left join tt2 --splice-properties index=idx_tt2, joinStrategy=MERGE, useSpark=false\n on d2 = 3000 and d1=d2 and b1=b2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "D1  |B1 | D2  | B2  |\n" +
                "----------------------\n" +
                "3000 |30 |3000 | 30  |\n" +
                "5000 |50 |NULL |NULL |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = sql = "select d1, b1, d2, b2 from tt1 left join tt2 --splice-properties index=idx_tt2, joinStrategy=MERGE, useSpark=true\n on d2 = 3000 and d1=d2 and b1=b2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTaleScanKeyCombiningGEStartKeyAndHashColumnValueCoveringIndexLeftJoin2() throws Exception {
         /* idx2_tt2 (a2, c2) is used, idx2_tt2 has index columns whose positions in the base table are in the same order, so there is no projectrestrict operation
           on top of the IndexScan, we can take advantage of the first qualified left on the join column to further restrict the scan of right table
          */
        String sql = "select a1, c1, a2, c2 from tt1 left join tt2 --splice-properties index=idx2_tt2, joinStrategy=MERGE, useSpark=false\n on a2 > 2 and a1=a2 and c1=c2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "A1 |C1  |A2 |C2  |\n" +
                "------------------\n" +
                " 3 |300 | 3 |300 |\n" +
                " 5 |500 | 5 |500 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = "select a1, c1, a2, c2 from tt1 left join tt2 --splice-properties index=idx2_tt2, joinStrategy=MERGE, useSpark=true\n on a2 > 2 and a1=a2 and c1=c2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightTaleScanKeyCombiningEqualStartKeyAndHashColumnValueCoveringIndexLeftJoin2() throws Exception {
         /* idx2_tt2 (a2, c2) is used, idx2_tt2 has index columns whose positions in the base table are in the same order, so there is no projectrestrict operation
           on top of the IndexScan, we can take advantage of the first qualified left on the join column to further restrict the scan of right table
          */
        String sql = "select a1, c1, a2, c2 from tt1 left join tt2 --splice-properties index=idx2_tt2, joinStrategy=MERGE, useSpark=false\n on a2 = 3 and a1=a2 and c1=c2";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "A1 |C1  | A2  | C2  |\n" +
                "---------------------\n" +
                " 3 |300 |  3  | 300 |\n" +
                " 5 |500 |NULL |NULL |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //test spark path
        sql = sql = "select a1, c1, a2, c2 from tt1 left join tt2 --splice-properties index=idx2_tt2, joinStrategy=MERGE, useSpark=true\n on a2 = 3 and a1=a2 and c1=c2";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testRightIndexScan() throws Exception {
        String sql = "select count(*)\n" +
                "                from --splice-properties joinOrder=fixed\n" +
                "                tab1 --splice-properties index=tabi\n" +
                "                , tab2 --splice-properties joinStrategy=MERGE, index=tab2i, useSpark=true\n" +
                "                where tab1.a=1 and tab1.b=1 and tab1.b=tab2.c and tab2.a=1\n";

        ResultSet rs = methodWatcher.executeQuery(sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals("incorrect row count", 1, rs.getInt(1));
    }

    @Test
    // DB-5428
     public void testMergeFeasibilityNonColumnReferences() throws Exception {
        methodWatcher.executeUpdate(
         format("CREATE TABLE %s.states(name VARCHAR(30)NOT NULL, " +
                 "abbrev CHAR(2)NOT NULL, "+
         "statehood DATE, "+
         "pop BIGINT, "+
         "PRIMARY KEY (abbrev))",CLASS_NAME));
        methodWatcher.executeUpdate(format("insert into %s.states(name, abbrev, statehood, pop)" +
                " values('Alabama', 'AL', '1819-12-14', 4040587), ('Alaska', 'AK', '1959-01-03', 550043)",CLASS_NAME));
        methodWatcher.executeQuery(
                format("SELECT YEAR (s2.statehood) AS yr, s1.name, s2.name FROM %s.states AS s1, %s.states AS s2 WHERE YEAR " +
         "(s1.statehood) = YEAR(s2.statehood) AND s1.name != s2.name ORDER BY yr, s1.name, s2.name",CLASS_NAME,CLASS_NAME));
     }

     @Test
     public void testMergeFeasiblityWithNonEqualityJoinConditionsOnKeys() throws Exception {
        // Negative case 1 inner join + inequality condition on PK
         String sql = "explain select * from t3, t4 --splice-properties joinStrategy=MERGE\n where a3<>a4 and b3=b4";
         try {
             methodWatcher.executeQuery(sql);
             Assert.fail("The query should error out for merge join");
         } catch (SQLException e) {
             Assert.assertEquals("Unexpected failure: "+ e.getMessage(), e.getSQLState(), SQLState.LANG_NO_BEST_PLAN_FOUND);
         }

         // Negative case 2 outer join + inequality condition on PK
         sql = "explain select * from t3 left join t4 --splice-properties joinStrategy=MERGE\n on a3<>a4 and b3=b4";
         try {
             methodWatcher.executeQuery(sql);
             Assert.fail("The query should error out for merge join");
         } catch (SQLException e) {
             Assert.assertEquals("Unexpected failure: "+ e.getMessage(), e.getSQLState(), SQLState.LANG_NO_BEST_PLAN_FOUND);
         }

         // Negative case 3 inner join + inequality condition on leading index column
         sql = "explain select b3,c3,d3,b4,c4,d4 from t3, t4 --splice-properties joinStrategy=MERGE\n where b3<>b4 and c3=c4";
         try {
             methodWatcher.executeQuery(sql);
             Assert.fail("The query should error out for merge join");
         } catch (SQLException e) {
             Assert.assertEquals("Unexpected failure: "+ e.getMessage(), e.getSQLState(), SQLState.LANG_NO_BEST_PLAN_FOUND);
         }

         // Negative case 4 inner join + constant condition + inequality condition on leading index column
         sql = "explain select b3,c3,d3,b4,c4,d4 from t3, t4 --splice-properties joinStrategy=MERGE\n where b3=1 and b4=1 and c3<>c4 and d3=d4";
         try {
             methodWatcher.executeQuery(sql);
             Assert.fail("The query should error out for merge join");
         } catch (SQLException e) {
             Assert.assertEquals("Unexpected failure: "+ e.getMessage(), e.getSQLState(), SQLState.LANG_NO_BEST_PLAN_FOUND);
         }

         // Positive case 4 inner join + constant condition + inequality condition on least significant index column
         sql = "select b3,c3,d3,b4,c4,d4 from t3, t4 --splice-properties joinStrategy=MERGE\n where b3=1 and b4=1 and c3=c4 and d3<>d4";
         String expected = "";
         ResultSet rs = methodWatcher.executeQuery(sql);
         assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
         rs.close();
     }

    //@Test
    // DB-5227

    /*

    create table BASIC_FLATTEN1.outer1 (c1 int, c2 int, c3 int) ;
    create table BASIC_FLATTEN1.outer2 (c1 int, c2 int, c3 int);
    create table BASIC_FLATTEN1.noidx (c1 int) ;
    create table BASIC_FLATTEN1.idx1 (c1 int) ;
    create table BASIC_FLATTEN1.idx2 (c1 int, c2 int) ;
    create table BASIC_FLATTEN1.nonunique_idx1 (c1 int) ;
create unique index BASIC_FLATTEN1.idx1_1 on idx1(c1) ;
create unique index BASIC_FLATTEN1.idx2_1 on idx2(c1, c2) ;
create index BASIC_FLATTEN1.nonunique_idx1_1 on nonunique_idx1(c1) ;
insert into BASIC_FLATTEN1.outer1 values (1, 2, 3) ;
insert into BASIC_FLATTEN1.outer1 values (4, 5, 6) ;
insert into BASIC_FLATTEN1.outer2 values (1, 2, 3) ;
insert into BASIC_FLATTEN1.outer2 values (4, 5, 6) ;
insert into BASIC_FLATTEN1.noidx values 1, 1 ;
insert into idx1 values 1, 2 ;
insert into idx2 values (1, 1), (1, 2) ;
insert into nonunique_idx1 values 1, 1 ;

select * from BASIC_FLATTEN1.outer1 where exists
(select * from BASIC_FLATTEN1.idx2 , BASIC_FLATTEN1.idx1 where BASIC_FLATTEN1.outer1.c1 + 0 = BASIC_FLATTEN1.idx2.c1 and BASIC_FLATTEN1.idx2.c2 + 0 = BASIC_FLATTEN1.idx1.c1 and BASIC_FLATTEN1.idx2.c2 = 1 + 0);

     */


    private static String getResource(String name) {
        return getResourceDirectory() + "tcph/data/" + name;
    }

}
