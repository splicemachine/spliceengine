package org.apache.derby.impl.sql.execute.operations.joins;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.AnnotationIntrospector;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static com.splicemachine.homeless.TestUtils.o;
import static com.splicemachine.homeless.TestUtils.resultSetToMaps;


public class InnerJoinIT extends SpliceUnitTest { 

	private static Logger LOG = Logger.getLogger(InnerJoinIT.class);
	private static final Map<String,String> tableMap = Maps.newHashMap();

	public static final String CLASS_NAME = InnerJoinIT.class.getSimpleName().toUpperCase()+ "_2"; 
	public static final String TABLE_NAME_1 = "A";
	public static final String TABLE_NAME_2 = "CC";
	public static final String TABLE_NAME_3 = "DD";
	public static final String TABLE_NAME_4 = "D";
	public static final String TABLE_NAME_5 = "E";
	public static final String TABLE_NAME_6 = "F";
	public static final String TABLE_NAME_7 = "G";

	

    protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(si varchar(40),sa character varying(40),sc varchar(40),sd int,se float)");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME,"(si varchar(40), sa varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3,CLASS_NAME,"(si varchar(40), sa varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4,CLASS_NAME,"(a varchar(20), b varchar(20), c varchar(10), d decimal, e varchar(15))");
	protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5,CLASS_NAME,"(a varchar(20), b varchar(20), w decimal(4),e varchar(15))");
	protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6,CLASS_NAME,"(a varchar(20), b varchar(20), c varchar(10), d decimal, e varchar(15))");
	protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE_NAME_7,CLASS_NAME,"(a varchar(20), b varchar(20), w decimal(4),e varchar(15))");

	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
        .around(spliceTableWatcher2)
        .around(spliceTableWatcher3)
        .around(spliceTableWatcher4)
        .around(spliceTableWatcher5)
        .around(spliceTableWatcher6)
        .around(spliceTableWatcher7)
		.around(new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {
                try {
                    PreparedStatement ps = spliceClassWatcher.prepareStatement(format("insert into %s (si, sa, sc,sd,se) values (?,?,?,?,?)", TABLE_NAME_1));
                    for (int i = 0; i < 10; i++) {
                        ps.setString(1, "" + i);
                        ps.setString(2, "i");
                        ps.setString(3, "" + i * 10);
                        ps.setInt(4, i);
                        ps.setFloat(5, 10.0f * i);
                        ps.executeUpdate();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    spliceClassWatcher.closeAll();
                }
            }

        }).around(new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {
                try {
                	Statement statement = spliceClassWatcher.getStatement();
                	statement.execute(String.format("insert into %s values  ('p1','mxss','design',10000,'deale')",TABLE_NAME_4));
                	statement.execute(String.format("insert into %s values  ('e2','alice',12,'deale')",TABLE_NAME_5));
                	statement.execute(String.format("insert into %s values  ('e3','alice',12,'deale')",TABLE_NAME_5));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    spliceClassWatcher.closeAll();
                }
            }

        }).around(new SpliceDataWatcher() {

                @Override
                protected void starting(Description description) {
                    try {
                        insertData(TABLE_NAME_2, TABLE_NAME_3, spliceClassWatcher);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            }).around(TestUtils.createFileDataWatcher(spliceClassWatcher, "small_msdatasample/startup.sql", CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/employee.sql", CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/hits.sql", CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/basic_join_dataset.sql", CLASS_NAME));

    @Rule
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    public static void insertData(String t1,String t2,SpliceWatcher spliceWatcher) throws Exception {
        PreparedStatement psC = spliceWatcher.prepareStatement("insert into "+t1+" values (?,?)");
        PreparedStatement psD = spliceWatcher.prepareStatement("insert into "+t2+" values (?,?)");
        for (int i =0; i< 10; i++) {
            psC.setString(1, "" + i);
            psC.setString(2, "i");
            psC.executeUpdate();
            if (i!=9) {
                psD.setString(1, "" + i);
                psD.setString(2, "i");
                psD.executeUpdate();
            }
        }
        spliceWatcher.commit();
    }

	@Test
	public void testScrollableInnerJoin() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc inner join dd on cc.si = dd.si");
		int j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(1).equals("9")) {
				Assert.assertNotNull("dd.si = null",rs.getString(2));
				Assert.assertEquals("dd.si != cc.si",rs.getString(1),rs.getString(2));
			} else {
				Assert.assertNull(rs.getString(2));
			}
		}	
		Assert.assertEquals(9, j);
	}

    @Test
    public void testRepeatedScrollableInnerJoin() throws Exception {
        for(int i=0;i<100;i++){
            System.out.println(i);
            testScrollableInnerJoin();
        }
    }

    @Test
	public void testSinkableInnerJoin() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc inner join dd on cc.si = dd.si group by cc.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("cc.si="+rs.getString(1)+",dd.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(2).equals("9")) {
				Assert.assertNotNull(rs.getString(1));
				Assert.assertEquals(1l,rs.getLong(2));
			} else {
				Assert.assertNull(rs.getString(1));
			}
		}	
		Assert.assertEquals(9, j);
    }

    @Test
    public void testTwoTableJoinWithNoResults() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("SELECT a.c1, a.c2, b.c1, b.c2 FROM table1 a JOIN table2 b ON a.c1 = b.c1 WHERE a.c1 < -1 " +
                "ORDER BY a.c1, a.c2, b.c1, b.c2");

        Assert.assertFalse("Should not return any results",rs.next());

    }

    @Test
    public void testMergeSortJoinOverIndexScan() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                "select c.schemaid from sys.systables c , sys.sysschemas s  --SPLICE-PROPERTIES joinStrategy=SORTMERGE \n" +
                        "where c.tablename like 'SYS%' and c.schemaid = s.schemaid");
        int values = 0;
        while (rs.next()) {
            values++;
        }
        Assert.assertTrue("Should return some values", values > 4);
    }

    @Test
    public void testProjectionOverMergeSortJoin() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("select 10*t1.GRADE as foo from STAFF t1 where t1.EMPNUM not in (select t2.EMPNUM from WORKS t2 where t1.EMPNUM = t2.EMPNUM)");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(new BigDecimal(130), results.get(0).get("FOO"));
    }

    @Test
     public void testThreeTableJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select t1.orl_order_id, t2.cst_id, t3.itm_id " +
                "from order_line t1, customer t2, item t3 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id order by t3.itm_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(10, results.size());

        Map zero = results.get(0);
        Map fifth = results.get(5);

        Assert.assertEquals("10058_7_1", zero.get("ORL_ORDER_ID"));
        Assert.assertEquals(143, zero.get("CST_ID"));
        Assert.assertEquals(7, zero.get("ITM_ID"));

        Assert.assertEquals("10059_274_1", fifth.get("ORL_ORDER_ID"));
        Assert.assertEquals(327, fifth.get("CST_ID"));
        Assert.assertEquals(274, fifth.get("ITM_ID"));
    }

    @Test
    public void testThreeTableJoinExtraProjections() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
                "from order_line t1, customer t2, item t3 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id " +
                "order by t3.itm_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(10, results.size());

        Map favRooms = results.get(0);
        Map seal = results.get(5);

        Assert.assertEquals("10058_7_1", favRooms.get("ORL_ORDER_ID"));
        Assert.assertEquals("Deutsch", favRooms.get("CST_LAST_NAME"));
        Assert.assertEquals("Leslie", favRooms.get("CST_FIRST_NAME"));
        Assert.assertEquals("50 Favorite Rooms", favRooms.get("ITM_NAME"));

        Assert.assertEquals("10059_274_1", seal.get("ORL_ORDER_ID"));
        Assert.assertEquals("Marko", seal.get("CST_LAST_NAME"));
        Assert.assertEquals("Shelby", seal.get("CST_FIRST_NAME"));
        Assert.assertEquals("Seal (94)", seal.get("ITM_NAME"));
    }

    @Test
    public void testThreeTableJoinWithCriteria() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
                "from order_line t1, customer t2, item t3 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id" +
                "      and t2.cst_last_name = 'Deutsch'");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(2, results.size());


        int foundRows=0;
        for(int i=0;i<results.size();i++){
            Map value = results.get(i);
            if("10058_325_1".equals(value.get("ORL_ORDER_ID"))){
                foundRows++;
                Assert.assertEquals("Deutsch", value.get("CST_LAST_NAME"));
                Assert.assertEquals("Leslie", value.get("CST_FIRST_NAME"));
                Assert.assertEquals("Waiting to Exhale: The Soundtrack", value.get("ITM_NAME"));
            }else if("10058_7_1".equals(value.get("ORL_ORDER_ID"))){
                foundRows++;
                Assert.assertEquals("Deutsch", value.get("CST_LAST_NAME"));
                Assert.assertEquals("Leslie", value.get("CST_FIRST_NAME"));
                Assert.assertEquals("50 Favorite Rooms", value.get("ITM_NAME"));
            }else{
                Assert.fail("Unexpected row returned:"+value.get("ORL_ORDER_ID"));
            }
        }
        Assert.assertEquals("Incorrect number of rows returned",2,foundRows);

    }

    @Test
    public void testThreeTableJoinWithSorting() throws Exception {
        ResultSet rs1 = methodWatcher.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
                "from order_line t1, customer t2, item t3 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id " +
                "order by orl_order_id asc");

        ResultSet rs2 = methodWatcher.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
                "from order_line t1, customer t2, item t3 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id " +
                "order by orl_order_id desc");

        List<Map> results1 = TestUtils.resultSetToMaps(rs1);
        LinkedList<Map> results2 = new LinkedList<Map>(TestUtils.resultSetToMaps(rs2));

        Assert.assertEquals(10, results1.size());
        Assert.assertEquals(10, results2.size());

        Iterator<Map> it1 = results1.iterator();
        Iterator<Map> it2 = results2.descendingIterator();

        while( it1.hasNext() && it2.hasNext()){
            Assert.assertEquals(it1.next(), it2.next());
        }
    }

    @Test
    public void testThreeTableJoinOnItems() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("select t1.itm_name, t2.sbc_desc, t3.cat_name " +
                "from item t1, category_sub t2, category t3 " +
                "where t1.itm_subcat_id = t2.sbc_id and t2.sbc_category_id = t3.cat_id " +
                "order by t1.itm_name");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(10, results.size());

        Assert.assertEquals("50 Favorite Rooms", results.get(0).get("ITM_NAME"));
        Assert.assertEquals("MicroStrategy Books", results.get(0).get("CAT_NAME"));
        Assert.assertEquals("Art & Architecture", results.get(0).get("SBC_DESC"));

        Assert.assertEquals("Seal (94)", results.get(7).get("ITM_NAME"));
        Assert.assertEquals("MicroStrategy Music", results.get(7).get("CAT_NAME"));
        Assert.assertEquals("Alternative", results.get(7).get("SBC_DESC"));

    }

    @Test
    public void testThreeTableJoinOnItemsPredicateIsExpression() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("select t1.itm_name, t2.sbc_desc, t3.cat_name " +
                "from item t1, category_sub t2, category t3 " +
                "where (t1.itm_subcat_id + 1) = (t2.sbc_id + 1) and (t2.sbc_category_id + 1) = (t3.cat_id + 1)");

        List parseResults = TestUtils.resultSetToMaps(rs);
        List<Map<String,String>> results = (List<Map<String,String>>)parseResults;

        Assert.assertEquals(10, results.size());

        List<Map<String,String>> correctResults = Arrays.asList(
                makeMap(Pair.newPair("ITM_NAME","50 Favorite Rooms"),Pair.newPair("CAT_NAME","MicroStrategy Books"),Pair.newPair("SBC_DESC","Art & Architecture")),
                makeMap(Pair.newPair("ITM_NAME","Digital Filters"),Pair.newPair("CAT_NAME","MicroStrategy Books"),Pair.newPair("SBC_DESC","Science & Technology")),
                makeMap(Pair.newPair("ITM_NAME","Don't Sweat the Small Stuff"),Pair.newPair("CAT_NAME","MicroStrategy Books"),Pair.newPair("SBC_DESC","Business")),
                makeMap(Pair.newPair("ITM_NAME","Charlotte's Web"),Pair.newPair("CAT_NAME","MicroStrategy Movies"),Pair.newPair("SBC_DESC","Kids / Family")),
                makeMap(Pair.newPair("ITM_NAME","Seal (94)"),Pair.newPair("CAT_NAME","MicroStrategy Music"),Pair.newPair("SBC_DESC","Alternative")),
                makeMap(Pair.newPair("ITM_NAME","Blade"),Pair.newPair("CAT_NAME","MicroStrategy Movies"),Pair.newPair("SBC_DESC","Action")),
                makeMap(Pair.newPair("ITM_NAME","Take My Hand"),Pair.newPair("CAT_NAME","MicroStrategy Music"),Pair.newPair("SBC_DESC","Music - Miscellaneous")),
                makeMap(Pair.newPair("ITM_NAME","Cracked Rearview"),Pair.newPair("CAT_NAME","MicroStrategy Music"),Pair.newPair("SBC_DESC","Pop")),
                makeMap(Pair.newPair("ITM_NAME","Waiting to Exhale: The Soundtrack"),Pair.newPair("CAT_NAME","MicroStrategy Music"),Pair.newPair("SBC_DESC","Pop")),
                makeMap(Pair.newPair("ITM_NAME","Road Tested"),Pair.newPair("CAT_NAME","MicroStrategy Music"),Pair.newPair("SBC_DESC","Rock"))
        );
        Comparator<Map<String, String>> c = new Comparator<Map<String, String>>() {
            @Override
            public int compare(Map<String, String> o1, Map<String, String> o2) {
                if (o1 == null) {
                    if (o2 == null) return 0;
                    return 1;
                } else if (o2 == null) return -1;

                return o1.get("ITM_NAME").compareTo(o2.get("ITM_NAME"));
            }
        };
        Collections.sort(correctResults, c);
        Collections.sort(results,c);

        for(int i=0;i<correctResults.size();i++){
            Map<String,String> correct = correctResults.get(i);
            Map<String,String> actual = correctResults.get(i);
            Assert.assertEquals(correct,actual);
        }
//        Assert.assertEquals("50 Favorite Rooms", results.get(0).get("ITM_NAME"));
//        Assert.assertEquals("MicroStrategy Books", results.get(0).get("CAT_NAME"));
//        Assert.assertEquals("Art & Architecture", results.get(0).get("SBC_DESC"));
//
//        Assert.assertEquals("Seal (94)", results.get(4).get("ITM_NAME"));
//        Assert.assertEquals("MicroStrategy Music", results.get(4).get("CAT_NAME"));
//        Assert.assertEquals("Alternative", results.get(4).get("SBC_DESC"));

    }

    private static Map<String,String> makeMap(Pair<String,String> ...entries){
        Map<String,String> map = Maps.newHashMap();
        for(Pair<String,String> entry:entries){
            map.put(entry.getFirst(),entry.getSecond());
        }
        return map;
    }

    @Test
    public void testThreeTableJoinOnItemsWithCriteria() throws Exception{
        ResultSet rs = methodWatcher.executeQuery("select t1.itm_name, t2.sbc_desc, t3.cat_name " +
                "from item t1, category_sub t2, category t3 " +
                "where t1.itm_subcat_id = t2.sbc_id and t2.sbc_category_id = t3.cat_id " +
                "and t1.itm_name = 'Seal (94)'");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(1, results.size());

        Assert.assertEquals("Seal (94)", results.get(0).get("ITM_NAME"));
        Assert.assertEquals("MicroStrategy Music", results.get(0).get("CAT_NAME"));
        Assert.assertEquals("Alternative", results.get(0).get("SBC_DESC"));

    }

    @Test
    public void testFourTableJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name, t4.sbc_desc " +
                "from order_line t1, customer t2, item t3, category_sub t4 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id" +
                "      and t3.itm_subcat_id = t4.sbc_id order by t4.sbc_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(10, results.size());

        Assert.assertEquals("50 Favorite Rooms", results.get(0).get("ITM_NAME"));
        Assert.assertEquals("Leslie", results.get(0).get("CST_FIRST_NAME"));
        Assert.assertEquals("Deutsch", results.get(0).get("CST_LAST_NAME"));
        Assert.assertEquals("Art & Architecture", results.get(0).get("SBC_DESC"));
        Assert.assertEquals("10058_7_1", results.get(0).get("ORL_ORDER_ID"));

        Assert.assertEquals("Seal (94)", results.get(5).get("ITM_NAME"));
        Assert.assertEquals("Shelby", results.get(5).get("CST_FIRST_NAME"));
        Assert.assertEquals("Marko", results.get(5).get("CST_LAST_NAME"));
        Assert.assertEquals("Alternative", results.get(5).get("SBC_DESC"));
        Assert.assertEquals("10059_274_1", results.get(5).get("ORL_ORDER_ID"));


    }

    @Test
    public void testFiveTableJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name, t5.cat_name, t4.sbc_desc " +
                "from order_line t1, customer t2, item t3, category_sub t4, category t5 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id" +
                "      and t3.itm_subcat_id = t4.sbc_id and t4.sbc_category_id = t5.cat_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(10, results.size());

    }


    @Test
    public void testSelfJoin() throws Exception {

        ResultSet rs = methodWatcher.executeQuery("select t1.cst_first_name, t1.cst_last_name, t2.cst_first_name as fn2, t2.cst_last_name as ln2, t1.cst_age_years " +
                "from customer t1, customer t2 " +
                "where t1.cst_age_years = t2.cst_age_years and t1.cst_id != t2.cst_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testJoinOnLeftColTwice() throws Exception {

        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT A2.* from %s A1, %s A2 where A1.si = A2.si and A1.si = A2.sc" , TABLE_NAME_1, TABLE_NAME_1));

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals("0", results.get(0).get("SI"));
        Assert.assertEquals("0", results.get(0).get("SC"));
    }

    @Test
    public void testThreeTableNestedLoopJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("SELECT STAFF.EMPNUM,WORKS.HOURS,UPUNIQ.COL2 FROM STAFF,WORKS,UPUNIQ\n" +
                "WHERE STAFF.EMPNUM = WORKS.EMPNUM AND UPUNIQ.COL2 = 'A'");

        List<Map> results = TestUtils.resultSetToMaps(rs);
        Collections.sort(results,new Comparator<Map>() {
            @Override
            public int compare(Map o1, Map o2) {
                if(o1==null){
                    if(o2==null) return 0;
                    return -1;
                }else if(o2==null)
                    return 1;

                String first = (String)o1.get("EMPNUM");
                String second = (String)o2.get("EMPNUM");
                int compare = first.compareTo(second);
                if(compare==0){
                    BigDecimal fHours = (BigDecimal)o1.get("HOURS");
                    BigDecimal sHours = (BigDecimal)o2.get("HOURS");
                    compare = fHours.compareTo(sHours);
                }
                return compare;
            }
        });

        Assert.assertEquals(12, results.size());

        Map<String,String> first = results.get(0);

        Assert.assertEquals("E1",results.get(0).get("EMPNUM"));
        Assert.assertEquals(new BigDecimal("12"), results.get(0).get("HOURS"));
        Assert.assertEquals("A",results.get(0).get("COL2"));

        Assert.assertEquals("E2",results.get(6).get("EMPNUM"));
        Assert.assertEquals(new BigDecimal("40"),results.get(6).get("HOURS"));
        Assert.assertEquals("A",results.get(6).get("COL2"));
    }

    @Test
    public void testAggregateSubtractionWithJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("SELECT MIN(PNAME) FROM PROJ, WORKS, STAFF WHERE PROJ.PNUM = WORKS.PNUM\n" +
                "AND WORKS.EMPNUM = STAFF.EMPNUM AND BUDGET - (GRADE * 600) IN (-4400, -1000, 4000)");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals("MXSS", results.get(0).get("1"));
    }

	@Test
	public void testScrollableCrossJoin() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc cross join dd");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("cc.si="+rs.getString(1)+",dd.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(1).equals("9")) {
				Assert.assertNotNull(rs.getString(1));
				Assert.assertNotNull(rs.getString(2));
			} else {
				Assert.assertTrue(!rs.getString(2).equals("9"));
			}
		}	
		Assert.assertEquals(90, j);
	}		
	
	@Test
	public void testSinkableCrossJoin() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc cross join dd group by cc.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("cc.si="+rs.getString(1)+",count="+rs.getLong(2));
			Assert.assertNotNull(rs.getString(1));
			Assert.assertEquals(9,rs.getLong(2));
        }
		Assert.assertEquals(10, j);
	}

    @Test
    public void testCrossOfCrossJoins() throws Exception {
        int expected = 625;
        // Purpose is to test NLJ with an NLJ on its right side
        ResultSet rs = methodWatcher.executeQuery("select * from " +
                "(select a.empname from staff a, staff b) f, " +
                "(select a.empnum from staff a, staff b) t");

        Assert.assertEquals(expected, resultSetSize(rs));

    }
	
	@Test
	public void testReturnOutOfOrderJoin() throws Exception{
		ResultSet rs = methodWatcher.executeQuery("select cc.sa, dd.sa,cc.si from cc inner join dd --SPLICE-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
		while(rs.next()){
			LOG.info(String.format("cc.sa=%s,dd.sa=%s",rs.getString(1),rs.getString(2)));
		}
	}

	@Test
	public void testScrollableInnerJoinWithJoinStrategy() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc inner join dd --SPLICE-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
		int j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull("cc.si is null!",rs.getString(1));
			if (!rs.getString(1).equals("9")) {
				Assert.assertNotNull("dd.si is null!",rs.getString(2));
				Assert.assertEquals("cc.si !=dd.si",rs.getString(1),rs.getString(2));
			} else {
				Assert.assertNull(rs.getString(2));
			}
		}
		Assert.assertEquals(9, j);
	}

	@Test
	public void testSinkableInnerJoinWithJoinStrategy() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc inner join dd --SPLICE-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si group by cc.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("cc.si="+rs.getString(1)+",dd.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(2).equals("9")) {
				Assert.assertNotNull(rs.getString(1));
				Assert.assertEquals(1l,rs.getLong(2));
			} else {
				Assert.assertNull(rs.getString(1));
			}
		}
		Assert.assertEquals(9, j);
	}

	@Test
	public void testScrollableNaturalJoin() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc natural join dd");
		int j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(1).equals("9")) {
				Assert.assertNotNull(rs.getString(2));
				Assert.assertEquals(rs.getString(1),rs.getString(2));
			} else {
				Assert.assertNull(rs.getString(2));
			}
		}	
		Assert.assertEquals(9, j);
	}		
	
	@Test
	public void testSinkableNaturalJoin() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc natural join dd group by cc.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("cc.si="+rs.getString(1)+",dd.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(2).equals("9")) {
				Assert.assertNotNull(rs.getString(1));
				Assert.assertEquals(1l,rs.getLong(2));
			} else {
				Assert.assertNull(rs.getString(1));
			}
		}	
		Assert.assertEquals(9, j);
	}	

	@Test
	public void testScalarNoValues() throws Exception {		
			ResultSet rs = methodWatcher.executeQuery(String.format("select a from %s where %s.e = (select %s.e from %s where a > 'e1' )"
				, TABLE_NAME_6,TABLE_NAME_6, TABLE_NAME_7,TABLE_NAME_7));
	}

    @Test
    public void testInWithCorrelatedSubQueryOrSemijoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select empnum from staff where empnum in " +
                                                    "(select works.empnum from works where staff.empnum = works.empnum)");
        List results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(4, results.size());
    }

    @Test
	public void testNotInWithCorrelatedSubQueryOrAntijoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select empnum from staff where empnum not in " +
                                                    "(select works.empnum from works where staff.empnum = works.empnum)");
        List results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void testNotExistsWithCorrelatedSubqueryAKAAntiJoin() throws Exception {
        List<Object[]> expected = Arrays.asList(o("E3"), o("E4"));

        ResultSet rs = methodWatcher.executeQuery("SELECT w1.empnum " +
                "FROM   works w1 " +
                "WHERE  w1.pnum = 'P2' " +
                "       AND NOT EXISTS (SELECT * " +
                "                       FROM   works w2 " +
                "                       WHERE  w2.empnum = w1.empnum " +
                "                              AND w2.pnum = 'P1') " +
                "ORDER  BY 1 ASC");
        List results = TestUtils.resultSetToArrays(rs);
        Assert.assertArrayEquals(expected.toArray(), results.toArray());
    }

    @Test
    public void testSimpleSelfJoin() throws Exception {
        List<Object[]> expected = Arrays.asList(
                // Single "Akron" row
                o("E5", "E5"),
                // "Deale" rows
                o("E1", "E1"),  o("E1", "E4"), o("E4", "E1"), o("E4", "E4"),
                // "Vienna" rows
                o("E2", "E2"), o("E2", "E3"), o("E3", "E2"), o("E3", "E3"));
        ResultSet rs = methodWatcher.executeQuery("select a.empnum, b.empnum from staff a inner join staff b on a.city = b.city order by a.city, a.empnum, b.empnum");
        List results = TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected.toArray(), results.toArray());

    }

    @Test
    public void testSelfJoinWithConstantProjection() throws Exception {
        List<Object[]> expected = Arrays.asList(
                o(1,2,"a"), o(1,3,"a"), o(1,4,"a"), o(1,6,"a"), o(1,8,"a"),
                o(2,3,"a"), o(2,4,"a"), o(2,6,"a"), o(2,8,"a"),
                o(3,4,"a"), o(3,6,"a"), o(3,8,"a"),
                o(4,6,"a"), o(4,8,"a"),
                o(6,8,"a"));

        ResultSet rs = methodWatcher.executeQuery("select a.numkey, b.numkey, 'a' from upuniq a inner join upuniq b on a.numkey < b.numkey order by a.numkey, b.numkey");
        List results = TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected.toArray(), results.toArray());
    }

    @Test
    public void testJoinUnionWithNotExists() throws Exception {
        List<Object[]> expected = Arrays.asList(
                o("Alice","P1",BigDecimal.valueOf(40)), o("Alice","P2",BigDecimal.valueOf(20)), o("Alice","P3",BigDecimal.valueOf(80)), o("Alice","P4",BigDecimal.valueOf(20)),o("Alice","P5",BigDecimal.valueOf(12)),o("Alice","P6",BigDecimal.valueOf(12)),
                o("Betty","P1",BigDecimal.valueOf(40)), o("Betty","P2",BigDecimal.valueOf(80)),
                o("Carmen","P2",BigDecimal.valueOf(20)),
                o("Don","P2",BigDecimal.valueOf(20)),o("Don","P4",BigDecimal.valueOf(40)),o("Don","P5",BigDecimal.valueOf(80))
        );
        ResultSet resultSet = methodWatcher.executeQuery("select empname,pnum,hours from staff,works where staff.empnum = works.empnum  union" +
                "     select empname,pnum,hours from staff,works  where not exists" +
                "     (select hours from works) order by empname, pnum");

        List result = TestUtils.resultSetToArrays(resultSet);

        Assert.assertArrayEquals(expected.toArray(),result.toArray());
    }

    @Test
    public void testJoinUnionWithNotExistsWithCriteria() throws Exception {
        List<Object[]> expected = Arrays.asList(
                o("Alice","P1",BigDecimal.valueOf(40)), o("Alice","P2",BigDecimal.valueOf(20)), o("Alice","P3",BigDecimal.valueOf(80)), o("Alice","P4",BigDecimal.valueOf(20)),o("Alice","P5",BigDecimal.valueOf(12)),o("Alice","P6",BigDecimal.valueOf(12)),
                o("Betty","P1",BigDecimal.valueOf(40)), o("Betty","P2",BigDecimal.valueOf(80)),
                o("Carmen","P2",BigDecimal.valueOf(20)),
                o("Don","P2",BigDecimal.valueOf(20)),o("Don","P4",BigDecimal.valueOf(40)),o("Don","P5",BigDecimal.valueOf(80)),
                o("Ed","P1",BigDecimal.valueOf(40)), o("Ed","P2",BigDecimal.valueOf(20)),o("Ed","P2",BigDecimal.valueOf(80)), o("Ed","P3",BigDecimal.valueOf(80)),o("Ed","P4",BigDecimal.valueOf(20)), o("Ed","P4",BigDecimal.valueOf(40)),o("Ed","P5",BigDecimal.valueOf(12)),o("Ed","P5",BigDecimal.valueOf(80)),o("Ed","P6",BigDecimal.valueOf(12))
        );
        ResultSet resultSet = methodWatcher.executeQuery("select empname,pnum,hours from staff,works where staff.empnum = works.empnum  union" +
                "     select empname,pnum,hours from staff,works  where not exists" +
                "     (select hours from works where staff.empnum = works.empnum) order by empname, pnum");

        List result = TestUtils.resultSetToArrays(resultSet);

        Assert.assertArrayEquals(expected.toArray(),result.toArray());
    }

    @Test
    public void testSelfJoinWithLessThanSelection() throws Exception {
        List<Object[]> expected = Arrays.asList(
                o("E1", "E4", "Alice", "Don"),
                o("E2", "E3", "Betty", "Carmen"));

        ResultSet rs = methodWatcher.executeQuery("select a.empnum, b.empnum, a.empname, b.empname from staff a join staff b on a.city = b.city where a.empnum < b.empnum " +
                "order by a.empnum");
        List results = TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected.toArray(), results.toArray());
    }

    @Test
    public void testSelfJoinWithGreaterThanSelection() throws Exception {
        List<Object[]> expected = Arrays.asList(
                o("E3", "E2", "Carmen", "Betty"),
                o("E4", "E1", "Don", "Alice"));

        ResultSet rs = methodWatcher.executeQuery("select a.empnum, b.empnum, a.empname, b.empname from staff a join staff b on a.city = b.city where a.empnum > b.empnum " +
                "order by a.empnum");
        List results = TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected.toArray(), results.toArray());
    }

    @Test
    public void testJoinOverAggregates() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select a.* from monthly_hits a join monthly_hits b --SPLICE-PROPERTIES joinStrategy=SORTMERGE \n" +
                "on a.month = b.month ");
        List results = TestUtils.resultSetToArrays(rs);

        Assert.assertEquals(108, results.size());
    }

    @Test
    public void testJoinWithConditionReferringToTwoTablesOnLeft() throws Exception {
        List<Object[]> expected = Arrays.asList(
                o(BigDecimal.valueOf(12)),
                o(BigDecimal.valueOf(20)),
                o(BigDecimal.valueOf(20)),
                o(BigDecimal.valueOf(40)),
                o(BigDecimal.valueOf(40)),
                o(BigDecimal.valueOf(80)));
        ResultSet rs = methodWatcher.executeQuery("select w.hours " +
                "from proj p inner join works w on p.pnum = w.pnum " +
                "inner join staff s on w.empnum = s.empnum and p.city = s.city " +
                "order by w.hours");
        List results = TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected.toArray(), results.toArray());

    }
}
