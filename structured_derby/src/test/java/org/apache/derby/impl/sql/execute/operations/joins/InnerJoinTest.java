package org.apache.derby.impl.sql.execute.operations.joins;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class InnerJoinTest extends SpliceUnitTest {

	private static Logger LOG = Logger.getLogger(InnerJoinTest.class);
	private static final Map<String,String> tableMap = Maps.newHashMap();

	public static final String CLASS_NAME = InnerJoinTest.class.getSimpleName().toUpperCase() + "_2";
	public static final String TABLE_NAME_1 = "A";
	public static final String TABLE_NAME_2 = "CC";
	public static final String TABLE_NAME_3 = "DD";

    protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(si varchar(40),sa character varying(40),sc varchar(40),sd int,se float)");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME,"(si varchar(40), sa varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3,CLASS_NAME,"(si varchar(40), sa varchar(40))");

	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
        .around(spliceTableWatcher2)
        .around(spliceTableWatcher3)
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
                        insertData("CC", "DD", spliceClassWatcher);
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
                        TestUtils.executeSqlFile(spliceClassWatcher, "small_msdatasample/startup.sql", CLASS_NAME);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {

                    try {
                        TestUtils.executeSqlFile(spliceClassWatcher, "test_data/employee.sql", CLASS_NAME);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

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
				Assert.assertNotNull(rs.getString(2));
				Assert.assertEquals(rs.getString(1),rs.getString(2));
			} else {
				Assert.assertNull(rs.getString(2));
			}
		}	
		Assert.assertEquals(9, j);
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
     public void testThreeTableJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select t1.orl_order_id, t2.cst_id, t3.itm_id " +
                "from order_line t1, customer t2, item t3 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id");

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
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id");

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

        Map first = results.get(0);
        Map second = results.get(1);

        Assert.assertEquals("10058_325_1", first.get("ORL_ORDER_ID"));
        Assert.assertEquals("Deutsch", first.get("CST_LAST_NAME"));
        Assert.assertEquals("Leslie", first.get("CST_FIRST_NAME"));
        Assert.assertEquals("Waiting to Exhale: The Soundtrack", first.get("ITM_NAME"));

        Assert.assertEquals("10058_7_1", second.get("ORL_ORDER_ID"));
        Assert.assertEquals("Deutsch", second.get("CST_LAST_NAME"));
        Assert.assertEquals("Leslie", second.get("CST_FIRST_NAME"));
        Assert.assertEquals("50 Favorite Rooms", second.get("ITM_NAME"));
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
                "where t1.itm_subcat_id = t2.sbc_id and t2.sbc_category_id = t3.cat_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(10, results.size());

        Assert.assertEquals("50 Favorite Rooms", results.get(0).get("ITM_NAME"));
        Assert.assertEquals("MicroStrategy Books", results.get(0).get("CAT_NAME"));
        Assert.assertEquals("Art & Architecture", results.get(0).get("SBC_DESC"));

        Assert.assertEquals("Seal (94)", results.get(5).get("ITM_NAME"));
        Assert.assertEquals("MicroStrategy Music", results.get(5).get("CAT_NAME"));
        Assert.assertEquals("Alternative", results.get(5).get("SBC_DESC"));

    }

    @Test
    public void testFourTableJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name, t4.sbc_desc " +
                "from order_line t1, customer t2, item t3, category_sub t4 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id" +
                "      and t3.itm_subcat_id = t4.sbc_id");

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

    @Ignore("Throws ArrayIndexOutOfBoundsException - logged as 333")
    @Test
    public void testFiveTableJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name, t5.cat_name, t4.sbc_desc " +
                "from order_line t1, customer t2, item t3, category_sub t4, category t5 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id" +
                "      and t3.itm_subcat_id = t4.sbc_id and t4.sbc_category_id = t5.cat_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(10, results.size());

    }

    @Ignore("Currently failing, written up as bug 338")
    @Test
    public void testSelfJoin() throws Exception {

        ResultSet rs = methodWatcher.executeQuery("select t1.cst_first_name, t1.cst_last_name, t2.cst_first_name as fn2, t2.cst_last_name as ln2, t1.cst_age_years " +
                "from customer t1, customer t2 " +
                "where t1.cst_age_years = t2.cst_age_years and t1.cst_id != t2.cst_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testThreeTableNestedLoopJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("SELECT STAFF.EMPNUM,WORKS.HOURS,UPUNIQ.COL2 FROM STAFF,WORKS,UPUNIQ\n" +
                "WHERE STAFF.EMPNUM = WORKS.EMPNUM AND UPUNIQ.COL2 = 'A'");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(12, results.size());

        Map<String,String> first = results.get(0);

        Assert.assertEquals("E1",results.get(0).get("EMPNUM"));
        Assert.assertEquals(new BigDecimal("40"), results.get(0).get("HOURS"));
        Assert.assertEquals("A",results.get(0).get("COL2"));

        Assert.assertEquals("E2",results.get(6).get("EMPNUM"));
        Assert.assertEquals(new BigDecimal("40"),results.get(6).get("HOURS"));
        Assert.assertEquals("A",results.get(6).get("COL2"));
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
        @Ignore("Bug 324")
	public void testSinkableCrossJoin() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc cross join dd group by cc.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("cc.si="+rs.getString(1)+",count="+rs.getLong(2));
			Assert.assertNotNull(rs.getString(1));
			Assert.assertEquals(9,rs.getLong(2));
		}	
		Assert.assertEquals(9, j);
	}	
	
	@Test
	public void testScrollableVarcharLeftOuterJoinWithJoinStrategy() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc left outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
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
		Assert.assertEquals(10, j);
	}

	@Test
	public void testSinkableVarcharLeftOuterJoinWithJoinStrategy() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc left outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si group by cc.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info(String.format("cc.sa=%s,count=%dd",rs.getString(1),rs.getInt(2)));
//			Assert.assertNotNull(rs.getString(1));
//			if (!rs.getString(1).equals("9")) {
//				Assert.assertEquals(1l,rs.getLong(2));
//			}
		}
		Assert.assertEquals(10, j);
	}

	@Test
	public void testReturnOutOfOrderJoin() throws Exception{
		ResultSet rs = methodWatcher.executeQuery("select cc.sa, dd.sa,cc.si from cc inner join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
		while(rs.next()){
			LOG.info(String.format("cc.sa=%s,dd.sa=%s",rs.getString(1),rs.getString(2)));
		}
	}

	@Test
	public void testScrollableInnerJoinWithJoinStrategy() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc inner join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
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
	public void testSinkableInnerJoinWithJoinStrategy() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc inner join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si group by cc.si");
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
	@Ignore ("Bug 325")
	public void testScrollableVarcharRightOuterJoinWithJoinStrategy() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc right outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("cc.si="+rs.getString(1)+",dd.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(2));
			if (!rs.getString(2).equals("9")) {
				Assert.assertNotNull(rs.getString(1));
				Assert.assertEquals(rs.getString(1),rs.getString(2));
			} else {
				Assert.assertNull(rs.getString(1));
			}
		}	
		Assert.assertEquals(9, j);
	}	
	@Test
	@Ignore ("Bug 325")
	public void testSinkableVarcharRightOuterJoinWithJoinStrategy() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc right outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si group by cc.si");
		int j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(1).equals("9")) {
				Assert.assertEquals(1l,rs.getLong(2));
			} else {
				Assert.assertNotNull(null);
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
	public void testScrollableVarcharLeftOuterJoin() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc left outer join dd on cc.si = dd.si");
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
		Assert.assertEquals(10, j);
	}		

	@Test
	public void testSinkableVarcharLeftOuterJoin() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc left outer join dd on cc.si = dd.si group by cc.si");
		int j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(1).equals("9")) {
				Assert.assertEquals(1l,rs.getLong(2));
			}
		}	
		Assert.assertEquals(10, j);
	}		
	
	@Test
	public void testScrollableVarcharRightOuterJoin() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, dd.si from cc right outer join dd on cc.si = dd.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("c.si="+rs.getString(1)+",d.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(2));
			if (!rs.getString(2).equals("9")) {
				Assert.assertNotNull(rs.getString(1));
				Assert.assertEquals(rs.getString(1),rs.getString(2));
			} else {
				Assert.assertNull(rs.getString(1));
			}
		}	
		Assert.assertEquals(9, j);
	}	
	@Test
	public void testSinkableVarcharRightOuterJoin() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select cc.si, count(*) from cc right outer join dd on cc.si = dd.si group by cc.si");
		int j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(1).equals("9")) {
				Assert.assertEquals(1l,rs.getLong(2));
			} else {
				Assert.assertNotNull(null);
			}
		}	
		Assert.assertEquals(9, j);
	}

}
