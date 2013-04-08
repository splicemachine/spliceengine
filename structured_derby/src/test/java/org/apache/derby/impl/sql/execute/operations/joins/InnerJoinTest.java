package org.apache.derby.impl.sql.execute.operations.joins;

import org.junit.Ignore;

import com.splicemachine.derby.test.framework.SpliceUnitTest;

@Ignore("Ryan")
public class InnerJoinTest extends SpliceUnitTest {
	/*
	private static Logger LOG = Logger.getLogger(InnerJoinTest.class);
	private static final Map<String,String> tableMap = Maps.newHashMap();

	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = InnerJoinTest.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "A";
	public static final String TABLE_NAME_2 = "B";
	
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(si varchar(40),sa character varying(40),sc varchar(40),sd int,se float)");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
				PreparedStatement ps = spliceClassWatcher.prepareStatement(format("insert into %s.%s (si, sa, sc,sd,se) values (?,?,?,?,?)",CLASS_NAME, TABLE_NAME));
				for (int i =0; i< 10; i++) {
					ps.setString(1, "" + i);
					ps.setString(2, "i");
					ps.setString(3, "" + i*10);
					ps.setInt(4, i);
					ps.setFloat(5,10.0f*i);
					ps.executeUpdate();
				}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

	
    static {
        tableMap.put("cc", "si varchar(40), sa varchar(40)");
        tableMap.put("dd", "si varchar(40), sa varchar(40)");
    }

	@BeforeClass
	public static void startup() throws Exception{
		DerbyTestRule.start();
		rule.createTables();
		insertData("cc","dd",rule);
        TestUtils.executeSqlFile(rule.getConnection(), "small_msdatasample/startup.sql");
	}
	
	@AfterClass
	public static void shutdown() throws Exception{
		rule.dropTables();
        TestUtils.executeSqlFile(rule.getConnection(), "small_msdatasample/shutdown.sql");
		DerbyTestRule.shutdown();
	}

	@Test
	public void testScrollableInnerJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select cc.si, dd.si from cc inner join dd on cc.si = dd.si");
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
	public void testSinkableInnerJoin() throws SQLException {			
		ResultSet rs = rule.executeQuery("select cc.si, count(*) from cc inner join dd on cc.si = dd.si group by cc.si");
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
     public void testThreeTableJoin() throws SQLException {
        ResultSet rs = rule.executeQuery("select t1.orl_order_id, t2.cst_id, t3.itm_id " +
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
    public void testThreeTableJoinExtraProjections() throws SQLException {
        ResultSet rs = rule.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
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
    public void testThreeTableJoinWithCriteria() throws SQLException {
        ResultSet rs = rule.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
                "from order_line t1, customer t2, item t3 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id" +
                "      and t2.cst_last_name = 'Deutsch'");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(2, results.size());
                                                                                       Si
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
    public void testThreeTableJoinWithSorting() throws SQLException {
        ResultSet rs1 = rule.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
                "from order_line t1, customer t2, item t3 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id " +
                "order by orl_order_id asc");

        ResultSet rs2 = rule.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
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

    @Ignore("Throws ArrayIndexOutOfBoundsException might be related to bug 333")
    @Test
    public void testThreeTableJoinOnItems() throws SQLException{
        ResultSet rs = rule.executeQuery("select t1.itm_name, t2.sbc_desc, t3.cat_name " +
                "from item t1, category_sub t2, category t3 " +
                "where t1.itm_subcat_id = t2.sbc_id and t2.sbc_category_id = t3.cat_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(10, results.size());


    }

    @Ignore("Throws ArrayIndexOutOfBoundsException - logged as 333")
    @Test
    public void testFourTableJoin() throws SQLException {
        ResultSet rs = rule.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name, t4.sbc_desc " +
                "from order_line t1, customer t2, item t3, category_sub t4 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id" +
                "      and t3.itm_subcat_id = t4.sbc_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(10, results.size());

    }

    @Ignore("Throws ArrayIndexOutOfBoundsException - logged as 333")
    @Test
    public void testFiveTableJoin() throws SQLException {
        ResultSet rs = rule.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name, t5.cat_name, t4.sbc_desc " +
                "from order_line t1, customer t2, item t3, category_sub t4, category t5 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id" +
                "      and t3.itm_subcat_id = t4.sbc_id and t4.sbc_category_id = t5.cat_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(10, results.size());

    }
<<<<<<< HEAD

    @Ignore("Currently failing, written up as bug 338")
    @Test
    public void testSelfJoin() throws SQLException {

        ResultSet rs = rule.executeQuery("select t1.cst_first_name, t1.cst_last_name, t2.cst_first_name as fn2, t2.cst_last_name as ln2, t1.cst_age_years " +
                "from customer t1, customer t2 " +
                "where t1.cst_age_years = t2.cst_age_years and t1.cst_id != t2.cst_id");

        List<Map> results = TestUtils.resultSetToMaps(rs);

        Assert.assertEquals(2, results.size());
    }

=======
	@Test
	public void testScrollableCrossJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select cc.si, dd.si from cc cross join dd");
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
	public void testSinkableCrossJoin() throws SQLException {			
		ResultSet rs = rule.executeQuery("select cc.si, count(*) from cc cross join dd group by cc.si");
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
	public void testScrollableVarcharLeftOuterJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select cc.si, dd.si from cc left outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
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
	public void testSinkableVarcharLeftOuterJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select cc.si, count(*) from cc left outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si group by cc.si");
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
	public void testReturnOutOfOrderJoin() throws SQLException{
		ResultSet rs = rule.executeQuery("select cc.sa, dd.sa,cc.si from cc inner join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
		while(rs.next()){
			LOG.info(String.format("cc.sa=%s,dd.sa=%s",rs.getString(1),rs.getString(2)));
		}
	}

	@Test
	public void testScrollableInnerJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select cc.si, dd.si from cc inner join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
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
	public void testSinkableInnerJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select cc.si, count(*) from cc inner join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si group by cc.si");
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
	public void testScrollableVarcharRightOuterJoin() throws SQLException {			
		ResultSet rs = rule.executeQuery("select cc.si, dd.si from cc right outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si");
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
	public void testSinkableVarcharRightOuterJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select cc.si, count(*) from cc right outer join dd --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on cc.si = dd.si group by cc.si");
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
	public void testScrollableNaturalJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select cc.si, dd.si from cc natural join dd");
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
	public void testSinkableNaturalJoin() throws SQLException {			
		ResultSet rs = rule.executeQuery("select cc.si, count(*) from cc natural join dd group by cc.si");
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
	public void testScrollableVarcharLeftOuterJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select f.si, g.si from f left outer join g on f.si = g.si");
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
	public void testSinkableVarcharLeftOuterJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select f.si, count(*) from f left outer join g on f.si = g.si group by f.si");
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
	public void testScrollableVarcharRightOuterJoin() throws SQLException {			
		ResultSet rs = rule.executeQuery("select f.si, g.si from f right outer join g on f.si = g.si");
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
	public void testSinkableVarcharRightOuterJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select f.si, count(*) from f right outer join g on f.si = g.si group by f.si");
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
	*/
}
