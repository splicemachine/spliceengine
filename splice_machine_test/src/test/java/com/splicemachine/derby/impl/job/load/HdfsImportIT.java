package com.splicemachine.derby.impl.job.load;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SlowTest;

import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HdfsImportIT extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = HdfsImportIT.class.getSimpleName().toUpperCase();
	protected static String TABLE_1 = "A";
	protected static String TABLE_2 = "B";
	protected static String TABLE_3 = "C";
	protected static String TABLE_4 = "D";
	protected static String TABLE_5 = "E";
	protected static String TABLE_6 = "F";
	protected static String TABLE_7 = "G";
	protected static String TABLE_8 = "H";
	protected static String TABLE_9 = "I";
	protected static String TABLE_10 = "J";
	protected static String TABLE_11 = "K";
	protected static String TABLE_12 = "L";
	protected static String TABLE_13 = "M";	
	protected static String TABLE_14 = "N";	
	protected static String TABLE_15 = "O";	
	protected static String TABLE_16 = "P";	
	protected static String TABLE_17 = "Q";	
	protected static String TABLE_18 = "R";
	private static final String AUTO_INCREMENT_TABLE = "INCREMENT";

	
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_1,spliceSchemaWatcher.schemaName,"(name varchar(40), title varchar(40), age int)");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_2,spliceSchemaWatcher.schemaName,"(name varchar(40), title varchar(40), age int,PRIMARY KEY(name))");
	protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_3,spliceSchemaWatcher.schemaName,"(order_id VARCHAR(50), item_id INT, order_amt INT,order_date TIMESTAMP, emp_id INT, "+
															"promotion_id INT, qty_sold INT, unit_price FLOAT, unit_cost FLOAT, discount FLOAT, customer_id INT)");
	protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_4,spliceSchemaWatcher.schemaName,"(cust_city_id int, cust_city_name varchar(64), cust_state_id int)");
	protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_5,spliceSchemaWatcher.schemaName,"(i int, j varchar(20))");
	protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_6,spliceSchemaWatcher.schemaName,"(name varchar(40), title varchar(40), age int)");
	protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE_7,spliceSchemaWatcher.schemaName,"(name varchar(40), title varchar(40), age int)");
	protected static SpliceTableWatcher spliceTableWatcher8 = new SpliceTableWatcher(TABLE_8,spliceSchemaWatcher.schemaName,"(cust_city_id int, cust_city_name varchar(64), cust_state_id int)");
	protected static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher(TABLE_9,spliceSchemaWatcher.schemaName,"(order_date TIMESTAMP)");
	protected static SpliceTableWatcher spliceTableWatcher10 = new SpliceTableWatcher(TABLE_10,spliceSchemaWatcher.schemaName,"(i int, j float, k varchar(20), l TIMESTAMP)");
	protected static SpliceTableWatcher spliceTableWatcher11 = new SpliceTableWatcher(TABLE_11,spliceSchemaWatcher.schemaName,"(i int default 10, j int)");
	protected static SpliceTableWatcher spliceTableWatcher12 = new SpliceTableWatcher(TABLE_12,spliceSchemaWatcher.schemaName,"(d date, t time)");
	protected static SpliceTableWatcher spliceTableWatcher13 = new SpliceTableWatcher(TABLE_13,spliceSchemaWatcher.schemaName,
			"( CUSTOMER_PRODUCT_ID INTEGER NOT NULL PRIMARY KEY, SHIPPED_DATE TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP, SOURCE_SYS_CREATE_DTS TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP NOT NULL,SOURCE_SYS_UPDATE_DTS TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP NOT NULL,"+
							"SDR_CREATE_DATE TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP, SDR_UPDATE_DATE TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP,DW_SRC_EXTRC_DTTM TIMESTAMP WITH DEFAULT CURRENT_TIMESTAMP)");
	protected static SpliceTableWatcher spliceTableWatcher14 = new SpliceTableWatcher(TABLE_14,spliceSchemaWatcher.schemaName,
			"( C_CUSTKEY INTEGER NOT NULL PRIMARY KEY, C_NAME VARCHAR(25), C_ADDRESS VARCHAR(40), C_NATIONKEY INTEGER NOT NULL,"+
			"C_PHONE CHAR(15), C_ACCTBAL DECIMAL(15,2), C_MKTSEGMENT  CHAR(10), C_COMMENT VARCHAR(117))");
	protected static SpliceTableWatcher spliceTableWatcher15 = new SpliceTableWatcher(TABLE_15,spliceSchemaWatcher.schemaName,
			"( cUsToMeR_pRoDuCt_Id InTeGeR NoT NuLl PrImArY KeY, ShIpPeD_DaTe TiMeStAmP WiTh DeFaUlT CuRrEnT_tImEsTaMp, SoUrCe_SyS_CrEaTe_DtS TiMeStAmP WiTh DeFaUlT cUrReNt_TiMeStAmP NoT NuLl,sOuRcE_SyS_UpDaTe_DtS TiMeStAmP WiTh DeFaUlT cUrReNt_TiMeStAmP NoT NuLl,"+
							"SdR_cReAtE_dAtE tImEsTaMp wItH DeFaUlT CuRrEnT_tImEsTaMp, SdR_uPdAtE_dAtE TimEstAmp With deFauLT cuRRent_tiMesTamP,Dw_srcC_ExtrC_DttM TimEStamP WitH DefAulT CurrEnt_TimesTamp)");
	protected static SpliceTableWatcher spliceTableWatcher16 = new SpliceTableWatcher(TABLE_16,spliceSchemaWatcher.schemaName,"(id int, description varchar(1000), name varchar(10))");
	protected static SpliceTableWatcher spliceTableWatcher17 = new SpliceTableWatcher(TABLE_17,spliceSchemaWatcher.schemaName,"(name varchar(40), title varchar(40), age int,PRIMARY KEY(name))");
	protected static SpliceTableWatcher spliceTableWatcher18 = new SpliceTableWatcher(TABLE_18,spliceSchemaWatcher.schemaName,"(name varchar(40), title varchar(40), age int)");


	
	
	
		protected static SpliceTableWatcher autoIncTableWatcher = new SpliceTableWatcher(AUTO_INCREMENT_TABLE,spliceSchemaWatcher.schemaName,"(i int generated always as identity, j int)");

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
            .around(spliceTableWatcher8)
            .around(spliceTableWatcher9)
						.around(spliceTableWatcher10)
						.around(spliceTableWatcher11)
						.around(spliceTableWatcher12)
						.around(spliceTableWatcher13)
						.around(spliceTableWatcher14)
						.around(spliceTableWatcher15)
						.around(spliceTableWatcher16)
						.around(spliceTableWatcher17)
						.around(spliceTableWatcher18)
						.around(autoIncTableWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Rule
	public TemporaryFolder baddir = new TemporaryFolder();
	
//    @After
//    public void tearDownTest() throws Exception{
//        rule.dropTables();
//    }

	@BeforeClass
	public static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table HdfsImportIT.num_dt1 (i smallint, j int, k bigint, primary key(j))")
                .withInsert("insert into HdfsImportIT.num_dt1 values(?,?,?)")
                .withRows(rows(
                        row(4256, 42031, 87049),
                        row(1140, 30751, 791),
                        row(25, 81278, 975),
                        row(-54, 62648, 3115),
                        row(57, 21099, 1081),
                        row(1430, 68915, null),
                        row(49, 19765, null),
                        row(-31, 10610, null),
                        row(-47, 34483, 40801),
                        row(7694, 20015, 52662),
                        row(35, 14202, 80476),
                        row(9393, 61174, 68211),
                        row(7058, 75830, null),
                        row(302, 5770, 53257),
                        row(3567, 15812, null),
                        row(-71, 92497, 85),
                        row(6229, 65149, 1583),
                        row(-36, 53846, 9128),
                        row(57, 95839, null),
                        row(3832, 90042, 433),
                        row(4818, 1483, 71600),
                        row(4493, 31875, 75291),
                        row(58, 85771, 3383),
                        row(9477, 77588, null),
                        row(6150, 88770, null),
                        row(8755, 44597, null),
                        row(68, 51844, 29940),
                        row(5926, 74926, 90887),
                        row(6017, 45829, 146),
                        row(8053, 45192,null)))
                .withIndex("create index idx1 on HdfsImportIT.num_dt1(k)")
                .create();
	}
	@Test
	public void testHdfsImport() throws Exception{
		testImport(spliceSchemaWatcher.schemaName,TABLE_1,getResourceDirectory()+"importTest.in","NAME,TITLE,AGE");
	}

    @Test
//    @Ignore("Weird error, need to deal later -SF-")
    public void testImportWithPrimaryKeys() throws Exception{
        testImport(spliceSchemaWatcher.schemaName,TABLE_2,getResourceDirectory()+"importTest.in","NAME,TITLE,AGE");
    }
    
    
  @Test
  public void testNewImportDirectory() throws Exception{
	  // importdir has a subdirectory as well with files in it
      testNewImport(spliceSchemaWatcher.schemaName,TABLE_2,getResourceDirectory()+"importdir","NAME,TITLE,AGE",baddir.newFolder().getCanonicalPath(),0,8);
  }

  // more tests to write:
  // test bad records at threshold and beyond threshold

    private void testImport(String schemaName, String tableName,String location,String colList) throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s','%s',null, '%s',',',null,null,null,null)",schemaName,tableName,colList,location));
        ps.execute();
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s",schemaName,tableName));
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String name = rs.getString(1);
            String title = rs.getString(2);
            int age = rs.getInt(3);
            Assert.assertTrue("age was null!", !rs.wasNull());
            Assert.assertNotNull("Name is null!", name);
            Assert.assertNotNull("Title is null!", title);
            Assert.assertNotNull("Age is null!",age);
            results.add(String.format("name:%s,title:%s,age:%d",name,title,age));
        }
        Assert.assertTrue("no rows imported!", results.size() > 0);
    }
    
    // uses new syntax
    // removes rows from table before insertion
    // checks count at the end
    private void testNewImport(String schemaName, String tableName,String location,String colList,String badDir,int failErrorCount,int importCount) throws Exception {
		methodWatcher.executeUpdate("delete from "+schemaName + "." + tableName);
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s','%s','%s',',',null,null,null,null,%d,'%s')",
        		schemaName,tableName,colList,location,failErrorCount,badDir));
        ps.execute();
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s",schemaName,tableName));
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String name = rs.getString(1);
            String title = rs.getString(2);
            int age = rs.getInt(3);
            Assert.assertTrue("age was null!", !rs.wasNull());
            Assert.assertNotNull("Name is null!", name);
            Assert.assertNotNull("Title is null!", title);
            Assert.assertNotNull("Age is null!",age);
            results.add(String.format("name:%s,title:%s,age:%d",name,title,age));
        }
        Assert.assertTrue("Incorrect number of rows imported", results.size() == importCount);
        
    }
    
    @Test
    public void testAlternateDateAndTimeImport() throws Exception {
		methodWatcher.executeUpdate("delete from "+spliceSchemaWatcher.schemaName + "." + TABLE_12);
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s','%s',null,'%s',',',null,null,'MM/dd/yyyy','HH.mm.ss',%d,'%s')",
        		spliceSchemaWatcher.schemaName,TABLE_12,getResourceDirectory()+"dateAndTime.in",0,baddir.newFolder().getCanonicalPath()));
        ps.execute();
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s",spliceSchemaWatcher.schemaName,TABLE_12));
        List<String> results = Lists.newArrayList();
        
        while(rs.next()){
            Date d = rs.getDate(1);
            Time t = rs.getTime(2);
            Assert.assertNotNull("Date is null!", d);
            Assert.assertNotNull("Time is null!", t);
            results.add(String.format("Date:%s,Time:%s",d,t));
        }
        Assert.assertTrue("Incorrect number of rows imported", results.size() == 2);
        
    }



	@Test
//	@Ignore("Bug")
	public void testImportHelloThere() throws Exception {
		String csvLocation = getResourceDirectory()+"hello_there.csv";
		PreparedStatement ps =
                methodWatcher.prepareStatement(
                        format("call SYSCS_UTIL.SYSCS_IMPORT_DATA('%s','%s', null, null, '%s', ',', null, null,null,null)",
                                spliceSchemaWatcher.schemaName,TABLE_5,csvLocation));
		ps.execute();
		ResultSet rs = methodWatcher.executeQuery(format("select i, j from %s.%s order by i",spliceSchemaWatcher.schemaName,TABLE_5));
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			Integer i = rs.getInt(1);
			String j = rs.getString(2);
			Assert.assertNotNull("i is null!", i);
			Assert.assertNotNull("j is null!", j);
			results.add(String.format("i:%d,j:%s",i,j));
		}
		Assert.assertEquals("wrong row count imported!", 2, results.size());
		Assert.assertEquals("first row wrong","i:1,j:Hello", results.get(0));
		Assert.assertEquals("second row wrong", "i:2,j:There", results.get(1));
	}

    @Test
	public void testHdfsImportGzipFile() throws Exception{
		testImport(spliceSchemaWatcher.schemaName, TABLE_6, getResourceDirectory() + "importTest.in.gz", "NAME,TITLE,AGE");
	}

	@Test
	public void testImportFromSQL() throws Exception{
		PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null,?" +
				",',',null,null,null,null)",spliceSchemaWatcher.schemaName,TABLE_3));
        ps.setString(1,getResourceDirectory()+"order_detail_small.csv");
		ps.execute();

		ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", spliceSchemaWatcher.schemaName, TABLE_3));
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			String orderId = rs.getString(1);
			int item_id = rs.getInt(2);
			int order_amt = rs.getInt(3);
			Timestamp order_date = rs.getTimestamp(4);
			int emp_id = rs.getInt(5);
			int prom_id = rs.getInt(6);
			int qty_sold = rs.getInt(7);
			float unit_price = rs.getInt(8);
			float unit_cost = rs.getFloat(9);
			float discount = rs.getFloat(10);
			int cust_id = rs.getInt(11);
			Assert.assertNotNull("No Order Id returned!",orderId);
			Assert.assertTrue("ItemId incorrect!", item_id > 0);
			Assert.assertTrue("Order amt incorrect!", order_amt > 0);
			Assert.assertNotNull("order_date incorrect",order_date);
			Assert.assertTrue("EmpId incorrect", emp_id > 0);
			Assert.assertEquals("prom_id incorrect",0,prom_id);
			Assert.assertTrue("qty_sold incorrect", qty_sold > 0);
			Assert.assertTrue("unit price incorrect!", unit_price > 0);
			Assert.assertTrue("unit cost incorrect", unit_cost > 0);
			Assert.assertEquals("discount incorrect",0.0f,discount,1/100f);
			Assert.assertTrue("cust_id incorrect", cust_id != 0);
			results.add(String.format("orderId:%s,item_id:%d,order_amt:%d,order_date:%s,emp_id:%d,prom_id:%d,qty_sold:%d," +
					"unit_price:%f,unit_cost:%f,discount:%f,cust_id:%d",orderId,item_id,order_amt,order_date,emp_id,prom_id,qty_sold,unit_price,unit_cost,discount,cust_id));
		}
		Assert.assertTrue("import failed!", results.size() > 0);
	}
	
	@Test
	public void testImportISODateFormat() throws Exception{
		PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null,?" +
				",',','\"','yyyy-MM-dd''T''HH:mm:ss.SSS''Z''',null,null)",spliceSchemaWatcher.schemaName,TABLE_9));
        ps.setString(1,getResourceDirectory()+"iso_order_date.csv");
		ps.execute();

		ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", spliceSchemaWatcher.schemaName, TABLE_9));
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			Timestamp order_date = rs.getTimestamp(1);
			Assert.assertNotNull("order_date incorrect",order_date);
			Assert.assertEquals(order_date.toString(),"2013-06-06 15:02:48.0");
			results.add(String.format("order_date:%s",order_date));
		}
		Assert.assertTrue("import failed!", results.size() == 1);
	}

		@Test
		public void testImportCustomTimeFormatMillisWithTz() throws Exception{
				methodWatcher.executeUpdate("delete from "+spliceTableWatcher9);

				PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null,?" +
								",',','\"','yyyy-MM-dd hh:mm:ss.SSSZ',null,null)",spliceSchemaWatcher.schemaName,TABLE_9));
				ps.setString(1,getResourceDirectory()+"tz_ms_order_date.csv");
				ps.execute();

				ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", spliceSchemaWatcher.schemaName, TABLE_9));
				List<String> results = Lists.newArrayList();
				while(rs.next()){
						Timestamp order_date = rs.getTimestamp(1);
						Assert.assertNotNull("order_date incorrect",order_date);
						//have to deal with differing time zones here
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						sdf.setTimeZone(TimeZone.getTimeZone("America/Chicago"));
						String textualFormat = sdf.format(order_date);
						Assert.assertEquals("2013-04-21 09:21:24.980",textualFormat);
						results.add(String.format("order_date:%s",order_date));
				}
				Assert.assertTrue("import failed!", results.size() == 1);
		}
		@Test
		public void testImportCustomTimeFormat() throws Exception{
				methodWatcher.executeUpdate("delete from "+spliceTableWatcher9);

				PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null,?" +
								",',','\"','yyyy-MM-dd HH:mm:ssZ',null,null)",spliceSchemaWatcher.schemaName,TABLE_9));
				ps.setString(1,getResourceDirectory()+"tz_order_date.cs");
				ps.execute();

				ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", spliceSchemaWatcher.schemaName, TABLE_9));
				List<String> results = Lists.newArrayList();
				while(rs.next()){
						Timestamp order_date = rs.getTimestamp(1);
						Assert.assertNotNull("order_date incorrect",order_date);
						//have to deal with differing time zones here
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
						sdf.setTimeZone(TimeZone.getTimeZone("America/Chicago"));
						String textualFormat = sdf.format(order_date);
						Assert.assertEquals("2013-06-06 15:02:48.0",textualFormat);
						results.add(String.format("order_date:%s",order_date));
				}
				Assert.assertTrue("import failed!", results.size() == 1);
		}

		@Test
	public void testImportNullFields() throws Exception{
		PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null,?" +
				",',','\"',null,null,null)",spliceSchemaWatcher.schemaName,TABLE_10));
        ps.setString(1,getResourceDirectory()+"null_field.csv");
		ps.execute();

		ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", spliceSchemaWatcher.schemaName, TABLE_10));
		int count = 0;
		while(rs.next()){
			Integer i = rs.getInt(1);
			Float j = rs.getFloat(2);
			String k = rs.getString(3);
			Timestamp l = rs.getTimestamp(4);
			Assert.assertEquals(i.byteValue(), 0);
			Assert.assertEquals(j.byteValue(),0);
			Assert.assertNull("String failure " + k, k);
			Assert.assertNull("Timestamp failure " + l, l);
			count++;
		}
		Assert.assertTrue("import failed!" + count, count == 1);
	}

    @Test
	public void testHdfsImportNullColList() throws Exception{
		testImport(spliceSchemaWatcher.schemaName, TABLE_7, getResourceDirectory() + "importTest.in", null);
	}

    @Test
    public void testImportWithExtraTabDelimited() throws Exception{
        String location = getResourceDirectory()+"lu_cust_city.txt";
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null," +
                "'%s',',',null,null,null,null)", spliceSchemaWatcher.schemaName, TABLE_4,location));
        ps.execute();

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s",this.getTableReference(TABLE_4)));
        List<String>results = Lists.newArrayList();
        while(rs.next()){
            int id = rs.getInt(1);
            String name = rs.getString(2);
            int stateId = rs.getInt(3);

            results.add(String.format("%d\t%s\t%d",id,name,stateId));
        }
    }

    @Test
    public void testImportTabDelimited() throws Exception{
        String location = getResourceDirectory()+"lu_cust_city_tab.txt";
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null," +
                "'%s','\t',null,null,null,null)",spliceSchemaWatcher.schemaName,TABLE_8,location));
        ps.execute();

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s",spliceSchemaWatcher.schemaName,TABLE_8));
        List<String>results = Lists.newArrayList();
        while(rs.next()){
            int id = rs.getInt(1);
            String name = rs.getString(2);
            int stateId = rs.getInt(3);
            results.add(String.format("%d\t%s\t%d",id,name,stateId));
        }
    }
    
    @Test
    public void testImportTabDelimitedNullSeparator() throws Exception{
        String location = getResourceDirectory()+"lu_cust_city_tab.txt";
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null," +
                "'%s','\t','\0',null,null,null)",spliceSchemaWatcher.schemaName,TABLE_8,location));
        ps.execute();

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s",spliceSchemaWatcher.schemaName,TABLE_8));
        List<String>results = Lists.newArrayList();
        while(rs.next()){
            int id = rs.getInt(1);
            String name = rs.getString(2);
            int stateId = rs.getInt(3);
            results.add(String.format("%d\t%s\t%d",id,name,stateId));
        }
    }

	@Test
	public void testCallScript() throws Exception{
		ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getColumns(null, "SYS","SYSSCHEMAS",null);
		Map<String,Integer>colNameToTypeMap = Maps.newHashMap();
		colNameToTypeMap.put("SCHEMAID",Types.CHAR);
		colNameToTypeMap.put("SCHEMANAME",Types.VARCHAR);
		colNameToTypeMap.put("AUTHORIZATIONID", Types.VARCHAR);
		int count=0;
		while(rs.next()){
			String colName = rs.getString(4);
			int  colType = rs.getInt(5);
			Assert.assertTrue("ColName not contained in map: " + colName,
                    colNameToTypeMap.containsKey(colName));
			Assert.assertEquals("colType incorrect!",
								colNameToTypeMap.get(colName).intValue(),colType);
			count++;
		}
		Assert.assertEquals("incorrect count returned!",colNameToTypeMap.size(),count);
	}

    @Test
    public void testCallWithRestrictions() throws Exception{
        PreparedStatement ps = methodWatcher.prepareStatement("select schemaname,schemaid from sys.sysschemas where schemaname like ?");
        ps.setString(1, "SYS");
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while(rs.next()){
        	count++;
        }
        Assert.assertTrue("At least one row returned", count > 0);
    }

    @Test
    public void testDataIsAvailable() throws Exception{
        long conglomId = 352; // TODO What is the test?
        ResultSet rs = methodWatcher.executeQuery("select * from sys.sysconglomerates");
        while(rs.next()){
            String tableId = rs.getString(2);
            long tconglomId = rs.getLong(3);
            if(tconglomId==conglomId){
	            rs.close();
	            rs = methodWatcher.executeQuery("select tablename,tableid from sys.systables");
                while(rs.next()){
                    if(tableId.equals(rs.getString(2))){
                        break;
                    }
                }
                break;
            }
        }
    }
    
    @Test
    public void testImportTabWithDefaultColumnValue() throws Exception{
        String location = getResourceDirectory()+"default_column.txt";
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null," +
                "'%s',',',null,null,null,null)",spliceSchemaWatcher.schemaName,TABLE_11,location));
        ps.execute();

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s",spliceSchemaWatcher.schemaName,TABLE_11));
        while(rs.next()){
            int i = rs.getInt(1);
            //System.out.println("i = " + i);
            Assert.assertEquals(i, 10);
        }
    }

		@Test
    public void testImportTableWithAutoIncrementColumn() throws Exception{
        String location = getResourceDirectory()+"default_column.txt";
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null," +
                "'%s',',',null,null,null,null)",spliceSchemaWatcher.schemaName,AUTO_INCREMENT_TABLE,location));
        ps.execute();

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s",spliceSchemaWatcher.schemaName,AUTO_INCREMENT_TABLE));
        while(rs.next()){
            int i = rs.getInt(1);
            Assert.assertEquals(i, 1);
        }
    }

		
	@Test
	public void testNullDatesWithMillisecondAccuracy() throws Exception {
        String location = getResourceDirectory()+"datebug.tbl";
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null," +
                "'%s',',',null,'yyyy-MM-dd HH:mm:ss.SSSSSS',null,null)",spliceSchemaWatcher.schemaName,TABLE_13,location));
        ps.execute();
        ResultSet rs = methodWatcher.executeQuery(format("select SHIPPED_DATE from %s.%s",spliceSchemaWatcher.schemaName,TABLE_13));
        int i =0;
        while(rs.next()){
        	i++;
        	Assert.assertTrue("Date is still null", rs.getDate(1) != null);
        }
		Assert.assertEquals("10 Records not imported",10,i);
	}

	@Test
	public void testNullDatesWithMixedCaseAccuracy() throws Exception {
        String location = getResourceDirectory()+"datebug.tbl";
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null," +
                "'%s',',',null,'yyyy-MM-dd HH:mm:ss.SSSSSS',null,null)",spliceSchemaWatcher.schemaName,TABLE_15,location));
        ps.execute();
        ResultSet rs = methodWatcher.executeQuery(format("select SHIPPED_DATE from %s.%s",spliceSchemaWatcher.schemaName,TABLE_15));
        int i =0;
        while(rs.next()){
        	i++;
        	Assert.assertTrue("Date is still null", rs.getDate(1) != null);
        }
		Assert.assertEquals("10 Records not imported",10,i);
	}

	/**
	 * Import the data with Unix newlines (LF) terminating the records and without embedded newlines in the fields.
	 * @throws Exception
	 */
	@Test
	public void testImportWithUnixNewlines() throws Exception{
		testNewlineImport(spliceSchemaWatcher.schemaName, TABLE_16, getResourceDirectory() + "embedded-newlines/unix/newlines-absent.tsv", null, 0, baddir.newFolder().getCanonicalPath(), 3);
	}

	/**
	 * Import the data with embedded Unix newlines (LF) surrounded by double quotes.
	 * @throws Exception
	 */
	@Test
	public void testImportWithEmbeddedUnixNewlinesInsideDoubleQuotes() throws Exception{
		testNewlineImport(spliceSchemaWatcher.schemaName, TABLE_16, getResourceDirectory() + "embedded-newlines/unix/newlines-with-double-quotes.tsv", null, 0, baddir.newFolder().getCanonicalPath(), 3);
	}

	/**
	 * Import the data with embedded Unix newlines (LF) surrounded by single quotes.
	 * @throws Exception
	 */
	@Test
	public void testImportWithEmbeddedUnixNewlinesInsideSingleQuotes() throws Exception{
		testNewlineImport(spliceSchemaWatcher.schemaName, TABLE_16, getResourceDirectory() + "embedded-newlines/unix/newlines-with-single-quotes.tsv", "''", 0, baddir.newFolder().getCanonicalPath(), 3);
	}

	/**
	 * Import the data with Windows newlines (CR+LF) terminating the records and without embedded newlines in the fields.
	 * @throws Exception
	 */
	@Test
	public void testImportWithWindowsNewlines() throws Exception{
		testNewlineImport(spliceSchemaWatcher.schemaName, TABLE_16, getResourceDirectory() + "embedded-newlines/windows/newlines-absent.tsv", null, 0, baddir.newFolder().getCanonicalPath(), 3);
	}

	/**
	 * Import the data with embedded Windows newlines (CR+LF) surrounded by double quotes.
	 * @throws Exception
	 */
	@Test
	public void testImportWithEmbeddedWindowsNewlinesInsideDoubleQuotes() throws Exception{
		testNewlineImport(spliceSchemaWatcher.schemaName, TABLE_16, getResourceDirectory() + "embedded-newlines/windows/newlines-with-double-quotes.tsv", null, 0, baddir.newFolder().getCanonicalPath(), 3);
	}

	/**
	 * Import the data with embedded Windows newlines (CR+LF) surrounded by single quotes.
	 * @throws Exception
	 */
	@Test
	public void testImportWithEmbeddedWindowsNewlinesInsideSingleQuotes() throws Exception{
		testNewlineImport(spliceSchemaWatcher.schemaName, TABLE_16, getResourceDirectory() + "embedded-newlines/windows/newlines-with-single-quotes.tsv", "''", 0, baddir.newFolder().getCanonicalPath(), 3);
	}

	/**
	 * Import the data with Classic Mac newlines (CR) terminating the records and without embedded newlines in the fields.
	 * @throws Exception
	 */
	@Test
	public void testImportWithClassicMacNewlines() throws Exception{
		testNewlineImport(spliceSchemaWatcher.schemaName, TABLE_16, getResourceDirectory() + "embedded-newlines/classic-mac/newlines-absent.tsv", null, 0, baddir.newFolder().getCanonicalPath(), 3);
	}

	/**
	 * Import the data with embedded Classic Mac newlines (CR) surrounded by double quotes.
	 * @throws Exception
	 */
	@Test
	public void testImportWithEmbeddedClassicMacNewlinesInsideDoubleQuotes() throws Exception{
		testNewlineImport(spliceSchemaWatcher.schemaName, TABLE_16, getResourceDirectory() + "embedded-newlines/classic-mac/newlines-with-double-quotes.tsv", null, 0, baddir.newFolder().getCanonicalPath(), 3);
	}

	/**
	 * Import the data with embedded Classic Mac newlines (CR) surrounded by single quotes.
	 * @throws Exception
	 */
	@Test
	public void testImportWithEmbeddedClassicMacNewlinesInsideSingleQuotes() throws Exception{
		testNewlineImport(spliceSchemaWatcher.schemaName, TABLE_16, getResourceDirectory() + "embedded-newlines/classic-mac/newlines-with-single-quotes.tsv", "''", 0, baddir.newFolder().getCanonicalPath(), 3);
	}

	/**
	 * Tests import with different types of newlines (Unix, Windows, and Classic Mac) and
	 * with newlines embedded inside of values (strings) that are being imported.
	 * @param schemaName
	 * @param tableName
	 * @param location  the location of the data file
	 * @param charDelimiter
	 * @param failErrorCount
	 * @param badDir
	 * @param importCount
	 * @throws Exception
	 */
	private void testNewlineImport(String schemaName, String tableName, String location, String charDelimiter, int failErrorCount, String badDir, int importCount) throws Exception {
		methodWatcher.executeUpdate("delete from " + schemaName + "." + tableName);
		PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA('%s', '%s', null, '%s', '\t', %s, null, null, null, %d, '%s')",
				schemaName, tableName, location, (charDelimiter == null ? "null" : String.format("'%s'", charDelimiter)), failErrorCount, badDir));
		ps.execute();
		ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s",schemaName,tableName));
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			int id = rs.getInt(1);
			Assert.assertTrue("ID was null!", !rs.wasNull());
			String description = rs.getString(2);
			String name = rs.getString(3);
			Assert.assertNotNull("ID is null!", id);
			Assert.assertNotNull("DESCRIPTION is null!", description);
			Assert.assertNotNull("NAME is null!", name);
			results.add(String.format("id:%d,description:%s,name:%s",id, description, name));
		}
		Assert.assertEquals("Incorrect number of rows imported", importCount, results.size());
	}

	@Test
	public void testNewImportCheckDirectory() throws Exception{
		testNewImportCheck(spliceSchemaWatcher.schemaName, TABLE_17, getResourceDirectory() + "importdir", "NAME,TITLE,AGE", baddir.newFolder().getCanonicalPath(), 0, 0);
	}

	// uses new stored procedure
	// removes rows from table before insertion
	// checks count at the end
	private void testNewImportCheck(String schemaName, String tableName,String location,String colList,String badDir,int failErrorCount,int importCount) throws Exception {
		methodWatcher.executeUpdate("delete from "+schemaName + "." + tableName);
		PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_CHECK_DATA('%s','%s','%s','%s',',',null,null,null,null,%d,'%s',-1)",
				schemaName,tableName,colList,location,failErrorCount,badDir));
		ps.execute();
		ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s",schemaName,tableName));
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			String name = rs.getString(1);
			String title = rs.getString(2);
			int age = rs.getInt(3);
			Assert.assertTrue("age was null!", !rs.wasNull());
			Assert.assertNotNull("Name is null!", name);
			Assert.assertNotNull("Title is null!", title);
			Assert.assertNotNull("Age is null!",age);
			results.add(String.format("name:%s,title:%s,age:%d",name,title,age));
		}
		Assert.assertTrue("Incorrect number of rows imported", results.size() == importCount);
	}

	/**
	 * Tests an import scenario where a quoted column is missing the end quote and the EOF is
	 * reached before the maximum number of lines in a quoted column is exceeded.
	 *
	 * @throws Exception
	 */
	@Test
	public void testMissingEndQuoteForQuotedColumnEOF() throws Exception {
		/*
		 * PLEASE NOTE:
		 * I do not like this test.  It is nasty to fetch the root cause's message and substring match parts of it.
		 * However, since Derby and Splice wrap the exceptions so many times, I don't see any option to determine whether
		 * the correct exception is being thrown.
		 * I also do not see an expectRootCause method and the junit team has rejected the request for it a couple times:
		 *     https://github.com/junit-team/junit/issues/714
		 *     https://github.com/junit-team/junit/pull/778
		 */
		//try {
			testMissingEndQuoteForQuotedColumn(
					spliceSchemaWatcher.schemaName,
					TABLE_18,
					getResourceDirectory() + "import/missing-end-quote/employees.csv",
					"NAME,TITLE,AGE",
					baddir.newFolder().getCanonicalPath(),
					0,
					1); //6);
//		} catch (Throwable t) {
//			String expectedMessage1 = "unexpected end of file";
//			String expectedMessage2 = "org.supercsv.exception.SuperCsvException";
//			String rootCauseMessage = Throwables.getRootCause(t).getMessage();
//            if (! rootCauseMessage.contains("Intentional task invalidation")) {
//                Assert.assertTrue(
//                        String.format("Root cause message does not contain '%s' and '%s'.  Actual root cause message is '%s'.", expectedMessage1, expectedMessage2, rootCauseMessage),
//                        rootCauseMessage.contains(expectedMessage1) && rootCauseMessage.contains(expectedMessage2));
//            }
//        }
	}

	/**
	 * Tests an import scenario where a quoted column is missing the end quote and the
	 * maximum number of lines in a quoted column is exceeded.
	 *
	 * @throws Exception
	 */
	@Test
	public void testMissingEndQuoteForQuotedColumnMax() throws Exception {
		/*
		 * PLEASE NOTE:
		 * I do not like this test.  It is nasty to fetch the root cause's message and substring match parts of it.
		 * However, since Derby and Splice wrap the exceptions so many times, I don't see any option to determine whether
		 * the correct exception is being thrown.
		 * I also do not see an expectRootCause method and the junit team has rejected the request for it a couple times:
		 *     https://github.com/junit-team/junit/issues/714
		 *     https://github.com/junit-team/junit/pull/778
		 */
		//try {
			testMissingEndQuoteForQuotedColumn(
					spliceSchemaWatcher.schemaName,
					TABLE_18,
					getResourceDirectory() + "import/missing-end-quote/employeesMaxQuotedColumnLines.csv",
					"NAME,TITLE,AGE",
					baddir.newFolder().getCanonicalPath(),
					0,
					2); //201000);
//		} catch (Throwable t) {
//			String expectedMessage1 = "Quoted column beginning on line";
//			String expectedMessage2 = "org.supercsv.exception.SuperCsvException";
//			String rootCauseMessage = Throwables.getRootCause(t).getMessage();
//            if (! rootCauseMessage.contains("Intentional task invalidation")) {
//                Assert.assertTrue(
//                        String.format("Root cause message does not contain '%s' and '%s'.  Actual root cause message is '%s'.", expectedMessage1, expectedMessage2, rootCauseMessage),
//                        rootCauseMessage.contains(expectedMessage1) && rootCauseMessage.contains(expectedMessage2));
//            }
//        }
	}

    //DB-3685
    @Test
    public void testImportTableWithPKAndIndex() throws Exception {
        methodWatcher.executeUpdate("delete from HdfsImportIT.num_dt1");
        methodWatcher.execute(format("call syscs_util.import_data('HdfsImportIT', 'num_dt1', null, '%s', ',', null, null,null,null,1000,'/tmp/BAD')", getResourceDirectory() + "numdt1.2.gz"));
        methodWatcher.execute(format("call syscs_util.import_data('HdfsImportIT', 'num_dt1', null, '%s', ',', null, null,null,null,0,'/tmp/BAD')", getResourceDirectory() + "numdt1_12"));
        ResultSet rs = methodWatcher.executeQuery("select count(*) from HdfsImportIT.num_dt1 --SPLICE-PROPERTIES index=null");
        assertTrue(rs.next());
        int c1 = rs.getInt(1);
        rs = methodWatcher.executeQuery("select count(*) from HdfsImportIT.num_dt1 --SPLICE-PROPERTIES index=idx1");
        assertTrue(rs.next());
        int c2 = rs.getInt(1);
        assertTrue(c1==c2);
    }
	/**
	 * Worker method for import tests related to CSV files that are missing the end quote for a quoted column.
	 *
	 * @param schemaName
	 * @param tableName
	 * @param location
	 * @param colList
	 * @param badDir
	 * @param failErrorCount
	 * @param importCount
	 * @throws Exception
	 */
	private void testMissingEndQuoteForQuotedColumn(
			String schemaName, String tableName, String location, String colList, String badDir, int failErrorCount, int importCount)
					throws Exception {
		methodWatcher.executeUpdate("delete from " + schemaName + "." + tableName);
		PreparedStatement ps = methodWatcher.prepareStatement(
				format("call SYSCS_UTIL.IMPORT_DATA('%s','%s','%s','%s',',',null,null,null,null,%d,'%s')",
				schemaName, tableName, colList, location, failErrorCount, badDir));
		ps.execute();
		ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s", schemaName, tableName));
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			String name = rs.getString(1);
			String title = rs.getString(2);
			int age = rs.getInt(3);
			Assert.assertTrue("age was null!", !rs.wasNull());
			Assert.assertNotNull("name is null!", name);
			Assert.assertNotNull("title is null!", title);
			results.add(String.format("name:%s,title:%s,age:%d", name, title, age));
		}
		Assert.assertEquals("Incorrect number of rows imported", importCount, results.size());
	}
}
