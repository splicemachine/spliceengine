package com.splicemachine.derby.impl.job.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

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
						.around(autoIncTableWatcher);

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

//    @After
//    public void tearDownTest() throws Exception{
//        rule.dropTables();
//    }

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
      testNewImport(spliceSchemaWatcher.schemaName,TABLE_2,getResourceDirectory()+"importdir","NAME,TITLE,AGE",getResourceDirectory()+"baddir",0,8);
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
            Assert.assertTrue("age was null!",!rs.wasNull());
            Assert.assertNotNull("Name is null!", name);
            Assert.assertNotNull("Title is null!", title);
            Assert.assertNotNull("Age is null!",age);
            results.add(String.format("name:%s,title:%s,age:%d",name,title,age));
        }
        Assert.assertTrue("no rows imported!",results.size()>0);
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
            Assert.assertTrue("age was null!",!rs.wasNull());
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
        		spliceSchemaWatcher.schemaName,TABLE_12,getResourceDirectory()+"dateAndTime.in",0,getResourceDirectory()+"baddir"));
        ps.execute();
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s.%s",spliceSchemaWatcher.schemaName,TABLE_12));
        List<String> results = Lists.newArrayList();
        System.out.println("=======================");
        System.out.println("About to output results");
        System.out.println("=======================");
        while(rs.next()){
            Date d = rs.getDate(1);
            Time t = rs.getTime(2);
            System.out.println("Date: " + d + ", Time: " + t);
            Assert.assertNotNull("Date is null!", d);
            Assert.assertNotNull("Time is null!", t);
            results.add(String.format("Date:%s,Time:%s",d,t));
        }
        System.out.println("=======================");
        System.out.println("Result Count: " + results.size());
        System.out.println("=======================");
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
		Assert.assertEquals("second row wrong","i:2,j:There", results.get(1));
	}

    @Test
	public void testHdfsImportGzipFile() throws Exception{
		testImport(spliceSchemaWatcher.schemaName,TABLE_6,getResourceDirectory()+"importTest.in.gz","NAME,TITLE,AGE");
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
			Assert.assertTrue("ItemId incorrect!",item_id>0);
			Assert.assertTrue("Order amt incorrect!",order_amt>0);
			Assert.assertNotNull("order_date incorrect",order_date);
			Assert.assertTrue("EmpId incorrect",emp_id>0);
			Assert.assertEquals("prom_id incorrect",0,prom_id);
			Assert.assertTrue("qty_sold incorrect",qty_sold>0);
			Assert.assertTrue("unit price incorrect!",unit_price>0);
			Assert.assertTrue("unit cost incorrect",unit_cost>0);
			Assert.assertEquals("discount incorrect",0.0f,discount,1/100f);
			Assert.assertTrue("cust_id incorrect",cust_id!=0);
			results.add(String.format("orderId:%s,item_id:%d,order_amt:%d,order_date:%s,emp_id:%d,prom_id:%d,qty_sold:%d," +
					"unit_price:%f,unit_cost:%f,discount:%f,cust_id:%d",orderId,item_id,order_amt,order_date,emp_id,prom_id,qty_sold,unit_price,unit_cost,discount,cust_id));
		}
		Assert.assertTrue("import failed!",results.size()>0);
	}
	
	@Test
	public void testImportISODateFormat() throws Exception{
		PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null,?" +
				",',','\"','yyyy-MM-dd''T''HH:mm:ss.SSS',null,null)",spliceSchemaWatcher.schemaName,TABLE_9));
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
		Assert.assertTrue("import failed!",results.size()==1);
	}

		@Test
		public void testImportCustomTimeFormatMillisWithTz() throws Exception{
				methodWatcher.executeUpdate("delete from "+spliceTableWatcher9);

				PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.SYSCS_IMPORT_DATA ('%s','%s',null,null,?" +
								",',','\"','yyyy-MM-dd hh:mm:ss.SSZ',null,null)",spliceSchemaWatcher.schemaName,TABLE_9));
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
						Assert.assertEquals("2013-04-21 09:21:24.098",textualFormat);
						results.add(String.format("order_date:%s",order_date));
				}
				Assert.assertTrue("import failed!",results.size()==1);
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
				Assert.assertTrue("import failed!",results.size()==1);
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
			Assert.assertEquals(i.byteValue(),0);
			Assert.assertEquals(j.byteValue(),0);
			Assert.assertNull("String failure " + k,k);
			Assert.assertNull("Timestamp failure " + l,l);
			count++;
		}
		Assert.assertTrue("import failed!" + count,count==1);
	}

    @Test
	public void testHdfsImportNullColList() throws Exception{
		testImport(spliceSchemaWatcher.schemaName,TABLE_7,getResourceDirectory()+"importTest.in",null);
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
		colNameToTypeMap.put("AUTHORIZATIONID",Types.VARCHAR);
		int count=0;
		while(rs.next()){
			String colName = rs.getString(4);
			int  colType = rs.getInt(5);
			Assert.assertTrue("ColName not contained in map: "+ colName,
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
        ps.setString(1,"SYS");
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while(rs.next()){
        	count++;
        }
        Assert.assertTrue("At least one row returned", count>0);
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
            System.out.println("i = " + i);
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
            System.out.println("i = " + i);
            Assert.assertEquals(i, 1);
        }
    }

    
	
}
