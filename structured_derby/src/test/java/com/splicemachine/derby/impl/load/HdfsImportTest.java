package com.splicemachine.derby.impl.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.*;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class HdfsImportTest {
	private static final Logger LOG = Logger.getLogger(HdfsImportTest.class);
	
	static final Map<String,String> tableSchemaMap = Maps.newHashMap();
	static{
		tableSchemaMap.put("t","name varchar(40), title varchar(40), age int");
        tableSchemaMap.put("pk_t","name varchar(40), title varchar(40), age int,PRIMARY KEY(name)");
		tableSchemaMap.put("order_detail","order_id VARCHAR(50), item_id INT, order_amt INT,order_date TIMESTAMP, emp_id INT, " +
				"promotion_id INT, qty_sold INT, unit_price FLOAT, unit_cost FLOAT, discount FLOAT, customer_id INT");
		tableSchemaMap.put("lu_cust_city","cust_city_id int, cust_city_name varchar(64), cust_state_id int");
		tableSchemaMap.put("hello_there","i int, j varchar(20)");
	}
	
	@Rule public DerbyTestRule rule = new DerbyTestRule(tableSchemaMap,LOG);

	@BeforeClass
	public static void start() throws Exception{
		DerbyTestRule.start();
	}
	
	@AfterClass
	public static void shutdown() throws Exception {
		DerbyTestRule.shutdown();
	}

    @After
    public void tearDownTest() throws Exception{
        rule.dropTables();
    }

	@Test
	public void testHdfsImport() throws Exception{
		String baseDir = System.getProperty("user.dir");
		testImport("t",getBaseDirectory()+"importTest.in","NAME,TITLE,AGE");
	}

    @Test
    public void testImportWithPrimaryKeys() throws Exception{
        testImport("pk_t",getBaseDirectory()+"importTest.in","NAME,TITLE,AGE");
    }

	private void testImport(String tableName,String location,String colList) throws Exception {
		HdfsImport.importData(rule.getConnection(), null, tableName.toUpperCase(), colList, location, ",","\"");

		ResultSet rs = rule.executeQuery("select * from "+tableName);
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
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertTrue("no rows imported!",results.size()>0);
	}

	@Test
	public void testImportHelloThere() throws Exception {
		String csvLocation = getBaseDirectory()+"hello_there.csv";
		String importQuery = String.format("call SYSCS_UTIL.SYSCS_IMPORT_DATA(null,'HELLO_THERE', null, null, '%s', ',', null, null)", csvLocation);
		PreparedStatement ps = rule.prepareStatement(importQuery);
		ps.execute();

		ResultSet rs = rule.executeQuery("select i, j from HELLO_THERE");
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			Integer i = rs.getInt(1);
			String j = rs.getString(2);
			Assert.assertNotNull("i is null!", i);
			Assert.assertNotNull("j is null!", j);
			results.add(String.format("i:%d,j:%s",i,j));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertTrue("wrong row count imported!",results.size()==2);
		Assert.assertEquals("first row wrong","i:1,j:Hello", results.get(0));
		Assert.assertEquals("second row wrong","i:2,j:There", results.get(1));
	}

    private String getBaseDirectory() {
        String dir = System.getProperty("user.dir");
        if(!dir.endsWith("structured_derby"))
            dir = dir+"/structured_derby";
        return dir+"/src/test/resources/";
    }


    @Test
	public void testHdfsImportGzipFile() throws Exception{
		testImport("t",getBaseDirectory()+"importTest.in.gz","NAME,TITLE,AGE");
	}


	@Test
	public void testImportFromSQL() throws Exception{
		PreparedStatement ps = rule.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA (null,'ORDER_DETAIL',null,null,?" +
				",',',null,null)");
        ps.setString(1,getBaseDirectory()+"order_detail_small.csv");
		ps.execute();

		ResultSet rs = rule.executeQuery("select * from order_detail");
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
		for(String result:results){
			LOG.info(result);
		}

		Assert.assertTrue("import failed!",results.size()>0);
	}

	@Test
	public void testHdfsImportNullColList() throws Exception{
		testImport("t",getBaseDirectory()+"importTest.in",null);
	}

    @Test
    public void testImportWithExtraTabDelimited() throws Exception{
        String location = getBaseDirectory()+"lu_cust_city.txt";
        PreparedStatement ps = rule.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA (null,'LU_CUST_CITY',null,null," +
                "'"+location+"',',',null,null)");
        ps.execute();

        ResultSet rs = rule.executeQuery("select * from lu_cust_city");
        List<String>results = Lists.newArrayList();
        while(rs.next()){
            int id = rs.getInt(1);
            String name = rs.getString(2);
            int stateId = rs.getInt(3);

            results.add(String.format("%d\t%s\t%d",id,name,stateId));
        }
        for(String result:results){
            LOG.info(result);
        }
    }

    @Test
    public void testImportTabDelimited() throws Exception{
        String location = getBaseDirectory()+"lu_cust_city_tab.txt";
        PreparedStatement ps = rule.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA (null,'LU_CUST_CITY',null,null," +
                "'"+location+"','\t',null,null)");
        ps.execute();

        ResultSet rs = rule.executeQuery("select * from lu_cust_city");
        List<String>results = Lists.newArrayList();
        while(rs.next()){
            int id = rs.getInt(1);
            String name = rs.getString(2);
            int stateId = rs.getInt(3);

            results.add(String.format("%d\t%s\t%d",id,name,stateId));
        }
        for(String result:results){
            LOG.info(result);
        }
    }
	
	@Test
	public void testCallScript() throws Exception{
		ResultSet rs = rule.getConnection().getMetaData().getColumns(null, "SYS","SYSSCHEMAS",null);
		Map<String,Integer>colNameToTypeMap = Maps.newHashMap();
		colNameToTypeMap.put("SCHEMAID",Types.CHAR);
		colNameToTypeMap.put("SCHEMANAME",Types.VARCHAR);
		colNameToTypeMap.put("AUTHORIZATIONID",Types.VARCHAR);
		try{
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
		}finally{
			if(rs!=null)rs.close();
		}
	}

    @Test
    public void testCallWithRestrictions() throws Exception{
        PreparedStatement ps = rule.prepareStatement("select schemaname,schemaid from sys.sysschemas where schemaname like ?");
        ps.setString(1,"SYS");
        ResultSet rs = ps.executeQuery();//rule.executeQuery("select schemaname,schemaid from sys.sysschemas where schemaname like 'SYS'");
        while(rs.next()){
            LOG.info("schemaid="+rs.getString(1)+",schemaname="+rs.getString(2));
//            LOG.info("schemaname="+rs.getString(1));
        }
    }



    @Test
    public void testDataIsAvailable() throws Exception{
        long conglomId = 352;
        ResultSet rs = rule.executeQuery("select * from sys.sysconglomerates");
        while(rs.next()){
            String tableId = rs.getString(2);
            long tconglomId = rs.getLong(3);
            LOG.info("tableId="+tableId+",conglomid="+tconglomId);
            if(tconglomId==conglomId){
                LOG.warn("getting table name for conglomid "+ conglomId);
	            rs.close();
	            rs = rule.executeQuery("select tablename,tableid from sys.systables");
                while(rs.next()){
                    if(tableId.equals(rs.getString(2))){
                        LOG.warn("ConglomeId="+conglomId+",tableName="+rs.getString(1));
                        break;
                    }
                }
                break;
            }
        }
        LOG.error("Bytes.toBytes(SYS)="+ Arrays.toString(Bytes.toBytes("SYS")));
    }
	
}
