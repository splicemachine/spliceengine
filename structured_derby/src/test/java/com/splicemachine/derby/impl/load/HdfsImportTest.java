package com.splicemachine.derby.impl.load;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class HdfsImportTest {
	private static final Logger LOG = Logger.getLogger(HdfsImportTest.class);
	
	static final Map<String,String> tableSchemaMap = Maps.newHashMap();
	static{
		tableSchemaMap.put("t","name varchar(40), title varchar(40), age int");
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

	@Test
//    @Ignore
	public void testHdfsImport() throws Exception{
		String baseDir = System.getProperty("user.dir");
		String location = baseDir+"/structured_derby/src/test/resources/importTest.in";
		HdfsImport.importData(rule.getConnection(), null, "T", "NAME,TITLE,AGE", location,",");

		ResultSet rs = rule.executeQuery("select * from t");
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			String name = rs.getString(1);
			String title = rs.getString(2);
			Integer age = rs.getInt(3);
			Assert.assertNotNull("Name is null!",name);
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
