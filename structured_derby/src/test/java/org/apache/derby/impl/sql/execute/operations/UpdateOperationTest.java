package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.common.collect.Lists;
import com.splicemachine.derby.test.DerbyTestRule;
import org.apache.log4j.Logger;
import org.junit.*;

public class UpdateOperationTest {
	private static Logger LOG = Logger.getLogger(UpdateOperationTest.class);

	private static final Map<String,String> tableSchemas = new HashMap<String, String>();
	static{
		tableSchemas.put("locations","num int, addr varchar(50), zip char(5)");
		//tableSchemas.put("t1","int_col int, smallint_col smallint, char_30_col char(30), varchar_50_col varchar(50)");
	}
	@Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,LOG);

	@BeforeClass
	public static void startup() throws Exception{
		DerbyTestRule.start();
	}
	@AfterClass
	public static void shutdown() throws Exception{
		DerbyTestRule.shutdown();
	}

	@Before
	public void setupTest() throws Exception{
		PreparedStatement insertPs = rule.prepareStatement("insert into locations values(?,?,?)");
		insertPs.setInt(1,100);
		insertPs.setString(2,"100");
		insertPs.setString(3, "94114");
		insertPs.executeUpdate();

		insertPs.setInt(1,200);
		insertPs.setString(2,"200");
		insertPs.setString(3, "94509");
		insertPs.executeUpdate();

		insertPs.setInt(1,300);
		insertPs.setString(2,"300");
		insertPs.setString(3, "34166");
		insertPs.executeUpdate();		
				

		
	}

	@Test
	public void testUpdate() throws SQLException {
		int updated= rule.getStatement().executeUpdate("update locations set addr='240' where num=100");
		Assert.assertEquals("Incorrect num rows updated!",1,updated);
		ResultSet rs = rule.executeQuery("select * from locations where num = 100");
		List<String> results = Lists.newArrayListWithCapacity(1);
		while(rs.next()){
			Integer num = rs.getInt(1);
			String addr = rs.getString(2);
			String zip = rs.getString(3);
			Assert.assertNotNull("no zip returned!",zip);
			Assert.assertEquals("Incorrect num returned!",100,num.intValue());
			Assert.assertEquals("Address incorrect","240",addr);
			results.add(String.format("num:%d,addr:%s,zip:%s",num,addr,zip));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect rows returned!",1,results.size());
	}

	@Test
	public void testUpdateMultipleColumns() throws Exception{
		int updated = rule.getStatement().executeUpdate("update locations set addr='900',zip='63367' where num=300");
		Assert.assertEquals("incorrect number of records updated!",1,updated);
		ResultSet rs = rule.executeQuery("select * from locations where num=300");
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			Integer num = rs.getInt(1);
			String addr = rs.getString(2);
			String zip = rs.getString(3);
			Assert.assertEquals("incorrect num!",new Integer(300),num);
			Assert.assertEquals("Incorrect addr!","900",addr);
			Assert.assertEquals("incorrect zip!","63367",zip);
			results.add(String.format("num:%d,addr:5s,zip:%s",num,addr,zip));
		}
		Assert.assertEquals("Incorrect rows returned!",1,results.size());
	}
}
