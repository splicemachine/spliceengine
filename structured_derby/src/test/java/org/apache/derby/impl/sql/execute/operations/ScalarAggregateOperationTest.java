package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import com.splicemachine.utils.SpliceLogUtils;

public class ScalarAggregateOperationTest {
	private static final Logger LOG = Logger.getLogger(ScalarAggregateOperationTest.class);

	private static final Map<String,String> tableSchemas = Maps.newHashMap();
	static {
		
	}
	
	@Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);
	public static int size = 10;
	public static Stats stats = new Stats();
	
	@BeforeClass
	public static void startup() throws Exception {
		DerbyTestRule.start();
		setupTest();
	}
	
	@AfterClass
	public static void shutdown() throws Exception {
		tearDownTest();
		DerbyTestRule.shutdown();
	}
	
	public static void setupTest() throws Exception {
		LOG.info("Setting up test");
		rule.createTable("t","i int");

		rule.setAutoCommit(false);
		PreparedStatement ps = rule.prepareStatement("insert into t (i) values (?)");
		for(int i=0;i<size;i++){
			ps.setInt(1,i);
			stats.add(i);
			ps.executeUpdate();
		}
		rule.commit();
		
		LOG.info("finished inserting data");
		
		//we want to make sure that we have at least 2 regions to operate against
		//we also want to be sure that the regions aren't the same size, so that
		//we aren't confused about which region returns if something is wrong.
		rule.splitTable("t",size/3);
	}

	public static void tearDownTest() throws Exception {
		LOG.info("Tearing down test");
		rule.dropTable("t");
	}

	@Test
	public void testCountOperation() throws Exception{
		ResultSet rs = rule.executeQuery("select count(*) from t");
		int count =0;
		while(rs.next()){
			Assert.assertEquals("incorrect count returned!",stats.getCount(),rs.getInt(1));
			count++;
		}
		Assert.assertEquals("incorrect number of rows returned!",1,count);
	}

	@Test
	public void testSumOperation() throws Exception {
		ResultSet rs = rule.executeQuery("select sum(i) from t");
		int i=0;
		while(rs.next()){
			Assert.assertEquals("Incorrect sum returned!",stats.getSum(),rs.getInt(1));
			i++;
		}
		//ensure that only a single row comes back
		Assert.assertEquals(1, i);
	}
	
	@Test
	public void testMinOperation() throws Exception {
		ResultSet rs = rule.executeQuery("select min(i) from t");
		int i=0;
		while(rs.next()){
			Assert.assertEquals("Incorrect min returned!",stats.getMin(),rs.getInt(1));
			i++;
		}
		//ensure that only a single row comes back
		Assert.assertEquals(1, i);
	}
	
	@Test
	public void testMaxOperation() throws Exception {
		ResultSet rs = rule.executeQuery("select max(i) from t");
		int i=0;
		while(rs.next()){
			Assert.assertEquals(stats.getMax(),rs.getInt(1));
			i++;
		}
		//ensure that only a single row comes back
		Assert.assertEquals(1, i);
	}
	
	@Test
	public void textAvgOperation() throws Exception {
		ResultSet rs = rule.executeQuery("select avg(i) from t");
		int i=0;
		while(rs.next()){
			Assert.assertEquals(stats.getAvg(),rs.getInt(1));
			i++;
		}
		//ensure that only a single row comes back
		Assert.assertEquals(1, i);
	}
	
	@Test
	public void testAllOperations() throws Exception {
		ResultSet rs = rule.executeQuery("select sum(i), avg(i), max(i), min(i) from t");
		int i=0;
		while(rs.next()){
			int sum = rs.getInt(1);
			int avg = rs.getInt(2);
			int max = rs.getInt(3);
			int min = rs.getInt(4);
			SpliceLogUtils.info(LOG, "sum=%d, avg=%d,max=%d,min=%d",sum,avg,max,min);
			Assert.assertEquals(stats.getSum(),sum);
			Assert.assertEquals(stats.getAvg(),avg);
			Assert.assertEquals(stats.getMax(),max);
			Assert.assertEquals(stats.getMin(),min);
			i++;
		}
		//ensure that only a single row comes back
		Assert.assertEquals(1, i);
	}
}
