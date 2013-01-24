package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import com.splicemachine.derby.test.SpliceDerbyTest;
import com.splicemachine.utils.SpliceLogUtils;

public class ScalarAggregateOperationTest {
	private static final Logger LOG = Logger.getLogger(ScalarAggregateOperationTest.class);

	private static final Map<String,String> tableSchemas = Maps.newHashMap();
	static {
		
	}
	
	@Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);
	
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
		ps.setInt(1,1);
		ps.executeUpdate();
		ps.setInt(1,2);
		ps.executeUpdate();
		ps.setInt(1,3);
		ps.executeUpdate();
		rule.commit();
		
		LOG.info("finished inserting data");
		
		//we want to make sure that we have at least 2 regions to operate against
//		rule.splitLastTable();
	}

	public static void tearDownTest() throws Exception {
		LOG.info("Tearing down test");
		rule.dropTable("t");
	}
	
	@Test
	public void testSumOperation() throws Exception {
		ResultSet rs = null;
		rs = rule.executeQuery("select sum(i) from t");
		int i=0;
		while(rs.next()){
			Assert.assertEquals(6,rs.getInt(1));
			i++;
		}
		//ensure that only a single row comes back
		Assert.assertEquals(1, i);
	}
	
	@Test
	public void testMinOperation() throws Exception {
		ResultSet rs = null;
		rs = rule.executeQuery("select min(i) from t");
		int i=0;
		while(rs.next()){
			Assert.assertEquals(1,rs.getInt(1));
			i++;
		}
		//ensure that only a single row comes back
		Assert.assertEquals(1, i);
	}
	
	@Test
	public void testMaxOperation() throws Exception {
		ResultSet rs = null;
		rs = rule.executeQuery("select max(i) from t");
		int i=0;
		while(rs.next()){
			Assert.assertEquals(3,rs.getInt(1));
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
			Assert.assertEquals(2,rs.getInt(1));
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
			Assert.assertEquals(6,sum);
			Assert.assertEquals(2,avg);
			Assert.assertEquals(3,max);
			Assert.assertEquals(1,min);
			i++;
		}
		//ensure that only a single row comes back
		Assert.assertEquals(1, i);
	}
}
