package org.apache.derby.impl.sql.execute.operations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.splicemachine.derby.test.DerbyTestRule;
import junit.framework.Assert;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Tests for checking whether or not specific scans related to different data types work as expected.
 *
 * This is for tables with Timestamps, testing between clauses, etc.
 *
 * @author Scott Fines
 * Created: 1/31/13 9:17 AM
 */
public class DataTypeTableScanTest {
	private static final Logger LOG = Logger.getLogger(DataTypeTableScanTest.class);

	private static final Map<String,String> tableSchemas = Maps.newHashMap();
	private static final int size =10;
	private static final Timestamp startTime = new Timestamp((2012-1900),1,1,0,0,0,0);
	private static final Timestamp stopTime = new Timestamp((2012-1900),1,2,0,0,0,0);
	private static final long INTERVAL = 60l*60*1000;
	private static final List<String> ids = Arrays.asList("a","b","c","d");
	private static final Multimap<String,Pair<Timestamp,Float>> resultsMap = ArrayListMultimap.create();

	static{
		tableSchemas.put("times","id varchar(40),ts timestamp, value float");
		tableSchemas.put("ints","i int, s smallint");
	}

	@Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);

	@BeforeClass
	public static void startup() throws Exception{
		DerbyTestRule.start();
		setupTests();
	}

	@AfterClass
	public static void shutdown() throws Exception{
		tearDownTests();
		DerbyTestRule.shutdown();
	}

	private static void setupTests() throws Exception{
		rule.createTables();

		//insert data into times table
		PreparedStatement ps = rule.prepareStatement("insert into times (id, ts, value) values (?,?,?)");
		long stop = stopTime.getTime();
		Random random = new Random();
		for(String id:ids){
			ps.setString(1,id);
			long start = startTime.getTime();
			while(start < stop){
				start = start+INTERVAL;
				Timestamp next = new Timestamp(start);
				float value = random.nextFloat()*size;
				ps.setTimestamp(2,next);
				ps.setFloat(3, value);
				ps.executeUpdate();
				resultsMap.put(id,new Pair<Timestamp, Float>(next,value));
			}
		}

		//insert data into ints table
		PreparedStatement intPs= rule.prepareStatement("insert into ints (i,s) values (?,?)");
		intPs.setInt(1,1956);
		intPs.setShort(2,(short)475);
		intPs.executeUpdate();
	}

	private static void tearDownTests() throws Exception{
		rule.dropTables();
	}

	@Test
	public void testGetBySmallInt() throws Exception{
		/*
		 * regression test for Bug #208
		 */
//		PreparedStatement ps = rule.prepareStatement("select i from ints where s = ?");
//		ps.setShort(1,(short)475);
//		ResultSet rs = ps.executeQuery();
		ResultSet rs = rule.executeQuery("select i from ints where s = 475");
		if(!rs.next())Assert.fail("No records returned!");
		Assert.assertEquals(1956,rs.getInt(1));
	}

	@Test
//	@Ignore
	public void testGetBetweenTimestamps() throws Exception{
		PreparedStatement ps = rule.prepareStatement("select id,ts,value from times where ts between ? and ?");
		Timestamp finish = new Timestamp(startTime.getTime()+2*INTERVAL);
		ps.setTimestamp(1,startTime);
		ps.setTimestamp(2, finish);
		ResultSet rs = ps.executeQuery();
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			String id = rs.getString(1);
			Timestamp ts = rs.getTimestamp(2);
			Float value = rs.getFloat(3);

			Assert.assertNotNull("No id returned!", id);
			Assert.assertNotNull("no ts returned",ts);
			Assert.assertNotNull("no value returned",value);

			results.add(String.format("name:%s,ts:%s,value:%f",id,ts,value));
		}
		for(String result:results){
			LOG.info(result);
		}

		int correctNums = ids.size()*(int)((finish.getTime()-startTime.getTime())/INTERVAL);
		Assert.assertEquals("Incorrect rows returned!",correctNums,results.size());
	}

}
