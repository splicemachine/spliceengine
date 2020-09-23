/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import splice.com.google.common.collect.ArrayListMultimap;
import splice.com.google.common.collect.Lists;
import splice.com.google.common.collect.Multimap;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.utils.Pair;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Tests for checking whether or not specific scans related to different data types work as expected.
 *
 * This is for tables with Timestamps, testing between clauses, etc.
 *
 * @author Scott Fines
 * Created: 1/31/13 9:17 AM
 */
public class TimeTableScanIT extends SpliceUnitTest { 
	private static final Logger LOG = Logger.getLogger(TimeTableScanIT.class);

	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = TimeTableScanIT.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "A";
	public static final String TABLE_NAME_2 = "B";
	
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(id varchar(40),ts timestamp, value float,date date)");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME,"(i int, s smallint)");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s (id, ts, value,date) values (?,?,?,?)", CLASS_NAME, TABLE_NAME_1));
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
							Date date = new Date(next.getTime());
							ps.setDate(4,date);
							ps.executeUpdate();
							resultsMap.put(id,new Pair<Timestamp, Float>(next,value));
						}
					}
					//insert data into ints table
					PreparedStatement intPs= spliceClassWatcher.prepareStatement(String.format("insert into %s.%s (i,s) values (?,?)",CLASS_NAME,TABLE_NAME_2));
					intPs.setInt(1,1956);
					intPs.setShort(2,(short)475);
					intPs.executeUpdate();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

	private static final int size =10;
	private static final Timestamp startTime = new Timestamp((2012-1900),1,1,0,0,0,0);
	private static final Timestamp stopTime = new Timestamp((2012-1900),1,2,0,0,0,0);
	private static final long INTERVAL = 60l*60*1000;
	private static final List<String> ids = Arrays.asList("a","b","c","d");
	private static final Multimap<String,Pair<Timestamp,Float>> resultsMap = ArrayListMultimap.create();
	@Test
	public void testGetBySmallInt() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select i from %s where s = 475", this.getTableReference(TABLE_NAME_2)));
		if(!rs.next())Assert.fail("No records returned!");
		Assert.assertEquals(1956,rs.getInt(1));
	}

	@Test
	public void testGetBetweenTimestamps() throws Exception{
		PreparedStatement ps = methodWatcher.prepareStatement(format("select id,ts,value from %s where ts between ? and ?",this.getTableReference(TABLE_NAME_1)));
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
		int correctNums = ids.size()*(int)((finish.getTime()-startTime.getTime())/INTERVAL);
		Assert.assertEquals("Incorrect rows returned!",correctNums,results.size());
	}

	@Test
	public void testCastScan() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select cast(ts as varchar(30)) from %s", this.getTableReference(TABLE_NAME_1)));
		Assert.assertTrue("SQL query did not return data!",rs.next());
	}

	@Test
	public void testCurrentTime() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select ts from %s where ts < CURRENT_TIMESTAMP", this.getTableReference(TABLE_NAME_1)));
		Timestamp now = new Timestamp(System.currentTimeMillis());
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			Timestamp ts = rs.getTimestamp(1);
			Assert.assertTrue("incorrect time returned!",ts.before(now));
			results.add(String.format("ts:%s",ts.toString()));
		}
		Assert.assertEquals("incorrect number of rows returned!",resultsMap.size(),results.size());
	}
}
