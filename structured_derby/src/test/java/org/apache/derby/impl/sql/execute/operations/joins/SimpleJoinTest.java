package org.apache.derby.impl.sql.execute.operations.joins;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.splicemachine.derby.test.DerbyTestRule;

/**
 * @author Scott Fines
 *         Created: 2/5/13 1:49 PM
 */
public class SimpleJoinTest {

	private static final Logger LOG = Logger.getLogger(SimpleJoinTest.class);
	private static Map<String, String> tableSchemas = Maps.newHashMap();

	private static final Map<Integer,Pair<String,String>> fTable = Maps.newHashMap();
	private static final Map<Integer,Pair<String,Integer>>	hTable = Maps.newHashMap();
	private static final Multimap<String,String> joinedResults = ArrayListMultimap.create();

	static{
		tableSchemas.put("k","id int, name varchar(50), zip char(5)");
		tableSchemas.put("h","id int, addr varchar(50),fid int");
	}

	@Rule
	public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);

	@BeforeClass
	public static void setup() throws Exception{
		DerbyTestRule.start();
		rule.createTables();
		insertData();
	}

	@AfterClass
	public static void shutdown() throws Exception{
		rule.dropTables();
		DerbyTestRule.shutdown();
	}

	private static void insertData() throws SQLException {
		PreparedStatement fps = rule.prepareStatement("insert into k values (?,?,?)");
		Pair<String,String> pair = Pair.newPair("scott", "65203");
		fps.setInt(1,1);
		fps.setString(2, pair.getFirst());
		fps.setString(3,pair.getSecond());
		fps.executeUpdate();
		fTable.put(1,pair);

		pair = Pair.newPair("jessie","94114");
		fps.setInt(1,2);
		fps.setString(2,pair.getFirst());
		fps.setString(3,pair.getSecond());
		fps.executeUpdate();
		fTable.put(2,pair);

		pair = Pair.newPair("john","63367");
		fps.setInt(1,3);
		fps.setString(2,pair.getFirst());
		fps.setString(3,pair.getSecond());
		fps.executeUpdate();
		fTable.put(3,pair);

		PreparedStatement hps = rule.prepareStatement("insert into h values (?,?,?)");
		Pair<String,Integer> hPair = Pair.newPair("141", 1);
		hps.setInt(1,1);
		hps.setString(2,hPair.getFirst());
		hps.setInt(3,hPair.getSecond());
		hps.executeUpdate();
		hTable.put(1,hPair);

		hPair = Pair.newPair("237",2);
		hps.setInt(1,2);
		hps.setString(2,hPair.getFirst());
		hps.setInt(3,hPair.getSecond());
		hps.executeUpdate();
		hTable.put(2,hPair);

		hPair = Pair.newPair("1912",1);
		hps.setInt(1,3);
		hps.setString(2,hPair.getFirst());
		hps.setInt(3,hPair.getSecond());
		hps.executeUpdate();
		hTable.put(3,hPair);

		hPair = Pair.newPair("713",3);
		hps.setInt(1,4);
		hps.setString(2,hPair.getFirst());
		hps.setInt(3,hPair.getSecond());
		hps.executeUpdate();
		hTable.put(4,hPair);

		for(Integer key: hTable.keySet()){
			Pair<String,Integer> row = hTable.get(key);
			Pair<String,String> fRow = fTable.get(row.getSecond());
			joinedResults.put(fRow.getFirst(),row.getFirst());
		}
	}

	@Test
	public void testDefaultJoin() throws Exception{
		ResultSet rs = rule.executeQuery("select k.name, h.addr from k join h on k.id = h.fid");
		List<String> results = Lists.newArrayList();
        Multimap<String,String> foundResults = ArrayListMultimap.create();
		while(rs.next()){
			String name = rs.getString(1);
			String address = rs.getString(2);
			Assert.assertNotNull("No name specified!",name);
			Assert.assertTrue("address does not belong to "+name,joinedResults.get(name).contains(address));
            Assert.assertTrue("Address "+address+" already found for "+name,!foundResults.get(name).contains(address));
            foundResults.put(name,address);
			results.add(String.format("name:%s,addr:%s",name,address));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect number of rows returned!",joinedResults.size(),results.size());

	}

	@Test
	public void testOuterJoin() throws Exception{
		ResultSet rs = rule.executeQuery("select k.name, h.addr from k left outer join h on k.id = h.fid");
		List<String> results = Lists.newArrayList();

		while(rs.next()){
			String name = rs.getString(1);
			String address = rs.getString(2);
			Assert.assertNotNull("No name specified!",name);
			Assert.assertTrue("incorrect address!",joinedResults.get(name).contains(address));
			results.add(String.format("name:%s,addr:%s",name,address));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect number of rows returned!",joinedResults.size(),results.size());

	}

}
