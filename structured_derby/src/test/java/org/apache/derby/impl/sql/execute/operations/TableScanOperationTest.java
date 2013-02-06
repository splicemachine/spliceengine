package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import org.apache.log4j.Logger;
import org.junit.*;

import com.splicemachine.derby.test.SpliceDerbyTest;

/**
 * This tests basic table scans with and without projection/restriction
 */
public class TableScanOperationTest {
	private static Logger LOG = Logger.getLogger(TableScanOperationTest.class);

	private static final Map<String,String> tableSchemas = Maps.newHashMap();
	static{
		tableSchemas.put("a","si varchar(40),sa character varying(40),sc varchar(40),sd int");
	}

	@Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);

	@BeforeClass 
	public static void startup() throws Exception {
		DerbyTestRule.start();
		rule.createTables();
		insertData();
	}

	private static void insertData() throws SQLException {
		PreparedStatement ps = rule.prepareStatement("insert into a (si, sa, sc,sd) values (?,?,?,?)");
		for (int i =0; i< 10; i++) {
			ps.setString(1, "" + i);
			ps.setString(2, "i");
			ps.setString(3, "" + i*10);
			ps.setInt(4,i);
			ps.executeUpdate();
		}
	}

	@AfterClass 
	public static void shutdown() throws Exception {
		rule.dropTables();
		DerbyTestRule.shutdown();
	}

	@Test
	public void testSimpleTableScan() throws SQLException {			
			ResultSet rs = rule.executeQuery("select * from a");
			int i = 0;
			while (rs.next()) {
				i++;
				LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2)+"c.si="+rs.getString(3));
				Assert.assertNotNull(rs.getString(1));
				Assert.assertNotNull(rs.getString(2));				
				Assert.assertNotNull(rs.getString(3));				
			}	
			Assert.assertEquals(10, i);
	}

	@Test
	public void testScanForNullEntries() throws SQLException{
		ResultSet rs = rule.executeQuery("select si from a where si is null");

		boolean hasRows = false;
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			hasRows=true;
			results.add(String.format("si=%s",rs.getString(1)));
		}

		if(hasRows){
			for(String row:results){
				LOG.info(row);
			}
			Assert.fail("Rows returned! expected 0 but was "+ results.size());
		}
	}

	@Test
	public void testQualifierTableScanPreparedStatement() throws SQLException {
		PreparedStatement stmt = rule.prepareStatement("select * from a where si = ?");
		stmt.setString(1,"5");
		ResultSet rs = stmt.executeQuery();
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2)+",c.si="+rs.getString(3));
			Assert.assertNotNull(rs.getString(1));
			Assert.assertNotNull(rs.getString(2));
			Assert.assertNotNull(rs.getString(3));
		}
		Assert.assertEquals(1, i);
	}

	@Test
	public void testQualifierTableScan() throws SQLException {
		ResultSet rs = rule.executeQuery("select * from a where si = '5'");
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2)+",c.si="+rs.getString(3));
			Assert.assertNotNull(rs.getString(1));
			Assert.assertNotNull(rs.getString(2));
			Assert.assertNotNull(rs.getString(3));
		}
		Assert.assertEquals(1, i);
	}

	@Test
	public void testRestrictedTableScan() throws SQLException{
		ResultSet rs = rule.executeQuery("select si,sc from a");
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("a.si="+rs.getString(1)+",c.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(1));
			Assert.assertNotNull(rs.getString(2));
		}
		Assert.assertEquals(10, i);
	}

	@Test
	public void testScanWithLessThanOperator() throws SQLException{
		ResultSet rs = rule.executeQuery("select  sd from a where sd < 5");
		List<String> results  = Lists.newArrayList();
		while(rs.next()){
			int sd = rs.getInt(1);
			Assert.assertTrue("incorrect sd returned!",sd<5);
			results.add(String.format("sd:%d",sd));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect rows returned!",5,results.size());
	}

	@Test
	public void testScanWithLessThanEqualOperator() throws SQLException{
		ResultSet rs = rule.executeQuery("select  sd from a where sd <= 5");
		List<String> results  = Lists.newArrayList();
		while(rs.next()){
			int sd = rs.getInt(1);
			Assert.assertTrue("incorrect sd returned!",sd<=5);
			results.add(String.format("sd:%d",sd));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect rows returned!",6,results.size());
	}

	@Test
	public void testScanWithGreaterThanOperator() throws SQLException{
		ResultSet rs = rule.executeQuery("select  sd from a where sd > 5");
		List<String> results  = Lists.newArrayList();
		while(rs.next()){
			int sd = rs.getInt(1);
			Assert.assertTrue("incorrect sd returned!",sd>5);
			results.add(String.format("sd:%d",sd));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect rows returned!",4,results.size());
	}

	@Test
	public void testScanWithGreaterThanEqualsOperator() throws SQLException{
		ResultSet rs = rule.executeQuery("select  sd from a where sd >= 5");
		List<String> results  = Lists.newArrayList();
		while(rs.next()){
			int sd = rs.getInt(1);
			Assert.assertTrue("incorrect sd returned!",sd>=5);
			results.add(String.format("sd:%d",sd));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect rows returned!",5,results.size());
	}

	@Test
	public void testScanWithNotEqualsOperator() throws SQLException{
		ResultSet rs = rule.executeQuery("select  sd from a where sd != 5");
		List<String> results  = Lists.newArrayList();
		while(rs.next()){
			int sd = rs.getInt(1);
			Assert.assertTrue("incorrect sd returned!",sd!=5);
			results.add(String.format("sd:%d",sd));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect rows returned!",9,results.size());
	}

}
