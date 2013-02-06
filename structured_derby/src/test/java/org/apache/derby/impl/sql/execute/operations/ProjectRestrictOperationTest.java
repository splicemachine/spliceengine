package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import org.apache.log4j.Logger;
import org.junit.*;

import com.splicemachine.derby.test.SpliceDerbyTest;

/**
 * This tests basic table scans with and without projection/restriction
 */
public class ProjectRestrictOperationTest  {
	private static Logger LOG = Logger.getLogger(ProjectRestrictOperationTest.class);
	private static final Map<String,String> tableSchemas = Maps.newHashMap();
	static{
		tableSchemas.put("a","si varchar(40),sa varchar(40),sc int");
	}
	@Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);

	@BeforeClass
	public static void startup() throws Exception {
		DerbyTestRule.start();
		rule.createTables();

		PreparedStatement ps = rule.prepareStatement("insert into a values (?,?,?)");
		for(int i=0;i<10;i++){
			ps.setString(1,Integer.toString(i));
			ps.setString(2,Integer.toString(i) + " Times ten");
			ps.setInt(3,i);
			ps.executeUpdate();
		}
	}
	
	@AfterClass 
	public static void shutdown() throws Exception {
		rule.dropTables();
		DerbyTestRule.shutdown();
	}
	
	@Test
	public void testResrictWrapperOnTableScanPreparedStatement() throws SQLException {
		PreparedStatement s = rule.prepareStatement("select * from a where si like ?");
		s.setString(1,"%5%");
		ResultSet rs = s.executeQuery();
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(1));
			Assert.assertNotNull(rs.getString(2));
		}
		Assert.assertEquals(1, i);
	}

	@Test
	public void testRestrictOnAnythingTableScan() throws SQLException{
		ResultSet rs = rule.executeQuery("select * from a where si like '%'");
		int count=0;
		while(rs.next()){
			Assert.assertNotNull("a.si is null!",rs.getString(1));
			Assert.assertNotNull("b.si is null!",rs.getString(2));
			LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2));
			count++;
		}
		Assert.assertEquals("Incorrect number of rows returned!",10,count);
	}

	@Test
	public void testResrictWrapperOnTableScan() throws SQLException {
		ResultSet rs = rule.executeQuery("select * from a where si like '%5%'");
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("a.si="+rs.getString(1)+",b.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(1));
			Assert.assertNotNull(rs.getString(2));
		}
		Assert.assertEquals(1, i);
		rule.commit();
	}

	@Test
	public void testProjectRestrictWrapperOnTableScan() throws SQLException {
		ResultSet rs = rule.executeQuery("select si || 'Chicken Dumplings' from a where si like '%5%'");
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("a.si="+rs.getString(1));
			Assert.assertNotNull(rs.getString(1));
			Assert.assertEquals("String Concatenation Should Match", "5"+"Chicken Dumplings", rs.getString(1));
		}
		Assert.assertEquals(1, i);
		rule.commit();
	}

	@Test
	public void testInPredicate() throws Exception{
		List<String> corrects = Arrays.asList("1","2","4");
		String correctPredicate = Joiner.on("','").join(corrects);
		ResultSet rs = rule.executeQuery("select * from a where si in ('"+correctPredicate+"')");
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			String si	 = rs.getString(1);
			String sa = rs.getString(2);
			Assert.assertTrue("incorrect si returned!",corrects.contains(si));
			Assert.assertNotNull("incorrect sa!",sa);
			results.add(String.format("si=%s,sa=%s",si,sa));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect num rows returned!",corrects.size(),results.size());
	}

	@Test
	public void testSumOperator() throws Exception{
		int increment = 2;
		ResultSet rs = rule.executeQuery("select sc, sc+"+increment+" from a ");
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			int sc = rs.getInt(1);
			int scIncr = rs.getInt(2);
			Assert.assertEquals("Sum doesn't work!",sc+increment,scIncr);
			results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}

	@Test
	public void testTimesOperator() throws Exception{
		int multiplier = 2;
		ResultSet rs = rule.executeQuery("select sc, sc*"+multiplier+" from a ");
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			int sc = rs.getInt(1);
			int scIncr = rs.getInt(2);
			Assert.assertEquals("Sum doesn't work!",sc*multiplier,scIncr);
			results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}

	@Test
	public void testModulusOperator() throws Exception{
		int modulus = 2;
		ResultSet rs = rule.executeQuery("select sc, mod(sc,"+modulus+") from a ");
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			int sc = rs.getInt(1);
			int scIncr = rs.getInt(2);
			Assert.assertEquals("Modulus doesn't work!",sc%modulus,scIncr);
			results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}

	@Test
	public void testSubStringOperator() throws Exception{
		ResultSet rs = rule.executeQuery("select sa, substr(sa,1,1) from a ");
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			String sa = rs.getString(1);
			String sub = rs.getString(2);
			String subSa = sa.substring(0,1);
			Assert.assertEquals("Incorrect sub!",subSa,sub);
			results.add(String.format("sa=%s,sub=%s",sa,sub));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}

	@Test
	public void testConcatOperator() throws Exception{
		String concat = "b";
		ResultSet rs = rule.executeQuery("select si,si||'"+concat+"' from a ");
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			String sa = rs.getString(1);
			String concatAttempt = rs.getString(2);
			String correct = sa+concat;
			Assert.assertEquals("Incorrect concat!",correct,concatAttempt);
			results.add(String.format("sa=%s,concatAttempt=%s",sa,concatAttempt));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}

	@Test
	public void testUpperLowerOperator() throws Exception{
		ResultSet rs = rule.executeQuery("select sa,UPPER(sa),LOWER(sa) from a ");
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			String sa = rs.getString(1);
			String upper = rs.getString(2);
			String lower = rs.getString(3);
			String correctUpper = sa.toUpperCase();
			String correctLower = sa.toLowerCase();
			Assert.assertEquals("upper incorrect",correctUpper,upper);
			Assert.assertEquals("lower incorrect",correctLower,lower);

			results.add(String.format("sa:%s,upper:%s,lower:%s",sa,upper,lower));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect num rows returned!",10,results.size());
	}
}
