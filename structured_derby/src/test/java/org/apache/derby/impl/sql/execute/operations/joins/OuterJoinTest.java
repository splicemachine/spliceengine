package org.apache.derby.impl.sql.execute.operations.joins;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.splicemachine.derby.test.DerbyTestRule;

public class OuterJoinTest extends BaseJoinTest {
	private static Logger LOG = Logger.getLogger(OuterJoinTest.class);
	
	private static final Map<String,String> tableMap;
	static{
		Map<String,String> tMap = new HashMap<String,String>();
		tMap.put("f","si varchar(40), sa varchar(40)");
		tMap.put("g","si varchar(40), sa varchar(40)");
		tableMap = tMap;
	}
	@Rule public static DerbyTestRule rule = new DerbyTestRule(tableMap,false,LOG);
	
	@BeforeClass
	public static void startup() throws Exception{
		DerbyTestRule.start();
		rule.createTables();
		createData("f","g",rule);
	}
	
	@AfterClass
	public static void shutdown() throws Exception{
		rule.dropTables();
		DerbyTestRule.shutdown();
	}
	
	@Test
	public void testScrollableVarcharLeftOuterJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select f.si, g.si from f left outer join g on f.si = g.si");
		int j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(1).equals("9")) {
				Assert.assertNotNull(rs.getString(2));
				Assert.assertEquals(rs.getString(1),rs.getString(2));
			} else {
				Assert.assertNull(rs.getString(2));
			}
		}	
		Assert.assertEquals(10, j);
	}		

	@Test
	public void testSinkableVarcharLeftOuterJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select f.si, count(*) from f left outer join g on f.si = g.si group by f.si");
		int j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(1).equals("9")) {
				Assert.assertEquals(1l,rs.getLong(2));
			}
		}	
		Assert.assertEquals(10, j);
	}		
	
	@Test
	public void testScrollableVarcharRightOuterJoin() throws SQLException {			
		ResultSet rs = rule.executeQuery("select f.si, g.si from f right outer join g on f.si = g.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("c.si="+rs.getString(1)+",d.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(2));
			if (!rs.getString(2).equals("9")) {
				Assert.assertNotNull(rs.getString(1));
				Assert.assertEquals(rs.getString(1),rs.getString(2));
			} else {
				Assert.assertNull(rs.getString(1));
			}
		}	
		Assert.assertEquals(9, j);
	}	
	@Test
	public void testSinkableVarcharRightOuterJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select f.si, count(*) from f right outer join g on f.si = g.si group by f.si");
		int j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(1).equals("9")) {
				Assert.assertEquals(1l,rs.getLong(2));
			} else {
				Assert.assertNotNull(null);
			}
		}	
		Assert.assertEquals(9, j);
	}
}
