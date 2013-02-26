package org.apache.derby.impl.sql.execute.operations.joins;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.splicemachine.derby.test.DerbyTestRule;

public class MergeSortJoinTest extends BaseJoinTest {
	private static Logger LOG = Logger.getLogger(OuterJoinTest.class);

	private static final Map<String,String> tableMap;
	static{
		Map<String,String> tMap = new HashMap<String,String>();
		tMap.put("c","si varchar(40), sa varchar(40)");
		tMap.put("d","si varchar(40), sa varchar(40)");
		tableMap = tMap;
	}
	@Rule public static DerbyTestRule rule = new DerbyTestRule(tableMap,false,LOG);

	@BeforeClass
	public static void startup() throws Exception{
		DerbyTestRule.start();
		rule.createTables();
		createData(rule);
	}

	@AfterClass
	public static void shutdown() throws Exception{
		rule.dropTables();
		DerbyTestRule.shutdown();
	}

	@Test
	public void testScrollableVarcharLeftOuterJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select c.si, d.si from c left outer join d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on c.si = d.si");
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
		ResultSet rs = rule.executeQuery("select c.si, count(*) from c left outer join d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on c.si = d.si group by c.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info(String.format("c.sa=%s,count=%d",rs.getString(1),rs.getInt(2)));
//			Assert.assertNotNull(rs.getString(1));
//			if (!rs.getString(1).equals("9")) {
//				Assert.assertEquals(1l,rs.getLong(2));
//			}
		}
		Assert.assertEquals(10, j);
	}

	@Test
	public void testReturnOutOfOrderJoin() throws SQLException{
		ResultSet rs = rule.executeQuery("select c.sa, d.sa,c.si from c inner join d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on c.si = d.si");
		while(rs.next()){
			LOG.info(String.format("c.sa=%s,d.sa=%s",rs.getString(1),rs.getString(2)));
		}
	}

	@Test
	public void testScrollableInnerJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select c.si, d.si from c inner join d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on c.si = d.si");
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
		Assert.assertEquals(9, j);
	}

	@Test
	public void testSinkableInnerJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select c.si, count(*) from c inner join d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on c.si = d.si group by c.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("c.si="+rs.getString(1)+",d.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(2).equals("9")) {
				Assert.assertNotNull(rs.getString(1));
				Assert.assertEquals(1l,rs.getLong(2));
			} else {
				Assert.assertNull(rs.getString(1));
			}
		}
		Assert.assertEquals(9, j);
	}

	@Test
	@Ignore
	public void testScrollableVarcharRightOuterJoin() throws SQLException {			
		ResultSet rs = rule.executeQuery("select c.si, d.si from c right outer join d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on c.si = d.si");
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
	@Ignore
	public void testSinkableVarcharRightOuterJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select c.si, count(*) from c right outer join d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n on c.si = d.si group by c.si");
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

