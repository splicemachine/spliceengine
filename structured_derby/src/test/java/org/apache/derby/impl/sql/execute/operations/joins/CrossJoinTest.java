package org.apache.derby.impl.sql.execute.operations.joins;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;

public class CrossJoinTest extends BaseJoinTest {
	private static Logger LOG = Logger.getLogger(CrossJoinTest.class);
	
	private static final Map<String,String> tableMap = Maps.newHashMap();
	static{
		tableMap.put("cc","si varchar(40), sa varchar(40)");
		tableMap.put("dd","si varchar(40), sa varchar(40)");
	}
	
	@Rule public static DerbyTestRule rule = new DerbyTestRule(tableMap,false,LOG);
	
	@BeforeClass
	public static void startup() throws Exception{
		DerbyTestRule.start();
		rule.createTables();
		insertData("cc","dd",rule);
	}
	
	@AfterClass
	public static void shutdown() throws Exception{
		rule.dropTables();
		DerbyTestRule.shutdown();
	}
	
	@Test
	public void testScrollableCrossJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select cc.si, dd.si from cc cross join dd");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("cc.si="+rs.getString(1)+",dd.si="+rs.getString(2));
			Assert.assertNotNull(rs.getString(1));
			if (!rs.getString(1).equals("9")) {
				Assert.assertNotNull(rs.getString(1));
				Assert.assertNotNull(rs.getString(2));
			} else {
				Assert.assertTrue(!rs.getString(2).equals("9"));
			}
		}	
		Assert.assertEquals(90, j);
	}		
	
	@Test
	@Ignore // Does not work yet, ugh
	public void testSinkableCrossJoin() throws SQLException {			
		ResultSet rs = rule.executeQuery("select cc.si, count(*) from cc cross join dd group by cc.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("cc.si="+rs.getString(1)+",count="+rs.getLong(2));
			Assert.assertNotNull(rs.getString(1));
			Assert.assertEquals(9,rs.getLong(2));
		}	
		Assert.assertEquals(9, j);
	}		
}
