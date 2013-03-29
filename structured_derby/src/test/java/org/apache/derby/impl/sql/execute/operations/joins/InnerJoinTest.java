package org.apache.derby.impl.sql.execute.operations.joins;

import java.io.FileInputStream;
import java.sql.*;
import java.util.Map;

import com.splicemachine.homeless.TestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;

public class InnerJoinTest extends BaseJoinTest {
	private static Logger LOG = Logger.getLogger(InnerJoinTest.class);
	private static final Map<String,String> tableMap = Maps.newHashMap();

    static {
        tableMap.put("cc", "si varchar(40), sa varchar(40)");
        tableMap.put("dd", "si varchar(40), sa varchar(40)");
    }

    @Rule public static DerbyTestRule rule = new DerbyTestRule(tableMap,false,LOG);

	@BeforeClass
	public static void startup() throws Exception{
		DerbyTestRule.start();
		rule.createTables();
		insertData("cc","dd",rule);
        TestUtils.executeSqlFile(rule.getConnection(), "small_msdatasample/startup.sql");
	}
	
	@AfterClass
	public static void shutdown() throws Exception{
		rule.dropTables();
        String sqlStatementStrings = IOUtils.toString(new FileInputStream(TestUtils.getBaseDirectory() + "small_msdatasample/shutdown.sql"));
        TestUtils.executeSqlFile(rule.getConnection(), "small_msdatasample/shutdown.sql");
		DerbyTestRule.shutdown();
	}
	
	@Test
	public void testScrollableInnerJoin() throws SQLException {
		ResultSet rs = rule.executeQuery("select cc.si, dd.si from cc inner join dd on cc.si = dd.si");
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
		ResultSet rs = rule.executeQuery("select cc.si, count(*) from cc inner join dd on cc.si = dd.si group by cc.si");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("cc.si="+rs.getString(1)+",dd.si="+rs.getString(2));
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

    @Test(expected = java.sql.SQLException.class)
    public void testThreeTableJoin() throws SQLException {
        rule.executeQuery("select t1.orl_order_id, t2.cst_last_name, t2.cst_first_name, t3.itm_name " +
                "from order_line t1, customer t2, item t3 " +
                "where t1.orl_customer_id = t2.cst_id and t1.orl_item_id = t3.itm_id");
    }
}
