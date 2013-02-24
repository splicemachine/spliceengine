package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.splicemachine.derby.test.DerbyTestRule;
import com.splicemachine.derby.test.SpliceDerbyTest;
import org.junit.experimental.categories.Category;

/**
 * This tests basic table scans with and without projection/restriction
 */

public class DistinctScanOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(DistinctScanOperationTest.class);
	private static final Map<String,String> tableMap;
	static{
		Map<String,String> tMap = new HashMap<String,String>();
		tMap.put("foo","si varchar(40), sa varchar(40)");
		tMap.put("foobar","name varchar(40), empId int");		
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
	public static void createData(DerbyTestRule rule) throws Exception {
		rule.setAutoCommit(true);
		Statement s = rule.getStatement();
		for (int i =1; i<= 10; i++) {
			if (i%3 == 0)
				s.execute("insert into foo values('3','" + "i')");
			else if (i%4 == 0)
				s.execute("insert into foo values('4','" + "i')");
			else
				s.execute("insert into foo values('" + i + "','" + "i')");
		}
		s.execute("insert into foobar values('Mulgrew, Kate', 1)");
		s.execute("insert into foobar values('Shatner, William', 2)");
		s.execute("insert into foobar values('Nimoy, Leonard', 3)");
		s.execute("insert into foobar values('Stewart, Patrick', 4)");
		s.execute("insert into foobar values('Spiner, Brent', 5)");
		s.execute("insert into foobar values('Duncan, Rebort', 6)");
		s.execute("insert into foobar values('Nimoy, Leonard', 7)");
		s.execute("insert into foobar values('Ryan, Jeri', 8)");
	}	
	
	@Test
	public void testDistinctScanOperation() throws SQLException {			
		ResultSet rs = rule.executeQuery("select distinct si from foo");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("si="+rs.getString(1));
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(7, j);
	}		
	
	@Test
	public void testDistinctString() throws SQLException {			
		ResultSet rs = rule.executeQuery("select distinct name from foobar");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("person name="+rs.getString(1));
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(7, j);
	}	
	
	@Test
    @Category(OperationCategories.Transactional.class)
	public void testTransactionalDistinctString() throws SQLException {	
		rule.setAutoCommit(false);
		Statement s = rule.getStatement();
		s.execute("insert into foobar values('Noncommitted, Noncommitted', 9)");
		ResultSet rs = s.executeQuery("select distinct name from foobar");
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("before rollback, distinct person name="+rs.getString(1));
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(8, j);
		rule.rollback();				
		rs = s.executeQuery("select distinct name from foobar");
		j = 0;
		while (rs.next()) {
			j++;
			LOG.info("after rollback, person name="+rs.getString(1));
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(7, j);			
	}		
}
