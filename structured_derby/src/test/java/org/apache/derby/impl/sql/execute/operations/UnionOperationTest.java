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

/**
 * @author jessiezhang
 */
public class UnionOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(UnionOperationTest.class);
	
	private static final Map<String,String> tableMap;
	static{
		Map<String,String> tMap = new HashMap<String,String>();
		tMap.put("st_mars","empId int, empNo int, name varchar(40)");
		tMap.put("st_earth","empId int, empNo int, name varchar(40)");		
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
			s.execute("insert into st_mars values(1, 1, 'Mulgrew, Kate')");
			s.execute("insert into st_mars values(2, 1, 'Shatner, William')");
			s.execute("insert into st_mars values(3, 1, 'Nimoy, Leonard')");
			s.execute("insert into st_mars values(4, 1, 'Patrick')");
			s.execute("insert into st_mars values(5, 1, null)");			
			s.execute("insert into st_earth values(1, 1, 'Spiner, Brent')");
			s.execute("insert into st_earth values(2, 1, 'Duncan, Rebort')");
			s.execute("insert into st_earth values(3, 1, 'Nimoy, Leonard')");
			s.execute("insert into st_earth values(4, 1, 'Ryan, Jeri')");
			s.execute("insert into st_earth values(5, 1, null)");
	}	

	@Test
	public void testUnionAll() throws SQLException {			
		ResultSet rs = rule.executeQuery("select name from st_mars UNION ALL select name from st_earth");
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("person name="+rs.getString(1));
		}	
		Assert.assertEquals(10, i);
	}
	
	@Test
	public void testUnionOneColumn() throws SQLException {			
		ResultSet rs = rule.executeQuery("select name from st_mars UNION select name from st_earth");
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("person name="+rs.getString(1));
		}	
		Assert.assertEquals(8, i);
	}		
	
	@Test
	/**
	 * 
	 * This needs to use a provider interface for boths its traversals and not use isScan - JL
	 * 
	 * @throws SQLException
	 */
	public void testValuesUnion() throws SQLException {
		ResultSet rs = rule.executeQuery("SELECT TTABBREV, TABLE_TYPE from (VALUES ('T','TABLE'), ('S','SYSTEM TABLE'), ('V', 'VIEW'), ('A', 'SYNONYM')) T (TTABBREV,TABLE_TYPE)");
		int i = 0;
		while (rs.next()) {
			i++;
		}	
		Assert.assertTrue(i>0);
	}
	
	@Test
	public void testUnion() throws SQLException {			
		ResultSet rs = rule.executeQuery("select * from st_mars UNION select * from st_earth");
		int i = 0;
		while (rs.next()) {
			i++;
			LOG.info("id="+rs.getInt(1)+",person name="+rs.getString(2));
		}	
		Assert.assertEquals(8, i);
	}		
}
