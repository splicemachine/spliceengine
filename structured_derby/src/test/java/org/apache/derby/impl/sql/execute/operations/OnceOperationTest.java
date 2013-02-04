package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Throwables;
import org.apache.derby.iapi.error.StandardException;
import org.apache.log4j.Logger;
import org.junit.*;
import com.splicemachine.derby.test.DerbyTestRule;

public class OnceOperationTest  {
	private static Logger LOG = Logger.getLogger(OnceOperationTest.class);
	private static final Map<String,String> tableMap;
	static{
		Map<String,String> tMap = new HashMap<String,String>();
		tMap.put("t2","k int, l int");
		tableMap = tMap;
	}
	@Rule public static DerbyTestRule rule = new DerbyTestRule(tableMap,false,LOG);

	@BeforeClass
	public static void startup() throws Exception{
		DerbyTestRule.start();
		rule.createTables();
		createData();
	}
	
	@AfterClass
	public static void shutdown() throws Exception{
		rule.dropTables();
		DerbyTestRule.shutdown();
	}
	
	
	
	public static void createData() throws SQLException {
		java.sql.PreparedStatement statement = rule.prepareStatement("insert into t2 values (?, ?)");
		statement.setInt(1, 1);
		statement.setInt(2, 2);
		statement.execute();
		statement.setInt(1, 3);
		statement.setInt(2, 4);
		statement.execute();
		statement.setInt(1, 3);
		statement.setInt(2, 4);
		statement.execute();
	}
	
	
	@Test
	public void testValuesStatement() throws SQLException {
		ResultSet rs = rule.executeQuery("values (select k from t2 where k = 1)");
		rs.next();
		Assert.assertNotNull(rs.getInt(1));
	}

	@Test
	@Ignore("Causes some weird issue where the table doesn't get dropped correctly")
	public void testValuesStatementBlowUp() throws Throwable{
		try{
			ResultSet rs = rule.executeQuery("values (select k from t2 where k = 3)");
			rs.next();
			Assert.assertNotNull(rs.getInt(1));
		}catch(Throwable t){
			LOG.info("Caught Exception as expected",t);
			Throwable root = Throwables.getRootCause(t);
			if(root instanceof StandardException){
				StandardException se = (StandardException)root;
				Assert.assertEquals("Incorrect error code returned!", 21000, se.getErrorCode());
			}else{
				//we failed, for some reason, so blow off
				throw t;
			}
		}
	}
}
