package org.apache.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import org.apache.log4j.Logger;
import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created: 2/5/13 12:24 PM
 */
public class DeleteOperationTest {
	private static final Logger LOG = Logger.getLogger(DeleteOperationTest.class);

	private static final Map<String,String> tableSchemas = Maps.newHashMap();
	static{
		tableSchemas.put("t","v1 int,v2 int");
	}
	private static final Map<Integer,Integer> initialResults = Maps.newHashMap();

	@Rule
	public DerbyTestRule rule = new DerbyTestRule(tableSchemas,LOG);

	@BeforeClass
	public static void start() throws Exception{
		DerbyTestRule.start();
	}

	@AfterClass
	public static void shutdown() throws Exception{
		DerbyTestRule.shutdown();
	}

	@Before
	public void setUp() throws Exception {
		rule.createTables();

		PreparedStatement ps = rule.prepareStatement("insert into t values (?,?)");
		ps.setInt(1,1);
		ps.setInt(2,2);
		ps.executeUpdate();
		initialResults.put(1,2);

		ps.setInt(1,3);
		ps.setInt(2,4);
		ps.executeUpdate();
		initialResults.put(3,4);
	}

	@After
	public void tearDown() throws Exception {
		rule.dropTables();
	}

	@Test
	public void testDelete() throws Exception{
		Statement s = rule.getStatement();
		int numDeleted = s.executeUpdate("delete from t where v1 = 1");
		Assert.assertEquals("Incorrect num rows deleted", 1, numDeleted);
		Map<Integer,Integer> deletedResults = Maps.newHashMap(initialResults);
		deletedResults.remove(1);

		ResultSet rs = rule.executeQuery("select * from t where v1 = 1");
		Assert.assertTrue("Rows were returned!",!rs.next());

		rs = rule.executeQuery("select * from t ");
		List<String> results = Lists.newArrayList();
		while(rs.next()){
			Integer v1 = rs.getInt(1);
			Integer v2 = rs.getInt(2);
			Assert.assertNotNull("v1 not returned!",v1);
			Assert.assertNotNull("v2 not returned!",v2);

			deletedResults.put(v1,v2);
			results.add(String.format("v1:%d,v2:%d",v1,v2));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect returned row size!",initialResults.size()-numDeleted,deletedResults.size());
		for(Integer keptV1:deletedResults.keySet()){
			Assert.assertTrue("Not in original data set!",initialResults.containsKey(keptV1));
			Assert.assertEquals("incorrect v2!",initialResults.get(keptV1),deletedResults.get(keptV1));
		}
	}
}
