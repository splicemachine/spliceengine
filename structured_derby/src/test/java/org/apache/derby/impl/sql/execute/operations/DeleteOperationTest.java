package org.apache.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created: 2/5/13 12:24 PM
 */
public class DeleteOperationTest extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = DeleteOperationTest.class.getSimpleName().toUpperCase();
	private static final Logger LOG = Logger.getLogger(DeleteOperationTest.class);
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("T",CLASS_NAME,"(v1 int,v2 int)");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("a_test",spliceSchemaWatcher.schemaName,"(c1 smallint)");
	private static final Map<Integer,Integer> initialResults = Maps.newHashMap();

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
        .around(spliceTableWatcher2)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					PreparedStatement ps = spliceClassWatcher.prepareStatement(format("insert into %s.T values (?,?)",CLASS_NAME));
					ps.setInt(1,1);
					ps.setInt(2,2);
					ps.executeUpdate();
					initialResults.put(1,2);
					ps.setInt(1,3);
					ps.setInt(2,4);
					ps.executeUpdate();
					initialResults.put(3,4);

                    ps = spliceClassWatcher.prepareStatement(format("insert into %s values (?)",spliceTableWatcher2.toString()));
                    ps.setInt(1,32767);
                    ps.executeUpdate();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();


	@Test
	public void testDelete() throws Exception{
		Statement s = methodWatcher.getStatement();
		int numDeleted = s.executeUpdate(format("delete from %s where v1 = 1",this.getTableReference("T")));
		Assert.assertEquals("Incorrect num rows deleted", 1, numDeleted);
		Map<Integer,Integer> deletedResults = Maps.newHashMap(initialResults);
		deletedResults.remove(1);

		ResultSet rs = methodWatcher.executeQuery(format("select * from %s where v1 = 1",this.getTableReference("T")));
		Assert.assertTrue("Rows were returned!",!rs.next());

		rs = methodWatcher.executeQuery(format("select * from %s",this.getTableReference("T")));
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

    @Test(expected= SQLException.class,timeout=10000)
    public void testDeleteWithSumOverflowThrowsError() throws Exception {
        try{
            methodWatcher.getStatement().execute("delete from "+spliceTableWatcher2.toString()+" where c1+c1 > 0");
        }catch(SQLException sql){
           Assert.assertEquals("Incorrect SQLState for message " + sql.getMessage(),SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE,sql.getSQLState());
            throw sql;
        }
    }
}
