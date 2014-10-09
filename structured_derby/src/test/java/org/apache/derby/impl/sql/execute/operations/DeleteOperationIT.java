package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.test.SerialTest;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * @author Scott Fines
 *         Created: 2/5/13 12:24 PM
 */
//@Category(SerialTest.class)
public class DeleteOperationIT extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = DeleteOperationIT.class.getSimpleName().toUpperCase();
	private static final Logger LOG = Logger.getLogger(DeleteOperationIT.class);
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("T",CLASS_NAME,"(v1 int,v2 int)");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("a_test",spliceSchemaWatcher.schemaName,"(c1 smallint)");
	private static final Map<Integer,Integer> initialResults = Maps.newHashMap();

    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher("customer1",spliceSchemaWatcher.schemaName,"(cust_id int, status boolean)");
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher("shipment",spliceSchemaWatcher.schemaName,"(cust_id int)");
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher("customer2",spliceSchemaWatcher.schemaName,"(cust_id int, status boolean)");
    protected static SpliceTableWatcher deleteFailWatcher = new SpliceTableWatcher("delete",spliceSchemaWatcher.schemaName,"(cust_id int, status boolean)");

    @ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
        .around(spliceTableWatcher2)
        .around(spliceTableWatcher4)
        .around(spliceTableWatcher5)
        .around(spliceTableWatcher6)
            .around(deleteFailWatcher)
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

                    // Insert into customer and shipment tables
                    ps = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher4 + " values(?,?)");
                    ps.setInt(1, 1);
                    ps.setBoolean(2, true);
                    ps.executeUpdate();
                    ps.setInt(1, 2);
                    ps.setBoolean(2, true);
                    ps.executeUpdate();
                    ps.setInt(1, 3);
                    ps.setBoolean(2, true);
                    ps.executeUpdate();
                    ps.setInt(1, 4);
                    ps.setBoolean(2, true);
                    ps.executeUpdate();
                    ps.setInt(1, 5);
                    ps.setBoolean(2, true);
                    ps.executeUpdate();
                    
                    ps = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher6 + " values(?,?)");
                    ps.setInt(1, 1);
                    ps.setBoolean(2, true);
                    ps.executeUpdate();
                    ps.setInt(1, 2);
                    ps.setBoolean(2, true);
                    ps.executeUpdate();
                    ps.setInt(1, 3);
                    ps.setBoolean(2, true);
                    ps.executeUpdate();
                    ps.setInt(1, 4);
                    ps.setBoolean(2, true);
                    ps.executeUpdate();
                    ps.setInt(1, 5);
                    ps.setBoolean(2, true);
                    ps.executeUpdate();
                    
                    ps = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher5 + " values(?)");
                    ps.setInt(1, 2);
                    ps.executeUpdate();
                    ps.setInt(1, 4);
                    ps.executeUpdate();

            ps = spliceClassWatcher.prepareStatement("insert into "+ deleteFailWatcher+" values(?)");
            ps.setShort(1, (short) 1);ps.executeUpdate();
            ps.setShort(1, Short.MAX_VALUE);ps.executeUpdate();

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

	// If you change one of the following 'delete over join' tests,
    // you probably need to make a similar change to UpdateOperationIT.

    @Test
    public void testDeleteOverBroadcastJoin() throws Exception {
    	doTestDeleteOverJoin("BROADCAST", spliceTableWatcher4);
    }

    @Test
    public void testDeleteOverMergeSortJoin() throws Exception {
    	doTestDeleteOverJoin("SORTMERGE", spliceTableWatcher6);
    }

    private void doTestDeleteOverJoin(String hint, SpliceTableWatcher customerTable) throws Exception {
    	StringBuffer sb = new StringBuffer(200);
    	sb.append("delete from %s %s \n");
    	sb.append("where not exists ( \n");
		sb.append("  select 1 \n");
		sb.append("  from %s %s --SPLICE-PROPERTIES joinStrategy=%s \n");                                                                           
		sb.append("  where %s.cust_id = %s.cust_id \n");
		sb.append(") \n");
		String query = String.format(sb.toString(), customerTable, "customer", spliceTableWatcher5, "shipment", hint, "customer", "shipment");
    	int rows = methodWatcher.executeUpdate(query);
        Assert.assertEquals("Incorrect number of rows deleted.", 3, rows);
    }

    @Test(expected = SQLException.class)
		public void testDeleteThrowsDivideByZero() throws Exception {
				try{
						methodWatcher.executeUpdate(String.format("delete from %s where c1/0 = 1",spliceTableWatcher2));
				}catch(SQLException se){
						String sqlState = se.getSQLState();
						Assert.assertEquals("incorrect SQL state!","22012",sqlState);
						Assert.assertEquals("Incorrect message!","Attempt to divide by zero.",se.getMessage());
						throw se;
				}
		}

    @Test
    public void testDeleteThrowsOutOfRange() throws Exception {
        try{
            methodWatcher.executeUpdate(String.format("delete from %s where a+a > 0",deleteFailWatcher));
        }catch(SQLException se){
            Assert.assertEquals("Incorrect SQL state!", ErrorState.LANG_OUTSIDE_RANGE_FOR_DATATYPE.getSqlState(),se.getSQLState());
        }
    }
}
