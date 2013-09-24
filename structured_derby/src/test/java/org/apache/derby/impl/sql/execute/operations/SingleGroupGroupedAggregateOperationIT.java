package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.suites.Stats;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Tests aggregations around single-group entries.
 * @author Scott Fines
 *
 */
public class SingleGroupGroupedAggregateOperationIT extends SpliceUnitTest { 
    private static Logger LOG = Logger.getLogger(SingleGroupGroupedAggregateOperationIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = SingleGroupGroupedAggregateOperationIT.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "T";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(username varchar(40),i int)");
		
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					spliceClassWatcher.setAutoCommit(false);
			        PreparedStatement ps = spliceClassWatcher.prepareStatement(format("insert into %s.%s (username, i) values (?,?)",CLASS_NAME,TABLE_NAME_1));
			        List<String> users = Arrays.asList("jzhang","sfines","jleach");
			        for(int i=0;i< size;i++){
			            for(String user:users){
			                int value = i*10;

				            if(!unameStats.containsKey(user))
				                unameStats.put(user,new Stats());
				            unameStats.get(user).add(value);

				            ps.setString(1, user);
				            ps.setInt(2, value);
				            SpliceLogUtils.trace(LOG,"user="+user+",value="+value);
				            ps.executeUpdate();
			            }
			        }
			        spliceClassWatcher.commit();
			        spliceClassWatcher.splitTable(TABLE_NAME_1,CLASS_NAME,size/3);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
    private static Map<String,Stats> unameStats = new HashMap<String,Stats>();
    private static final int size = 10;

    @Test
    public void testGroupedWithInOperator() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select username, count(i) from %s where username in ('sfines','jzhang') group by username",this.getTableReference(TABLE_NAME_1)));
        int row =0;
        while(rs.next()){
            String uname = rs.getString(1);
            int count = rs.getInt(2);
            int correctCount = unameStats.get(uname).getCount();
            Assert.assertEquals("Incorrect count for uname "+ uname,correctCount,count);
            row++;
        }
        Assert.assertEquals("Not all groups found!", 2,row);
    }

    @Test
	public void testGroupedCountOperation() throws Exception{
			ResultSet rs = methodWatcher.executeQuery(format("select username,count(i) from %s group by username",this.getTableReference(TABLE_NAME_1)));
			int row =0;
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String uname = rs.getString(1);
            int count = rs.getInt(2);
            results.add(String.format("uname=%s,count=%d",uname,count));
            Stats stats = unameStats.get(uname);
            if(stats==null){
                for(String result:results){
                    System.out.println(result);
                }
            }
//                int correctCount = stats.getCount();
//				Assert.assertEquals("Incorrect count for uname "+ uname,correctCount,count);
            row++;
        }
        Assert.assertEquals("Not all groups found!", unameStats.size(),row);
    }

    @Test
//    @Ignore
    public void testRepeatedGroupedCount() throws Exception {
        /* Regression test for Bug 306 */
        for(int i=0;i<100;i++){
//            System.out.println(i);
            testGroupedCountOperation();
        }
    }

    @Test
	public void testGroupedMinOperation() throws Exception{
			ResultSet rs = methodWatcher.executeQuery(format("select username,min(i) from %s group by username",this.getTableReference(TABLE_NAME_1)));
			int row =0;
			while(rs.next()){
				String uname = rs.getString(1);
				int min = rs.getInt(2);
				int correctMin = unameStats.get(uname).getMin();
				Assert.assertEquals("Incorrect min for uname "+ uname,correctMin,min);
				row++;
			}
			Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}

	@Test
	public void testGroupedMaxOperation() throws Exception{
			ResultSet rs = methodWatcher.executeQuery(format("select username,max(i) from %s group by username",this.getTableReference(TABLE_NAME_1)));
			int row =0;
			while(rs.next()){
				String uname = rs.getString(1);
				int max = rs.getInt(2);
				int correctMax = unameStats.get(uname).getMax();
				Assert.assertEquals("Incorrect max for uname "+ uname,correctMax,max);
				row++;
			}
			Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testGroupedAvgOperation() throws Exception{
			ResultSet rs = methodWatcher.executeQuery(format("select username,avg(i) from %s group by username",this.getTableReference(TABLE_NAME_1)));
			int row =0;
			while(rs.next()){
				String uname = rs.getString(1);
				int avg = rs.getInt(2);
				int correctAvg = unameStats.get(uname).getAvg();
				Assert.assertEquals("Incorrect count for uname "+ uname,correctAvg,avg);
				row++;
			}
			Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testGroupedSumOperation() throws Exception{
			ResultSet rs = methodWatcher.executeQuery(format("select username,sum(i) from %s group by username",this.getTableReference(TABLE_NAME_1)));
			int row =0;
			while(rs.next()){
				String uname = rs.getString(1);
				int sum = rs.getInt(2);
				int correctSum = unameStats.get(uname).getSum();
				Assert.assertEquals("Incorrect count for uname "+ uname,correctSum,sum);
				row++;
			}
			Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
}
