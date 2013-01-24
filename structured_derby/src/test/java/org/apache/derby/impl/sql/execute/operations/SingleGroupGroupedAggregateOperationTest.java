package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import com.splicemachine.derby.test.SpliceDerbyTest;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.log4j.Logger;
import org.junit.*;

/**
 * Tests aggregations around single-group entries.
 * @author Scott Fines
 *
 */
public class SingleGroupGroupedAggregateOperationTest {
    private static Logger LOG = Logger.getLogger(SingleGroupGroupedAggregateOperationTest.class);

    static Map<String,String> tableSchemaMap = Maps.newHashMap();
    static{
        tableSchemaMap.put("t","username varchar(40),i int");
    }

    @Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemaMap,false,LOG);

    @BeforeClass
    public static void setup() throws Exception{
        DerbyTestRule.start();
        rule.createTables();
        insertData();
    }

    @AfterClass
    public static void shutdown() throws Exception {
        rule.dropTables();
        DerbyTestRule.shutdown();
    }

    private static Map<String,Stats> unameStats = new HashMap<String,Stats>();

    private static final int size = 10;

    public static void insertData() throws Exception{
    	rule.setAutoCommit(false);
        PreparedStatement ps = rule.prepareStatement("insert into t (username, i) values (?,?)");
        List<String> users = Arrays.asList("jzhang");//,"sfines","jleach");
        for(int i=0;i< size;i++){
            for(String user:users){
                int value = i*10;

	            if(!unameStats.containsKey(user))
	                unameStats.put(user,new Stats());
	            unameStats.get(user).add(value);

	            ps.setString(1, user);
	            ps.setInt(2, value);
	            ps.executeUpdate();
            }
        }
        rule.commit();

        rule.splitTable("t");
    }
	
	@Test
	public void testGroupedMinOperation() throws Exception{
			ResultSet rs = rule.executeQuery("select username,min(i) from t group by username");
			int row =0;
			while(rs.next()){
				String uname = rs.getString(1);
				int min = rs.getInt(2);
				int correctMin = unameStats.get(uname).getMin();
				SpliceLogUtils.trace(LOG, "uname=%s, min=%d, correctMin=%d",uname,min,correctMin);
				Assert.assertEquals("Incorrect min for uname "+ uname,correctMin,min);
				row++;
			}
			Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}

	@Test
	public void testGroupedMaxOperation() throws Exception{
			ResultSet rs = rule.executeQuery("select username,max(i) from t group by username");
			int row =0;
			while(rs.next()){
				String uname = rs.getString(1);
				int max = rs.getInt(2);
				int correctMax = unameStats.get(uname).getMax();
				SpliceLogUtils.trace(LOG, "uname=%s, max=%d, correctMax=%d",uname,max,correctMax);
				Assert.assertEquals("Incorrect max for uname "+ uname,correctMax,max);
				row++;
			}
			Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testGroupedAvgOperation() throws Exception{
			ResultSet rs =rule.executeQuery("select username,avg(i) from t group by username");
			int row =0;
			while(rs.next()){
				String uname = rs.getString(1);
				int avg = rs.getInt(2);
				int correctAvg = unameStats.get(uname).getAvg();
				SpliceLogUtils.trace(LOG, "uname=%s, avg=%d, correctSum=%d,correctAvg=%d",
                                                    uname,avg,unameStats.get(uname).getSum(),correctAvg);
				Assert.assertEquals("Incorrect count for uname "+ uname,correctAvg,avg);
				row++;
			}
			Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testGroupedSumOperation() throws Exception{
			ResultSet rs =rule.executeQuery("select username,sum(i) from t group by username");
			int row =0;
			while(rs.next()){
				String uname = rs.getString(1);
				int sum = rs.getInt(2);
				LOG.info("uname="+uname+",sum="+sum);
				int correctSum = unameStats.get(uname).getSum();
				Assert.assertEquals("Incorrect count for uname "+ uname,correctSum,sum);
				row++;
			}
			Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}
	
	@Test
	public void testAllGroupedOperations() throws Exception{
			ResultSet rs =rule.executeQuery("select username,sum(i),avg(i),min(i),max(i) from t group by username");
			int row =0;
			while(rs.next()){
				String uname = rs.getString(1);
				int sum = rs.getInt(2);
				int avg = rs.getInt(3);
				int min = rs.getInt(4);
				int max = rs.getInt(5);
				SpliceLogUtils.info(LOG,"uname=%s,sum=%d,avg=%d,min=%d,max=%d",uname,sum,avg,min,max);
				int correctSum = unameStats.get(uname).getSum();
				int correctAvg = unameStats.get(uname).getAvg();
				int correctMax = unameStats.get(uname).getMax();
				int correctMin = unameStats.get(uname).getMin();
				Assert.assertEquals("Incorrect sum for uname "+ uname,correctSum,sum);
				Assert.assertEquals("Incorrect avg for uname "+ uname,correctAvg,avg);
				Assert.assertEquals("Incorrect max for uname "+ uname,correctMax,max);
				Assert.assertEquals("Incorrect min for uname "+ uname,correctMin,min);
				row++;
			}
			Assert.assertEquals("Not all groups found!", unameStats.size(),row);
	}

    private static class Stats {
        private int count;
        private int sum;
        private int max;
        private int min;

        public Stats(){
            this.count = 0;
            this.sum = 0;
            this.max = Integer.MIN_VALUE;
            this.min = Integer.MAX_VALUE;
        }

        public int getSum(){ return this.sum;}

        public int getMax(){ return this.max;}

        public int getMin(){ return this.min;}

        public int getAvg(){ return this.sum/this.count;}

        public void add(int next){
            this.count++;
            this.sum+=next;
            if(next > max)
                this.max = next;
            if(next < min)
                this.min = next;
        }
    }
}
