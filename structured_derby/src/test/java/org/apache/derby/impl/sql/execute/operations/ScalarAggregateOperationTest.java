package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
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

public class ScalarAggregateOperationTest extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	private static final Logger LOG = Logger.getLogger(ScalarAggregateOperationTest.class);
	 public static final String CLASS_NAME = ScalarAggregateOperationTest.class.getSimpleName().toUpperCase();
		public static final String TABLE_NAME = "T";
		protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
		protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME,"(username varchar(40),i int)");
		protected static String INSERT = String.format("insert into %s.%s (i) values (?)", CLASS_NAME,TABLE_NAME);
		public static int size = 10;
		public static Stats stats = new Stats();
		@ClassRule 
		public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
			.around(spliceSchemaWatcher)
			.around(spliceTableWatcher)
			.around(new SpliceDataWatcher(){
				@Override
				protected void starting(Description description) {
					try {
						PreparedStatement ps = spliceClassWatcher.prepareStatement(INSERT);
						for(int i=0;i<size;i++){
							ps.setInt(1,i);
							stats.add(i);
							ps.executeUpdate();
						}
						spliceClassWatcher.splitTable(TABLE_NAME,CLASS_NAME,size/3);
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
	public void testCountOperation() throws Exception{
		ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s", this.getTableReference(TABLE_NAME)));
		int count =0;
		while(rs.next()){
			Assert.assertEquals("incorrect count returned!",stats.getCount(),rs.getInt(1));
			count++;
		}
		Assert.assertEquals("incorrect number of rows returned!",1,count);
	}

	@Test
	public void testSumOperation() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select sum(i) from %s", this.getTableReference(TABLE_NAME)));
		int i=0;
		while(rs.next()){
			Assert.assertEquals("Incorrect sum returned!",stats.getSum(),rs.getInt(1));
			i++;
		}
		Assert.assertEquals(1, i);
	}
	
	@Test
	public void testMinOperation() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select min(i) from %s", this.getTableReference(TABLE_NAME)));
		int i=0;
		while(rs.next()){
			Assert.assertEquals("Incorrect min returned!",stats.getMin(),rs.getInt(1));
			i++;
		}
		Assert.assertEquals(1, i);
	}
	
	@Test
	public void testMaxOperation() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select max(i) from %s", this.getTableReference(TABLE_NAME)));
		int i=0;
		while(rs.next()){
			Assert.assertEquals(stats.getMax(),rs.getInt(1));
			i++;
		}
		Assert.assertEquals(1, i);
	}

    @Test
    public void testQualifiedMaxOperation() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select max(i) from %s where i < %d", this.getTableReference(TABLE_NAME),size));
        int i=0;
        while(rs.next()){
            Assert.assertEquals(stats.getMax(),rs.getInt(1));
            i++;
        }
        Assert.assertEquals(1, i);
    }
	
	@Test
	public void textAvgOperation() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select avg(i) from %s", this.getTableReference(TABLE_NAME)));
		int i=0;
		while(rs.next()){
			Assert.assertEquals(stats.getAvg(),rs.getInt(1));
			i++;
		}
		Assert.assertEquals(1, i);
	}
	
	@Test
	public void testAllOperations() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("select sum(i), avg(i), max(i), min(i) from %s", this.getTableReference(TABLE_NAME)));
		int i=0;
		while(rs.next()){
			int sum = rs.getInt(1);
			int avg = rs.getInt(2);
			int max = rs.getInt(3);
			int min = rs.getInt(4);
			SpliceLogUtils.info(LOG, "sum=%d, avg=%d,max=%d,min=%d",sum,avg,max,min);
			Assert.assertEquals(stats.getSum(),sum);
			Assert.assertEquals(stats.getAvg(),avg);
			Assert.assertEquals(stats.getMax(),max);
			Assert.assertEquals(stats.getMin(),min);
			i++;
		}
		Assert.assertEquals(1, i);
	}

    @Test
    public void testRepeatedCountsPreparedStatement() throws Exception{
        //use a PreparedStatement twice, and make sure that the counts come back correct.
        PreparedStatement ps = methodWatcher.prepareStatement(format("select count(i) from %s where i <= ? and i > ?", this.getTableReference(TABLE_NAME)));
        ps.setInt(1,2);
        ps.setInt(2,0);
        ResultSet rs = ps.executeQuery();

        int i=0;
        try{
            while(rs.next()){
                int count = rs.getInt(1);
                SpliceLogUtils.info(LOG, "count=%d",count);
                Assert.assertEquals(2,count);
                i++;
            }
            Assert.assertEquals(1, i);
        }finally{
            rs.close();
        }

        ps.setInt(1,4);
        ps.setInt(2,2);
        rs = ps.executeQuery();
        i = 0;
        try{
            while(rs.next()){
                int count = rs.getInt(1);
                SpliceLogUtils.info(LOG, "count=%d",count);
                Assert.assertEquals(2,count);
                i++;
            }
            Assert.assertEquals(1, i);
        }finally{
            rs.close();
        }

    }
}
