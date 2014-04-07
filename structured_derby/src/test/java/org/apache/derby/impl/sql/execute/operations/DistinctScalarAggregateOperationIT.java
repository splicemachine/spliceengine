package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.Statement;

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

public class DistinctScalarAggregateOperationIT extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	private static Logger LOG = Logger.getLogger(DistinctScalarAggregateOperationIT.class);
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(DistinctScalarAggregateOperationIT.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("ORDERSUMMARY",DistinctScalarAggregateOperationIT.class.getSimpleName(),"(oid int, catalog varchar(40), score int, brand varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("EMPTY_TABLE",DistinctScalarAggregateOperationIT.class.getSimpleName(),"(oid int, catalog varchar(40), score int, brand char(40))");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(spliceTableWatcher2)		
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					Statement s = spliceClassWatcher.getStatement();
					s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(1, 'clothes', 2, 'zara')");
					s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(2, 'clothes', 2, 'ann taylor')");
					s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(2, 'clothes', 2, 'd&g')");
					s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(2, 'showes', 3, 'zara')");
					s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(3, 'showes', 3, 'burberry')");
					s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(3, 'clothes', 2, 'd&g')");
					s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(3, 'handbags', 5, 'd&g')");
					s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(3, 'handbags', 5, 'd&g')");
					s.execute("insert into " +DistinctScalarAggregateOperationIT.class.getSimpleName()+ ".ordersummary values(4, 'furniture', 6, 'ea')");
//                    spliceClassWatcher.splitTable("ordersummary", spliceSchemaWatcher.schemaName);
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
            }
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
		
	@Test
	public void testDistinctScalarAggregate() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("select sum(distinct score),max(distinct score),min(distinct score) from" + this.getPaddedTableReference("ORDERSUMMARY"));
		if (rs.next()) {
			LOG.info("sum of distinct="+rs.getInt(1));
			Assert.assertEquals("incorrect sum",16, rs.getInt(1));
            Assert.assertEquals("incorrect max",6,rs.getInt(2));
            Assert.assertEquals("incorrect min",2,rs.getInt(3));
        }else{
            Assert.fail("No results returned!");
        }
    }

    @Test
    public void testDistinctScalarAggregateRepeatedly() throws Exception {
        /*
         * This is a test to attempt to reproduce Bug 480. Under normal circumstances, this test
         * does nothing for us except take forever to run, so most of the time it should be ignored.
         */
        for(int i=0;i<100;i++){
            if(i%10==0)
                System.out.printf("Ran %d times without failure%n",i);
            testDistinctScalarAggregate();
        }
    }

    @Test
    public void testDistinctCount() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select count(distinct score) from" + this.getPaddedTableReference("ORDERSUMMARY"));
        if (rs.next()) {
            LOG.info("count of distinct="+rs.getInt(1));
            Assert.assertEquals("incorrect count",4,rs.getInt(1));
        }else{
            Assert.fail("No results returned!");
        }
    }

    @Test
    public void testDistinctScalarAggregateReturnsZeroOnEmptyTable() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select count(distinct brand),min(distinct brand),max(distinct brand) from "+ this.getPaddedTableReference("EMPTY_TABLE"));
        Assert.assertTrue("No rows returned!",rs.next());
        Assert.assertEquals("incorrect count returned",0,rs.getInt(1));
    }
}
