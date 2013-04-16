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

public class DistinctScalarAggregateOperationTest extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	private static Logger LOG = Logger.getLogger(DistinctScalarAggregateOperationTest.class);
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(DistinctScalarAggregateOperationTest.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("ORDERSUMMARY",DistinctScalarAggregateOperationTest.class.getSimpleName(),"(oid int, catalog varchar(40), score int, brand varchar(40))");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					Statement s = spliceClassWatcher.getStatement();
					s.execute("insert into " +DistinctScalarAggregateOperationTest.class.getSimpleName()+ ".ordersummary values(1, 'clothes', 2, 'zara')");
					s.execute("insert into " +DistinctScalarAggregateOperationTest.class.getSimpleName()+ ".ordersummary values(2, 'clothes', 2, 'ann taylor')");
					s.execute("insert into " +DistinctScalarAggregateOperationTest.class.getSimpleName()+ ".ordersummary values(2, 'clothes', 2, 'd&g')");
					s.execute("insert into " +DistinctScalarAggregateOperationTest.class.getSimpleName()+ ".ordersummary values(2, 'showes', 3, 'zara')");
					s.execute("insert into " +DistinctScalarAggregateOperationTest.class.getSimpleName()+ ".ordersummary values(3, 'showes', 3, 'burberry')");
					s.execute("insert into " +DistinctScalarAggregateOperationTest.class.getSimpleName()+ ".ordersummary values(3, 'clothes', 2, 'd&g')");
					s.execute("insert into " +DistinctScalarAggregateOperationTest.class.getSimpleName()+ ".ordersummary values(3, 'handbags', 5, 'd&g')");
					s.execute("insert into " +DistinctScalarAggregateOperationTest.class.getSimpleName()+ ".ordersummary values(3, 'handbags', 5, 'd&g')");
					s.execute("insert into " +DistinctScalarAggregateOperationTest.class.getSimpleName()+ ".ordersummary values(4, 'furniture', 6, 'ea')");
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
		ResultSet rs = methodWatcher.executeQuery("select sum(distinct score) from" + this.getPaddedTableReference("ORDERSUMMARY"));
		if (rs.next()) {
			LOG.info("sum of distinct="+rs.getInt(1));
			Assert.assertEquals(16, rs.getInt(1));
		}else{
            Assert.fail("No results returned!");
        }
	} 
}
