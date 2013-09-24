package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
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

public class DistinctGroupedAggregateOperationIT extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = DistinctGroupedAggregateOperationIT.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "A";
	private static Logger LOG = Logger.getLogger(DistinctGroupedAggregateOperationIT.class);
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(oid int, quantity int)");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
				Statement s = spliceClassWatcher.getStatement();
				s.execute(format("insert into %s.%s values(1, 5)",CLASS_NAME,TABLE_NAME_1));
				s.execute(format("insert into %s.%s values(2, 2)",CLASS_NAME,TABLE_NAME_1));
				s.execute(format("insert into %s.%s values(2, 1)",CLASS_NAME,TABLE_NAME_1));
				s.execute(format("insert into %s.%s values(3, 10)",CLASS_NAME,TABLE_NAME_1));
				s.execute(format("insert into %s.%s values(3, 5)",CLASS_NAME,TABLE_NAME_1));
				s.execute(format("insert into %s.%s values(3, 1)",CLASS_NAME,TABLE_NAME_1));
				s.execute(format("insert into %s.%s values(3, 1)",CLASS_NAME,TABLE_NAME_1));				
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
	@Ignore("Bug - JL")
	public void testDistinctGroupedAggregate() throws Exception {			
			ResultSet rs = methodWatcher.executeQuery(format("select oid, sum(distinct quantity) as summation from %s group by oid",this.getTableReference(TABLE_NAME_1)));
			int j = 0;
			while (rs.next()) {
				LOG.info("oid="+rs.getInt(1)+",sum(distinct quantity)="+rs.getInt(2));
				if (j==0) {
					Assert.assertEquals(5, rs.getInt(2));
				} else if (j==1) {
					Assert.assertEquals(3, rs.getInt(2));
				} else if (j==2) {
					Assert.assertEquals(16, rs.getInt(2));
				}
				j++;
			}	
	}		
}
