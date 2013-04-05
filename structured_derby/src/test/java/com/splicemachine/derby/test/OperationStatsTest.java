package com.splicemachine.derby.test;

import java.sql.ResultSet;
import java.sql.Statement;
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

/**
 * DDL test cases
 * 
 * @author jessiezhang
 */

public class OperationStatsTest extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = OperationStatsTest.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "A";
	public static final String TABLE_NAME_2 = "B";
	public static final String TABLE_NAME_3 = "C";
	
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(i int)");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME,"(j int)");

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					Statement s = spliceClassWatcher.getStatement();
					s.execute(String.format("insert into %s.%s values 1,2,5,2,54,6,57,6,6,86,555657,787,78894,2,324,4,3,44,4556,7,32,43,43545,46,565765,34,54,65664,34,45,3,3,35,5,6",CLASS_NAME,TABLE_NAME_1));
					s.execute(String.format("insert into %s.%s values 1,34,5,3,45,6,7,8,5,3,23,2,3,33,2,2,2,2,4,4,54,65,66767,678,78,7565,543434,56,657,6767,54,3345,6,755,3,65664,33,54,5,6,565765,43,433,43,43,434,6,6,54,44,2,5,4,3,3,4",CLASS_NAME,TABLE_NAME_1));
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
	public void testGroupAggregateJoinStats() throws Exception {
			Statement s = methodWatcher.getStatement();
			methodWatcher.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)").execute();
			methodWatcher.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)").execute();
			methodWatcher.prepareCall(String.format("CALL SYSCS_UTIL.SYSCS_SET_XPLAIN_SCHEMA('%s')",CLASS_NAME)).execute();
			ResultSet rs = s.executeQuery(format("select i, count(i) from %s left outer join %s on i=j group by i order by i",this.getTableReference(TABLE_NAME_1),this.getTableReference(TABLE_NAME_2)));
			while (rs.next()) {
				rs.getInt(1);
			}
			methodWatcher.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)").execute();						
			rs = s.executeQuery(format("select * from %s.SYSXPLAIN_STATEMENT_TIMINGS",CLASS_NAME));
			if (!rs.next()) 
				Assert.assertTrue("Runtime Statistics Not Generated for Connection!",false);
	}	
			
}
