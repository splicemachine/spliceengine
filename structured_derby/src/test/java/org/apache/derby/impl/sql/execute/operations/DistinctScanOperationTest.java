package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.suites.OperationCategories;

import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

/**
 * This tests basic table scans with and without projection/restriction
 */

public class DistinctScanOperationTest extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	private static Logger LOG = Logger.getLogger(DistinctScanOperationTest.class);
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(DistinctScanOperationTest.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher("FOO",DistinctScanOperationTest.class.getSimpleName(),"(si varchar(40), sa varchar(40))");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("FOOBAR",DistinctScanOperationTest.class.getSimpleName(),"(name varchar(40), empId int)");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					spliceClassWatcher.setAutoCommit(true);
					Statement s = spliceClassWatcher.getStatement();
					String schema = DistinctScanOperationTest.class.getSimpleName();
					for (int i =1; i<= 10; i++) {
						if (i%3 == 0)
							s.execute("insert into " + schema +".foo values('3','" + "i')");
						else if (i%4 == 0)
							s.execute("insert into " + schema +".foo values('4','" + "i')");
						else
							s.execute("insert into " + schema +".foo values('" + i + "','" + "i')");
					}
					s.execute("insert into " + schema +".foobar values('Mulgrew, Kate', 1)");
					s.execute("insert into " + schema +".foobar values('Shatner, William', 2)");
					s.execute("insert into " + schema +".foobar values('Nimoy, Leonard', 3)");
					s.execute("insert into " + schema +".foobar values('Stewart, Patrick', 4)");
					s.execute("insert into " + schema +".foobar values('Spiner, Brent', 5)");
					s.execute("insert into " + schema +".foobar values('Duncan, Rebort', 6)");
					s.execute("insert into " + schema +".foobar values('Nimoy, Leonard', 7)");
					s.execute("insert into " + schema +".foobar values('Ryan, Jeri', 8)");
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
	public void testDistinctScanOperation() throws Exception {			
		ResultSet rs = methodWatcher.executeQuery("select distinct si from" + this.getPaddedTableReference("FOO"));
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("si="+rs.getString(1));
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(7, j);
	}		
	
	@Test
	public void testDistinctString() throws Exception {			
		ResultSet rs = methodWatcher.executeQuery("select distinct name from" + this.getPaddedTableReference("FOOBAR"));
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("person name="+rs.getString(1));
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(7, j);
	}	
	
	@Test
    @Category(OperationCategories.Transactional.class)
	public void testTransactionalDistinctString() throws Exception {
		methodWatcher.setAutoCommit(false);
		Statement s = methodWatcher.getStatement();
		s.execute("insert into"+ this.getPaddedTableReference("FOOBAR")+"values('Noncommitted, Noncommitted', 9)");
		ResultSet rs = s.executeQuery("select distinct name from"+this.getPaddedTableReference("FOOBAR"));
		int j = 0;
		while (rs.next()) {
			j++;
			LOG.info("before rollback, distinct person name="+rs.getString(1));
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(8, j);
		methodWatcher.rollback();
		rs = s.executeQuery("select distinct name from" + this.getPaddedTableReference("FOOBAR"));
		j = 0;
		while (rs.next()) {
			j++;
			LOG.info("after rollback, person name="+rs.getString(1));
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(7, j);
	}		
}
