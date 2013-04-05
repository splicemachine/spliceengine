package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.Assert;
import org.apache.log4j.Logger;
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

public class MiscOperationTest extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	private static Logger LOG = Logger.getLogger(MiscOperationTest.class);
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(MiscOperationTest.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("A",MiscOperationTest.class.getSimpleName(),"(num int, addr varchar(50), zip char(5))");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
				Statement s = spliceClassWatcher.getStatement();	
				s.execute("insert into "+ MiscOperationTest.class.getSimpleName() + ".A values(100, '100: 101 Califronia St', '94114')");
				s.execute("insert into "+ MiscOperationTest.class.getSimpleName() + ".A values(200, '200: 908 Glade Ct.', '94509')");
				s.execute("insert into "+ MiscOperationTest.class.getSimpleName() + ".A values(300, '300: my addr', '34166')");
				s.execute("insert into "+ MiscOperationTest.class.getSimpleName() + ".A values(400, '400: 182 Second St.', '94114')");
				s.execute("insert into "+ MiscOperationTest.class.getSimpleName() + ".A(num) values(500)");
				s.execute("insert into "+ MiscOperationTest.class.getSimpleName() + ".A values(600, 'new addr', '34166')");
				s.execute("insert into "+ MiscOperationTest.class.getSimpleName() + ".A(num) values(700)");
				spliceClassWatcher.commit();
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
	public void testAlterTableAddColumn() throws Exception {
		Statement s = methodWatcher.getStatement();
		s.execute("Alter table"+this.getPaddedTableReference("A")+"add column salary float default 0.0");	
		s.execute("update"+this.getPaddedTableReference("A")+"set salary=1000.0 where zip='94114'");
		s.execute("update"+this.getPaddedTableReference("A")+"set salary=5000.85 where zip='94509'");
		ResultSet rs = s.executeQuery("select zip, salary from"+this.getPaddedTableReference("A"));
		while (rs.next()) {
			if (rs.getString(1)!= null && rs.getString(1).equals("94114"))
				Assert.assertEquals("94114 update not correct", new Double(1000.0), new Double(rs.getDouble(2)));
			if (rs.getString(1)!= null && rs.getString(1).equals("94509"))
				Assert.assertEquals("94509 update not correct", new Double(5000.85), new Double(rs.getDouble(2)));
		}	
	}
	
}
