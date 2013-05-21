package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.google.common.base.Throwables;
import org.apache.derby.iapi.error.StandardException;
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

public class OnceOperationTest extends SpliceUnitTest {
	private static Logger LOG = Logger.getLogger(OnceOperationTest.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = OnceOperationTest.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME = "A";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME,"(k int, l int)");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
				PreparedStatement statement = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?, ?)",CLASS_NAME,TABLE_NAME));
				statement.setInt(1, 1);
				statement.setInt(2, 2);
				statement.execute();
				statement.setInt(1, 3);
				statement.setInt(2, 4);
				statement.execute();
				statement.setInt(1, 3);
				statement.setInt(2, 4);
				statement.execute();
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
	public void testValuesStatement() throws Exception {
		ResultSet rs = methodWatcher.executeQuery(format("values (select k from %s where k = 1)",this.getTableReference(TABLE_NAME)));
		rs.next();
		Assert.assertNotNull(rs.getInt(1));
	}

	@Test(expected=SQLException.class)
	public void testValuesStatementNonScalarError() throws Exception{
		try {
			ResultSet rs = methodWatcher.executeQuery(format("values (select k from %s where k = 3)",this.getTableReference(TABLE_NAME)));
			rs.next();
		} catch (SQLException t) {
            t.printStackTrace();
            Assert.assertEquals("Incorrect SQLState returned","21000",t.getSQLState());
            throw t;
		}
	}
}
